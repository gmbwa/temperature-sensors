defmodule TemperatureSensors.Metrics do
  use GenServer
  @keys [:accepted, :rejected, :persisted, :failed, :batches]
  @wait_buckets_ms [1, 5, 10, 25, 50, 100, 250, 500, 1_000]

  def start_link(_), do: GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  def inc(key, n \\ 1), do: GenServer.cast(__MODULE__, {:inc, key, n})

  # Measures the time a gRPC handler waits for GenServer.call/3 to return.
  # This includes mailbox wait + collector processing, not just mailbox wait.
  def observe_collector_call(us), do: GenServer.cast(__MODULE__, {:collector_call, us})

  def init(_) do
    Process.send_after(self(), :report, 10_000)
    {:ok, fresh()}
  end

  defp fresh do
    %{counts: Map.new(@keys, &{&1, 0}), calls: 0, total_us: 0,
      max_us: 0, buckets: List.duplicate(0, length(@wait_buckets_ms) + 1)}
  end

  def handle_cast({:inc, key, n}, state) do
    {:noreply, update_in(state, [:counts, key], &(&1 + n))}
  end

  def handle_cast({:collector_call, us}, state) do
    ms = us / 1_000
    index = Enum.find_index(@wait_buckets_ms, &(ms <= &1)) || length(@wait_buckets_ms)
    buckets = List.update_at(state.buckets, index, &(&1 + 1))
    {:noreply, %{state | calls: state.calls + 1, total_us: state.total_us + us,
                         max_us: max(state.max_us, us), buckets: buckets}}
  end

  def handle_info(:report, state) do
    collector = Process.whereis(TemperatureSensors.Collector)
    mailbox = if collector do
      case Process.info(collector, :message_queue_len) do
        {:message_queue_len, n} -> n
        _ -> nil
      end
    else
      nil
    end

    avg_ms = if state.calls == 0, do: 0, else: Float.round(state.total_us / state.calls / 1_000, 3)
    max_ms = Float.round(state.max_us / 1_000, 3)

    # Percentile buckets are upper bounds, not exact percentile values.
    p95_le_ms = percentile_bucket(state.buckets, state.calls, 0.95)

    output = Map.merge(state.counts, %{
      collector_mailbox: mailbox,
      collector_call_count: state.calls,
      collector_call_avg_ms: avg_ms,
      collector_call_max_ms: max_ms,
      collector_call_p95_le_ms: p95_le_ms
    })
    IO.puts(JasonFallback.encode(output))
    Process.send_after(self(), :report, 10_000)
    {:noreply, fresh()}
  end

  defp percentile_bucket(_buckets, 0, _fraction), do: nil
  defp percentile_bucket(buckets, count, fraction) do
    target = ceil(count * fraction)
    {index, _} =
      Enum.with_index(buckets)
      |> Enum.reduce_while(0, fn {n, index}, running ->
        total = running + n
        if total >= target, do: {:halt, {index, total}}, else: {:cont, total}
      end)
    Enum.at(@wait_buckets_ms, index, ">1000")
  end
end

defmodule JasonFallback do
  def encode(map) do
    "{" <> (map |> Enum.map(fn {k, v} ->
      value = if is_nil(v), do: "null", else: if(is_binary(v), do: ~s("#{v}"), else: to_string(v))
      ~s("#{k}":#{value})
    end) |> Enum.join(",")) <> "}"
  end
end
