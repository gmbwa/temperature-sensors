defmodule TemperatureSensors.Collector do
  use GenServer
  alias TemperatureSensors.{Metrics, Writer}
  @batch_size 2_000
  @wait_ms 600
  @capacity 10_000

  def start_link(_), do: GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  def enqueue(reading), do: GenServer.call(__MODULE__, {:enqueue, reading}, 30_000)

  def init(_) do
    workers = System.get_env("WORKERS", "10") |> String.to_integer()
    if workers < 1, do: raise("WORKERS must be positive")
    state = %{batch: [], count: 0, queue: :queue.new(), pending: 0,
              active: %{}, workers: workers}
    Process.send_after(self(), :flush, @wait_ms)
    {:ok, state}
  end

  def handle_call({:enqueue, reading}, _from, state) do
    if state.pending >= @capacity do
      Metrics.inc(:rejected)
      {:reply, false, state}
    else
      Metrics.inc(:accepted)
      state = %{state | batch: [reading | state.batch], count: state.count + 1,
                        pending: state.pending + 1}
      state = if state.count >= @batch_size, do: flush(state), else: state
      {:reply, true, state}
    end
  end

  def handle_info(:flush, state) do
    Process.send_after(self(), :flush, @wait_ms)
    {:noreply, flush(state)}
  end

  def handle_info({ref, result}, state) when is_reference(ref) do
    case Map.pop(state.active, ref) do
      {nil, _} -> {:noreply, state}
      {size, active} ->
        Process.demonitor(ref, [:flush])
        Metrics.inc(if(result == :ok, do: :persisted, else: :failed), size)
        Metrics.inc(:batches)
        {:noreply, dispatch(%{state | active: active, pending: state.pending - size})}
    end
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    case Map.pop(state.active, ref) do
      {nil, _} -> {:noreply, state}
      {size, active} ->
        Metrics.inc(:failed, size)
        Metrics.inc(:batches)
        {:noreply, dispatch(%{state | active: active, pending: state.pending - size})}
    end
  end

  defp flush(%{count: 0} = state), do: state
  defp flush(state) do
    queue = :queue.in({Enum.reverse(state.batch), state.count}, state.queue)
    dispatch(%{state | batch: [], count: 0, queue: queue})
  end

  defp dispatch(state) when map_size(state.active) >= state.workers, do: state
  defp dispatch(state) do
    case :queue.out(state.queue) do
      {:empty, _} -> state
      {{:value, {batch, size}}, queue} ->
        task = Task.Supervisor.async_nolink(TemperatureSensors.CopyTasks, fn ->
          Writer.save(batch)
        end)
        dispatch(%{state | queue: queue, active: Map.put(state.active, task.ref, size)})
    end
  end
end
