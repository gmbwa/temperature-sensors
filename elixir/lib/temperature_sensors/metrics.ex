defmodule TemperatureSensors.Metrics do
  use GenServer
  @keys [:accepted, :rejected, :persisted, :failed, :batches]

  def start_link(_), do: GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  def inc(key, n \\ 1), do: GenServer.cast(__MODULE__, {:inc, key, n})
  def init(_), do: {:ok, Map.new(@keys, &{&1, 0}) |> schedule()}
  defp schedule(state) do
    Process.send_after(self(), :report, 10_000)
    state
  end
  def handle_cast({:inc, key, n}, state), do: {:noreply, Map.update(state, key, n, &(&1 + n))}
  def handle_info(:report, state) do
    IO.puts(JasonFallback.encode(state))
    {:noreply, Map.new(@keys, &{&1, 0}) |> schedule()}
  end
end

defmodule JasonFallback do
  def encode(map) do
    "{" <> (map |> Enum.map(fn {k, v} -> ~s("#{k}":#{v}) end) |> Enum.join(",")) <> "}"
  end
end
