defmodule TemperatureSensors.Application do
  use Application

  def start(_type, _args) do
    no_db = System.get_env("NO_DB") == "1"
    no_copy = System.get_env("NO_COPY") == "1"
    if no_db and no_copy, do: raise("Use NO_DB or NO_COPY, not both")

    db_children =
      if no_db or no_copy do
        []
      else
        url = System.fetch_env!("DATABASE_URL")
        [{Postgrex, name: TemperatureSensors.DB, url: url,
                     pool_size: String.to_integer(System.get_env("WORKERS", "10"))}]
      end

    children =
      [{TemperatureSensors.Metrics, []}, {Task.Supervisor, name: TemperatureSensors.CopyTasks}] ++
      db_children ++
      (if no_db, do: [], else: [{TemperatureSensors.Collector, []}]) ++
      [{GRPC.Server.Supervisor, {TemperatureSensors.Endpoint, 50051}}]

    Supervisor.start_link(children, strategy: :one_for_one, name: TemperatureSensors.Supervisor)
  end
end
