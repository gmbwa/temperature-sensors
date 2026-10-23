defmodule TemperatureSensors.MixProject do
  use Mix.Project

  def project do
    [app: :temperature_sensors, version: "0.1.0", elixir: "~> 1.16",
     start_permanent: Mix.env() == :prod, deps: deps()]
  end

  def application do
    [extra_applications: [:logger], mod: {TemperatureSensors.Application, []}]
  end

  defp deps do
    [
      {:grpc, "~> 0.10"},
      {:protobuf, "~> 0.15"},
      {:postgrex, "~> 0.20"}
    ]
  end
end
