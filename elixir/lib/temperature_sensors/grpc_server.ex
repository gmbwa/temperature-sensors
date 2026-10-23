defmodule TemperatureSensors.GrpcServer do
  use GRPC.Server, service: Temperature.V1.TemperatureService.Service
  alias Temperature.V1.RecordTemperatureResponse
  alias TemperatureSensors.{Collector, Metrics}

  def record_temperature(reading, _stream) do
    accepted =
      if System.get_env("NO_DB") == "1" do
        Metrics.inc(:accepted)
        true
      else
        started = System.monotonic_time(:microsecond)
        result = Collector.enqueue(reading)
        elapsed_us = System.monotonic_time(:microsecond) - started
        Metrics.observe_collector_call(elapsed_us)
        result
      end

    if accepted do
      %RecordTemperatureResponse{accepted: true}
    else
      raise GRPC.RPCError, status: GRPC.Status.resource_exhausted(), message: "queue full"
    end
  end
end

defmodule TemperatureSensors.Endpoint do
  use GRPC.Endpoint
  run TemperatureSensors.GrpcServer
end
