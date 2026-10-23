defmodule Temperature.V1.TemperatureReading do
  use Protobuf, syntax: :proto3
  field :device_id, 1, type: :string, json_name: "deviceId"
  field :temperature_c, 2, type: :double, json_name: "temperatureC"
  field :timestamp, 3, type: :int64
end

defmodule Temperature.V1.RecordTemperatureResponse do
  use Protobuf, syntax: :proto3
  field :accepted, 1, type: :bool
end

defmodule Temperature.V1.TemperatureService.Service do
  use GRPC.Service, name: "temperature.v1.TemperatureService"
  rpc :RecordTemperature, Temperature.V1.TemperatureReading, Temperature.V1.RecordTemperatureResponse
end

defmodule Temperature.V1.TemperatureService.Stub do
  use GRPC.Stub, service: Temperature.V1.TemperatureService.Service
end
