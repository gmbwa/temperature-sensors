# temperature-sensors

High-throughput temperature telemetry system built with Go and gRPC, designed to simulate and process data from up to 500,000 IoT sensors.

Phase 1 is a minimal gRPC service: one client sends temperature readings, and the server logs them.

## Prerequisites

- Go 1.25 or newer
- [Protocol Buffer compiler](https://protobuf.dev/installation/) (`protoc`)
- Go code generators:

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

`make proto` finds `protoc` on your `PATH` (or in `$(go env GOPATH)/bin`) and adds that bin directory so the plugins are visible. If you run `protoc` yourself, `$(go env GOPATH)/bin` must be on your `PATH`.

On macOS, `protoc` can be installed with:

```bash
brew install protobuf
```

If Homebrew cannot install it, download a `protoc` release for your machine from the [protobuf releases](https://github.com/protocolbuffers/protobuf/releases) and put the `protoc` binary on your `PATH`.

## Generate protobuf code

From the repository root:

```bash
make proto
```

That compiles `proto/temperature/v1/temperature.proto` into:

- `gen/temperature/v1/temperature.pb.go` — message types
- `gen/temperature/v1/temperature_grpc.pb.go` — client and server interfaces

Do not edit those files by hand. Change the `.proto` file and run `make proto` again.

## Start the gRPC server

```bash
go run ./cmd/server
```

The server listens on `127.0.0.1:50051`. Override it with `-addr`:

```bash
go run ./cmd/server -addr 127.0.0.1:50051
```

## Run the test client

In a second terminal, with the server still running:

```bash
go run ./cmd/client
```

The client reuses one gRPC connection and starts one goroutine per simulated device. The default is 10 devices, and each device sends one reading.

Choose the number of devices with `-devices`:

```bash
go run ./cmd/client -devices=10
go run ./cmd/client -devices=1000
go run ./cmd/client -devices=10000
```

Each successful request prints a line like:

```text
device-1 accepted: true
```

After all devices finish, the client prints a simple benchmark summary:

```text
Devices: 1000
Elapsed: 250ms
Throughput: 4000 readings/sec
```

The exact numbers depend on the machine and workload. The server logs one line per reading.

Use the same `-addr` flag if the server is not on the default port.

## How the pieces fit together

1. `temperature.proto` defines `TemperatureReading` (the request) and `RecordTemperatureResponse` (`accepted`). It also defines `TemperatureService` with one RPC, `RecordTemperature`.
2. `protoc` turns that contract into Go types and a gRPC client/server API under `gen/temperature/v1`.
3. The client fills a `TemperatureReading` in each goroutine, then calls `RecordTemperature` on one shared generated client. gRPC serializes each message and sends it over HTTP/2.
4. The server implements the generated `TemperatureServiceServer` interface. Its `RecordTemperature` method receives the decoded reading, logs it, and returns `{accepted: true}`.
5. The generated client decodes that response, and the goroutine prints whether it was accepted. `WaitGroup` keeps the process alive until all configured device calls finish.
