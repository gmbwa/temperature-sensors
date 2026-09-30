# temperature-sensors

High-throughput temperature telemetry system built with Go and gRPC, designed to simulate and process data from up to 500,000 IoT sensors.

Phase 1 is a minimal gRPC service: one client sends one temperature reading, and the server logs it.

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

The client sends one reading (`device-123` at `4.7°C`) and prints:

```text
accepted: true
```

The server logs:

```text
device-123: 4.7°C
```

Use the same `-addr` flag if the server is not on the default port.

## How the pieces fit together

1. `temperature.proto` defines `TemperatureReading` (the request) and `RecordTemperatureResponse` (`accepted`). It also defines `TemperatureService` with one RPC, `RecordTemperature`.
2. `protoc` turns that contract into Go types and a gRPC client/server API under `gen/temperature/v1`.
3. The client fills a `TemperatureReading`, then calls `RecordTemperature` on the generated client. gRPC serializes the message and sends it over HTTP/2.
4. The server implements the generated `TemperatureServiceServer` interface. Its `RecordTemperature` method receives the decoded reading, logs `device-123: 4.7°C`, and returns `{accepted: true}`.
5. The generated client decodes that response, and the test program prints `accepted`.
