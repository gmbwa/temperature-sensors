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


## k6 gRPC load test

The Go client simulates devices. The k6 test under `loadtest/temperature.js` is for repeatable performance measurement, including latency percentiles and checks.

Install k6 on macOS:

```bash
brew install k6
```

Start the gRPC server in one terminal:

```bash
go run ./cmd/server
```

Then run the target-rate test from the repository root:

```bash
k6 run loadtest/temperature.js
```

The test targets 8,333 unary gRPC readings per second for 60 seconds, approximately the average traffic from 500,000 devices reporting once per minute. It checks that gRPC calls succeed and readings are accepted, and includes a p95 gRPC request-duration threshold of 100 ms.

The load generator and server run on the same machine in this local test, so results measure the combined local setup rather than isolated server capacity.


## Performance experiments

These are local development measurements, not production capacity claims. k6 and the Go gRPC server ran on the same machine. The target rate of 8,333 readings/second approximates 500,000 devices each reporting once per minute.

| Experiment | Target rate | Completed rate | gRPC p95 | Dropped iterations | RPC checks | What it showed |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| Direct unary gRPC handler | 8,333/s | 8,308/s | 139 µs | 1,533 | 100% | Bare unary gRPC handled the target local synthetic workload with very low latency. |
| Buffered channel + fast consumer | 8,333/s | 8,174/s | 676.9 µs | 9,581 | 100% | Adding an in-memory channel introduced synchronization overhead but requests still completed successfully. |
| Buffered channel + intentionally slow consumer | 8,333/s | 1,054/s | 2.5 s | 434,336 | 100% | A 1 ms delay per consumed reading limited one consumer to roughly 1,000 readings/s. The channel filled, RPC handlers waited for space, and backpressure propagated to k6. |
| Buffered channel + 10 slow consumers | 8,333/s | 7,960/s | 3.88 ms | 21,720 | 100% | Ten concurrent consumers largely removed the one-worker bottleneck while keeping the same 1 ms simulated processing cost. The run approached, but did not fully sustain, the target arrival rate. |
| Buffered channel + 20 slow consumers | 8,333/s | 8,211/s | 488.66 µs | 7,330 | 100% | Twenty consumers provided enough headroom for the simulated 1 ms processing cost, bringing latency and achieved rate close to the fast-consumer baseline. |

The one-worker slow-consumer experiment is deliberately pathological. Its purpose is to make backpressure visible before introducing a worker pool. The 100% RPC check rate does not mean the overloaded run performed well: requests that did execute eventually succeeded, while latency increased sharply and k6 could not schedule most of the requested iterations.

## Local PostgreSQL configuration

Start PostgreSQL with `docker compose up -d`. Copy the development-only environment template into an ignored local file:

```bash
cp .env.example .env
set -a
source .env
set +a
go run ./cmd/server
```

`.env` is already excluded by `.gitignore`; do not commit real credentials. The server reads `DATABASE_URL` from its environment. A successful gRPC acknowledgement currently means the reading entered the in-memory queue, **not** that PostgreSQL committed it.

## PostgreSQL persistence benchmark (local k6)

Same k6 scenario: 8,333 scheduled iterations/sec for 60 seconds, up to 2,000 VUs. Tests run locally with PostgreSQL in Docker. Throughput is completed k6 iterations per second, **not** maximum server capacity.

| Persistence strategy | Completed/sec | gRPC p95 | Dropped iterations | Executed RPC checks | Verified PostgreSQL rows |
| --- | ---: | ---: | ---: | ---: | ---: |
| 20 workers, one INSERT per reading | 4,719 | 602.85 ms | 210,222 | 100% | 289,747 new rows (plus 10 existing) |
| 20 workers, CopyFrom batches of up to 100, 50 ms flush | 8,188 | 1.45 ms | 8,147 | 100% | 491,829 (table truncated before run) |
| 20 workers, CopyFrom batches of up to 500, 50 ms flush | 8,270 | 270.54 µs | 3,802 | 100% | 496,182 (table truncated before run) |

Both tests matched their expected database row counts after the workers drained. This verifies persistence in those runs, **not** crash durability: the server acknowledges once a reading enters an in-memory channel, and database failures are logged without retry.

### Higher arrival-rate experiment (batch size 500)

With 20 workers, 500-row maximum CopyFrom batches and 50 ms flush timeout, `RATE=12000 k6 run loadtest/temperature.js` yielded **714,790 completed iterations (11,913/sec)**, **1.65 ms gRPC p95**, **5,215 dropped iterations**, and **100% checks**. After truncating the table before the test and allowing the queue to drain, PostgreSQL contained **714,790 rows**. This validates matching accepted/persisted counts for this run, but the RPC acknowledgment precedes the database commit; no crash durability is implied. The result is not a measured maximum throughput.

### 15,000/sec arrival-rate experiment (batch size 500)

At `RATE=15000`, k6 completed **897,166 iterations (14,953/sec)**, with **1.19 ms gRPC p95**, **2,844 dropped iterations**, and **100% checks**. The reported database total was **1,611,956 rows**, which equals **714,790 from the previous 12k/sec run + 897,166 from this run**. The user confirmed the table was not truncated between runs, so the increase of 897,166 rows exactly matches the 897,166 completed requests (zero missing rows in this test). RPC acknowledgment remains in-memory only, and this does not establish crash durability or the system's maximum capacity.

### 20,000/sec arrival-rate experiment (batch size 500)

At `RATE=20000`, k6 completed **1,179,518 iterations (19,655/sec)** with **6.75 ms gRPC p95**, **20,479 dropped iterations**, and **100% RPC checks**. The reported PostgreSQL row count was **1,179,518**, equal to completed requests, but whether the table was truncated before this run is not yet confirmed. The increased p95 relative to 15k/sec suggests greater load pressure; no specific bottleneck has been identified. RPC acknowledgments remain non-durable.
