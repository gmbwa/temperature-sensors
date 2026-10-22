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

At `RATE=20000`, k6 completed **1,179,518 iterations (19,655/sec)** with **6.75 ms gRPC p95**, **20,479 dropped iterations**, and **100% RPC checks**. The user confirmed the table was truncated before this run. PostgreSQL contained **1,179,518 rows**, exactly matching completed requests (zero missing rows in this run). The increased p95 relative to 15k/sec suggests greater load pressure; no specific bottleneck has been identified. RPC acknowledgments remain non-durable.

### 25,000/sec arrival-rate experiment (batch size 500)

At `RATE=25000`, k6 completed **1,356,794 iterations (22,612/sec)**, with **34.99 ms gRPC p95**, **143,038 dropped iterations**, and **100% checks**. The table was confirmed truncated before the run; PostgreSQL contained **1,356,794 rows**, exactly matching the completed iteration count (zero missing rows in this run). Compared with 20k/sec, increased latency and dropped iterations suggest saturation of the local end-to-end test setup, but do not isolate a PostgreSQL bottleneck. The gRPC acknowledgment occurs before persistence.

### Node.js gRPC/PostgreSQL COPY comparison (local)

The `node/` implementation uses the same protobuf service and PostgreSQL table, with a bounded in-memory queue, 20 COPY workers, shared batches of up to 500 readings, and a 50 ms flush interval. Its acknowledgment means queued in memory, not durably committed. Run it with `cd node && npm install && npm start` (stop Go first); benchmark from the repository root.

At `RATE=10000`, a full 60-second k6 run completed **595,484 iterations (9,925/sec)** with **9.26 ms gRPC p95**, **4,517 dropped iterations**, and **100% RPC checks**. The reported PostgreSQL count was **595,484 rows**, matching completed iterations; table truncation immediately before this run was not explicitly confirmed. Earlier partial 10k/sec runs were manually interrupted and are not sustained results.

At `RATE=20000`, earlier Node implementations with per-worker batches completed approximately **9,069–9,457/sec** with **204–225 ms gRPC p95** and frequent connection establishment errors. Those runs are not a controlled comparison with the newer shared-batch implementation. At higher loads, k6 can create many gRPC connections (one per VU), and the load generator, server and PostgreSQL share the same local machine. More sustained testing and active event-loop metrics are needed before attributing the difference to language runtime performance.

### Node.js two-process cluster, 15,000/sec target (local)

With `npm run start:cluster` (two Node.js processes, 10 database workers and 5,000 queue slots per process), `RATE=15000` ran the full 60 seconds and completed **615,725 iterations (10,247/sec)**. gRPC p95 was **327.06 ms** (failed the 100 ms threshold), **284,288 iterations were dropped**, and k6 hit the **2,000 VU** limit. All **615,725 completed requests** passed both RPC checks; the reported PostgreSQL row count was **615,725**, matching completed iterations (assuming the table was cleared before this run). This is not a controlled same-target comparison against the one-process run, and does not isolate whether the limit is in the Node server, cluster port sharing, or the local k6/OS setup.

### Node.js two-process cluster, 12,000/sec target (local)

Full 60-second `RATE=12000` run: **538,932 completed iterations (8,962/sec)**, gRPC p95 **361.76 ms** (failed `<100 ms`), **181,082 dropped iterations**, k6 reached **2,000 VUs**, and 100% of checks on completed iterations passed. User reported **538,932 PostgreSQL rows**, matching completions (assuming a clean table and completed writes). This run performed worse than the earlier 15k-target cluster test; causes not yet isolated. Investigate per-process event-loop metrics and machine-level contention before tuning further.

### Node.js cluster repeat at 12,000/sec (local)

Second full 60-second `RATE=12000` run: **656,902 completed iterations (10,909/sec)**, gRPC p95 **216.58 ms** (failed `<100 ms`), **63,102 dropped iterations**, k6 reached **2,000 VUs**, and 100% of checks on completed iterations passed. Both process logs showed event-loop utilization increasing from ~0.83–0.87 to ~0.96–0.97 and event-loop p95 increasing to 106–151 ms. `rejected=0`, `failed=0`, and `dbWaiting=0` in provided samples; DB row count not provided for this run. The result improves over the first 12k run without a confirmed configuration change; repeatability and shared-machine contention remain concerns.

### Node.js cluster third run at 12,000/sec (local)

Third full 60-second `RATE=12000` run: **659,349 completed iterations (10,960/sec)**, gRPC p95 **224.69 ms** (failed `<100 ms`), **61,417 dropped iterations**, k6 reached **2,000 VUs**, and 100% of checks on completed iterations passed. User reported **659,349 PostgreSQL rows**, matching completed iterations (assuming a clean table and fully drained writes). Two processes had balanced acceptance and event-loop utilization climbing to ~0.98–0.99, with p95 event-loop delay peaking at ~127–137 ms; reported `rejected=0`, `failed=0`, `dbWaiting=0`. Results similar to second 12k run, suggesting ~10.9k/sec observed throughput under these local conditions, but not meeting latency or offered-load targets.

### Node.js two-process cluster NO_DB isolation test at 12,000/sec (local)

With `NO_DB=1 npm run start:cluster`, full 60-second `RATE=12000` k6 run completed **707,299 iterations (11,788/sec)** with **3.48 ms gRPC p95** (passed `<100 ms`), **12,704 dropped iterations**, and 100% checks on completed iterations. Two process logs showed event-loop utilization mostly ~0.43–0.64 under load and p95 event-loop delay ~21 ms. No persistence occurs in this mode (`persisted=0` by design). Versus recent normal-mode 12k run (659,349 iterations, 10,960/sec, 224.69 ms p95, 61,417 dropped), bypassing queue, batching, CSV and PostgreSQL COPY together dramatically reduced latency and event-loop pressure. This does **not** isolate PostgreSQL itself; further experiments should separate queue/batching overhead from COPY/DB work. Even NO_DB dropped iterations, so k6/local-machine capacity may contribute.

### Node.js two-process cluster NO_DB at 25,000/sec target (local)

Full 60-second `RATE=25000` with `NO_DB=1`: **976,936 completed iterations (16,213/sec)**, gRPC p95 **228.91 ms** (failed `<100 ms`), **523,891 dropped iterations**, and 100% checks on completed iterations. k6 reached 2,000 VUs by ~3 seconds. Both Node processes showed event-loop utilization ~0.985–0.999 during load, event-loop p95 ~68–82 ms (PID 93303) and ~142–200 ms (PID 93302). `persisted=0` by design. The test confirms that removing persistence does not remove the high-load bottleneck, although server-vs-k6/shared-machine contention is not isolated. Compare against NO_DB 12k: 11,788/sec and 3.48 ms p95.

### Node.js two-process cluster NO_DB at 25,000/sec with increased file-descriptor limit

With `ulimit -n 8192` in both Node and k6 shells (previous test used 256), full 60-second `RATE=25000` / `NO_DB=1` run completed **1,040,416 iterations (17,318/sec)**, gRPC p95 **192.69 ms** (failed `<100 ms`), **459,537 dropped iterations**, and 100% checks on completed iterations. k6 again reached 2,000 VUs. Both Node processes reached ~0.995–1.0 event-loop utilization, event-loop p95 ~87–112 ms during sustained load, with no DB connections or app-level rejections. Previous 256-limit run: 976,936 (16,213/sec), p95 228.91 ms, 523,891 dropped. Improvement is associated with the changed limit but a single A/B run cannot establish causality; benchmark variability and shared-machine contention remain possible.

### Node.js two-process cluster NO_DB at 15,000/sec target (ulimit 8192)

Full 60-second `RATE=15000` / `NO_DB=1` run completed **858,011 iterations (14,260/sec)**, gRPC p95 **85.22 ms** (passed `<100 ms`), **42,984 dropped iterations**, and 100% checks on completed iterations. Maximum active k6 VUs: 1,975 of 2,000. Node process event-loop utilization generally ~0.61–0.80 during sustained load, event-loop p95 ~23–50 ms, no app-level rejections, and no DB writes by design. Although latency threshold passed, achieved throughput was ~95.1% of target; near-VU saturation suggests k6/host contention may affect results. Next consider a lower-rate test to establish sustainable throughput before adding more benchmark modes.

### Node.js two-process cluster WITH PostgreSQL at 14,000/sec target (configuration changed unintentionally)

User ran `npm run start:cluster` (without `NO_DB=1`), so this is a **database-enabled** run, not the intended 14k NO_DB isolation test. Full 60-second `RATE=14000`: **438,651 completed iterations (7,285/sec)**, gRPC p95 **539.28 ms** (failed `<100 ms`), **401,089 dropped iterations**, k6 reached 2,000 VUs, and 100% checks on completed iterations. Both Node processes reached ELU ~1.0; event-loop p95 reached 517 ms; batch durations averaged as high as 404 ms; `dbActive` typically 1–5, `dbWaiting=0`, no reported failed batches; queues drained at end. Cannot directly compare as controlled 14k vs 15k NO_DB A/B due to differing requested rates and workload. Next rerun 14k explicitly with `NO_DB=1` before interpreting threshold.

### Node.js two-process cluster NO_DB at 14,000/sec target (ulimit 8192)

Corrected 60-second `RATE=14000` / `NO_DB=1` run: **825,706 completed iterations (13,762/sec)**, gRPC p95 **11.42 ms** (passed `<100 ms`), **14,301 dropped iterations** (~1.7% of 840,000 scheduled), 100% checks on completed iterations, and maximum allocated k6 VUs 615. Node ELU during sustained load ~0.54–0.69, event-loop p95 ~21–26 ms; no rejections and no DB writes by design. Compare NO_DB 15k: 14,260/sec, p95 85.22 ms, 42,984 dropped. A single run at each rate is insufficient to establish a sharp performance boundary; repeated runs would help.

### Node.js two-process cluster NO_COPY at 12,000/sec target (ulimit 8192)

Full 60-second `RATE=12000` / `NO_COPY=1` run: **712,729 completed iterations (11,879/sec)**, gRPC p95 **6.80 ms** (passed `<100 ms`), **7,274 dropped iterations**, 100% checks on completed iterations, and max allocated k6 VUs 460. Both processes retained queue, batch collector, and worker scheduling but skipped CSV conversion, COPY streams, and PostgreSQL. ELU during load ~0.47–0.66; event-loop p95 ~21–24 ms; small queues generally <300, no rejected or failed batches, no database connections, and no persistence by design. For comparison, 12k NO_DB run was 11,788/sec and 3.48 ms p95; normal DB-enabled 12k run was 10,960/sec and 224.69 ms p95. These runs suggest queue/batching overhead is modest and the persistence stage contributes substantial overhead, but separate runs and shared-machine effects preclude precise attribution.

### Node.js CPU-profiling benchmark (PostgreSQL, 12k target)

With `NODE_OPTIONS="--cpu-prof --cpu-prof-dir=." npm run start:cluster`, k6 completed 648,936 iterations (10,800/sec), dropped 71,071, gRPC p95 243.44ms, and reached 2,000 VUs. Completed checks passed 100%; latency threshold failed. During load, event-loop utilization approached 0.98–0.99 and event-loop p95 delay reached 141ms. CSV preparation averaged less than 1ms/batch, while COPY stream durations rose from approximately 32–48ms to 80–109ms. No DB pool waiters were reported and queues drained. CPU profile files are needed to attribute CPU usage to functions. Run-to-run variation is substantial: the prior DB-enabled 12k test achieved 8,838/sec and p95 311.35ms.
