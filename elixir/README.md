# Elixir OTP temperature ingestion

An Elixir implementation of the same `temperature.v1.TemperatureService` RPC as the Go and Node services. Uses a supervised GenServer collector, a bounded queue (10,000 readings), 2,000-reading batches, a 600 ms flush timer, a Task.Supervisor with configurable COPY concurrency, and Postgrex.

## Run

From the repository root:

```sh
cd elixir
mix deps.get
NO_DB=1 mix run --no-halt
```

Other modes (stop the previous server before switching):

```sh
NO_COPY=1 WORKERS=2 mix run --no-halt
DATABASE_URL='postgres://...' WORKERS=2 mix run --no-halt
```

From the repository root, run the same k6 benchmark:

```sh
RATE=11000 DURATION=2m k6 run loadtest/temperature.js
```

A successful gRPC response means the reading was accepted into memory, **not** necessarily committed. `NO_DB=1` bypasses batching; `NO_COPY=1` exercises the collector and task workers but skips database writes.

This code has not yet been compiled or benchmarked on the target Mac. Check dependency compatibility and COPY behavior before treating results as a language comparison.
