# Temperature Sensor Load Testing MCP (Phase 1)

An MCP stdio server built with Node.js/TypeScript and the official MCP SDK. It reuses the existing `loadtest/temperature.js` without modifying the ingestion services.

## Setup

From the repository root:

```sh
cd mcp
npm install
npm run build
```

Install k6 separately and ensure `k6` is on PATH. Start **one** Go, Node.js, or Elixir ingestion server at `127.0.0.1:50051` in another terminal.

For Cursor or Claude Desktop, configure a local stdio MCP server with:
- command: `node`
- args: `["/ABSOLUTE/PATH/temperature-sensors/mcp/dist/index.js"]`

Use an absolute path and restart your MCP client after changing its configuration. The MCP process uses stdout for protocol messages; k6 output is suppressed and results are saved to disk.

## Tools

- `start_load_test({ implementation: "elixir", rate: 8000, durationSeconds: 30 })` returns a run ID immediately.
- `get_test_results({ runId })` returns `running`, `completed`, or `failed` and summary metrics.
- `compare_test_runs({ firstRunId, secondRunId })` compares saved runs.

**Safety:** Only the checked-in local k6 script and its fixed `127.0.0.1:50051` target are permitted. Rate is limited to 20,000/s, duration to 120 seconds, and only one test can run at a time per MCP process. The implementation name is a label; the tool does not start or switch the target service. Avoid multiple MCP instances if you need global test serialization.

## Data

Each run is saved under ignored `mcp/results/<runId>.json`; the original k6 summary is retained as `<runId>.summary.json`. The summary includes throughput, completed requests, dropped iterations, checks, and gRPC latency percentiles. k6 exits nonzero when thresholds fail; the run is marked failed even if a summary is available.

**Not yet implemented:** server-side PostgreSQL COPY/worker metrics, live progress monitoring, service auto-start, and cross-run history search. These belong to later phases. k6 summaries alone cannot establish actual database persistence.

The tool has not yet been locally built or tested. Run `npm run build` before configuring the MCP client.
