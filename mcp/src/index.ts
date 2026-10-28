import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";
import { spawn, type ChildProcess } from "node:child_process";
import { mkdir, readFile, writeFile, readdir } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { randomUUID } from "node:crypto";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "../../..");
const script = join(root, "loadtest/temperature.js");
const resultsDir = join(root, "mcp/results");
const maxRate = 20_000;
const maxSeconds = 120;
const implementations = ["go", "node", "elixir"] as const;
type Implementation = typeof implementations[number];
type Run = {
  id: string; implementation: Implementation; rate: number; durationSeconds: number;
  startedAt: string; finishedAt?: string; status: "running" | "completed" | "failed";
  exitCode?: number | null; error?: string; metrics?: Record<string, unknown>;
};
let active: { run: Run; child: ChildProcess } | undefined;
const server = new McpServer({ name: "temperature-sensors-loadtest", version: "0.1.0" });

const text = (value: unknown) => ({ content: [{ type: "text" as const, text: JSON.stringify(value, null, 2) }] });
const fileFor = (id: string) => join(resultsDir, id + ".json");
async function save(run: Run) { await mkdir(resultsDir, { recursive: true }); await writeFile(fileFor(run.id), JSON.stringify(run, null, 2)); }
async function load(id: string): Promise<Run> {
  if (!/^[0-9a-f-]{36}$/.test(id)) throw new Error("Invalid run ID");
  return JSON.parse(await readFile(fileFor(id), "utf8")) as Run;
}
type K6Metric = { values?: Record<string, number> };
function parseSummary(summary: Record<string, unknown>): Record<string, unknown> {
  const metrics = (summary.metrics ?? {}) as Record<string, K6Metric>;
  const values = (key: string) => metrics[key]?.values ?? {};
  return {
    completedRequests: values("iterations").count ?? null,
    completedRps: values("iterations").rate ?? null,
    droppedIterations: values("dropped_iterations").count ?? 0,
    grpcLatencyMs: {
      avg: values("grpc_req_duration").avg ?? null,
      p50: values("grpc_req_duration")["med"] ?? null,
      p95: values("grpc_req_duration")["p(95)"] ?? null,
      p99: values("grpc_req_duration")["p(99)"] ?? null,
    },
    checksRate: values("checks").rate ?? null,
    checksFailed: values("checks").fails ?? null,
    // Server-side DB timings are not part of the k6 summary.
    databaseMetrics: null,
  };
}

server.registerTool("start_load_test", {
  description: "Start one bounded k6 run against an already-running local gRPC temperature service. The implementation is a label, not an automatic service switch.",
  inputSchema: {
    implementation: z.enum(implementations),
    rate: z.number().int().min(1).max(maxRate),
    durationSeconds: z.number().int().min(1).max(maxSeconds),
  },
}, async ({ implementation, rate, durationSeconds }) => {
  if (active) return text({ error: "Another test is running", runId: active.run.id });
  const run: Run = { id: randomUUID(), implementation, rate, durationSeconds, startedAt: new Date().toISOString(), status: "running" };
  await save(run);
  const summaryFile = join(resultsDir, run.id + ".summary.json");
  // No shell, no user-controlled executable, script, target address, or output path.
  const child = spawn("k6", ["run", "--summary-export", summaryFile, script], {
    cwd: root, env: { ...process.env, RATE: String(rate), DURATION: durationSeconds + "s" },
    stdio: ["ignore", "pipe", "pipe"],
  });
  active = { run, child };
  let stderr = "";
  child.stderr?.on("data", (data: Buffer) => { stderr = (stderr + data.toString()).slice(-4000); });
  child.stdout?.resume();
  let finished = false;
  const finish = async (code: number | null, error?: string) => {
    if (finished) return;
    finished = true;
    run.finishedAt = new Date().toISOString();
    run.exitCode = code;
    try {
      const summary = JSON.parse(await readFile(summaryFile, "utf8"));
      run.metrics = parseSummary(summary);
    } catch (e) {
      run.error = "Could not read k6 summary: " + String(e);
    }
    if (error || code !== 0) run.error = [error, stderr.slice(-1500), run.error].filter(Boolean).join("\n");
    run.status = code === 0 && !!run.metrics ? "completed" : "failed";
    try { await save(run); } finally { if (active?.run.id === run.id) active = undefined; }
  };
  child.once("error", (err) => { void finish(null, err.message); });
  child.once("close", (code) => { void finish(code); });
  return text({ runId: run.id, status: run.status, implementation, rate, durationSeconds, note: "Ensure the named implementation is listening on 127.0.0.1:50051. Query get_test_results for completion." });
});

server.registerTool("get_test_results", {
  description: "Retrieve saved results or current status for a load-test run.",
  inputSchema: { runId: z.string().uuid() },
}, async ({ runId }) => {
  try { return text(active?.run.id === runId ? active.run : await load(runId)); }
  catch (error) { return text({ error: String(error) }); }
});

server.registerTool("compare_test_runs", {
  description: "Compare throughput, latency, and failures of two completed local benchmark runs.",
  inputSchema: { firstRunId: z.string().uuid(), secondRunId: z.string().uuid() },
}, async ({ firstRunId, secondRunId }) => {
  try {
    const [first, second] = await Promise.all([load(firstRunId), load(secondRunId)]);
    if (first.status !== "completed" || second.status !== "completed") return text({ error: "Both runs must be completed" });
    const a = first.metrics!, b = second.metrics!;
    const n = (x: unknown) => typeof x === "number" ? x : null;
    const delta = (x: unknown, y: unknown) => n(x) !== null && n(y) !== null ? Number((Number(y) - Number(x)).toFixed(3)) : null;
    const latA = a.grpcLatencyMs as Record<string, unknown>;
    const latB = b.grpcLatencyMs as Record<string, unknown>;
    return text({
      first: { id: first.id, implementation: first.implementation, rate: first.rate, durationSeconds: first.durationSeconds, metrics: a },
      second: { id: second.id, implementation: second.implementation, rate: second.rate, durationSeconds: second.durationSeconds, metrics: b },
      secondMinusFirst: { completedRps: delta(a.completedRps, b.completedRps), p95Ms: delta(latA.p95, latB.p95), droppedIterations: delta(a.droppedIterations, b.droppedIterations) },
      caveat: "Implementation labels are user-provided. Verify which server was running; compare like-for-like rates, durations, and machine conditions. Database metrics are not captured in phase 1.",
    });
  } catch (error) { return text({ error: String(error) }); }
});

await mkdir(resultsDir, { recursive: true });
await server.connect(new StdioServerTransport());
