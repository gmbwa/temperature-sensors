import cluster from 'node:cluster';
import grpc from '@grpc/grpc-js';
import protoLoader from '@grpc/proto-loader';
import pg from 'pg';
import { from as copyFrom } from 'pg-copy-streams';
import { pipeline } from 'node:stream/promises';
import { Readable } from 'node:stream';
import { performance, monitorEventLoopDelay } from 'node:perf_hooks';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import dotenv from 'dotenv';

const here = dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: resolve(here, '../.env') });
const DATABASE_URL = process.env.DATABASE_URL;
if (!DATABASE_URL) throw new Error('DATABASE_URL is required');
// Split the same total queue and database concurrency across two cluster workers.
const CLUSTERED = cluster.isWorker;
const CAPACITY = CLUSTERED ? 5000 : 10000;
const WORKERS = CLUSTERED ? 10 : 20;
const BATCH_SIZE = 500, WAIT_MS = 50;
// Shared batch collector: one 50ms flush timer, 20 concurrent COPY workers.
const pool = new pg.Pool({ connectionString: DATABASE_URL, max: WORKERS });
let currentBatch = [];
const batchQueue = [];
const idleWorkers = [];
let pendingReadings = 0;
let accepted = 0, rejected = 0, persisted = 0, failed = 0;
let batchCount = 0, batchMs = 0, batchRows = 0;
const loopDelay = monitorEventLoopDelay({ resolution: 20 });
loopDelay.enable();
let previousELU = performance.eventLoopUtilization();

function depth() { return pendingReadings; }
function enqueue(reading) {
  if (pendingReadings >= CAPACITY) { rejected++; return false; }
  pendingReadings++;
  accepted++;
  currentBatch.push(reading);
  if (currentBatch.length >= BATCH_SIZE) flush();
  return true;
}
function flush() {
  if (!currentBatch.length) return;
  batchQueue.push(currentBatch);
  currentBatch = [];
  dispatch();
}
function dispatch() {
  while (idleWorkers.length && batchQueue.length) {
    idleWorkers.pop()(batchQueue.shift());
  }
}
function nextBatch() {
  if (batchQueue.length) return Promise.resolve(batchQueue.shift());
  return new Promise(resolve => idleWorkers.push(resolve));
}
setInterval(flush, WAIT_MS).unref();

setInterval(() => {
  const elu = performance.eventLoopUtilization(previousELU);
  previousELU = performance.eventLoopUtilization();
  const eventLoopP95Ms = Number.isFinite(loopDelay.percentile(95)) ? Math.round(loopDelay.percentile(95) / 1e6) : 0;
  loopDelay.reset();
  console.log(JSON.stringify({
    pid: process.pid, accepted, rejected, persisted, failed, queueDepth: depth(),
    dbActive: pool.totalCount - pool.idleCount,
    dbWaiting: pool.waitingCount,
    batches: batchCount,
    avgBatchSize: batchCount ? Math.round(batchRows / batchCount) : 0,
    queuedBatches: batchQueue.length,
    avgBatchMs: batchCount ? Math.round(batchMs / batchCount) : 0,
    eventLoopUtilization: Number(elu.utilization.toFixed(3)),
    eventLoopP95Ms,
  }));
  accepted = rejected = persisted = failed = batchCount = batchMs = batchRows = 0;
}, 10000).unref();

function csvField(value) {
  const s = String(value);
  return '"' + s.replaceAll('"', '""') + '"';
}

async function saveBatch(batch) {
  const client = await pool.connect();
  try {
    const lines = batch.map(r =>
      [csvField(r.device_id), Number(r.temperature_c), new Date(Number(r.timestamp) * 1000).toISOString()].join(',') + '\n'
    );
    await pipeline(
      Readable.from(lines),
      client.query(copyFrom('COPY temperature_readings (device_id, temperature_c, recorded_at) FROM STDIN WITH (FORMAT csv)'))
    );
  } finally {
    client.release();
  }
}

async function worker(id) {
  while (true) {
    const batch = await nextBatch();
    const started = performance.now();
    try {
      await saveBatch(batch);
      persisted += batch.length;
    } catch (err) {
      failed += batch.length;
      console.error('Worker', id, 'failed batch of', batch.length, err);
    } finally {
      pendingReadings -= batch.length;
      batchCount++;
      batchRows += batch.length;
      batchMs += performance.now() - started;
    }
  }
}

const definition = protoLoader.loadSync(resolve(here, '../proto/temperature/v1/temperature.proto'), {
  keepCase: true, longs: String, enums: String, defaults: true, oneofs: true,
});
const proto = grpc.loadPackageDefinition(definition).temperature.v1;
const server = new grpc.Server();
server.addService(proto.TemperatureService.service, {
  RecordTemperature(call, callback) {
    if (!enqueue(call.request)) {
      callback({ code: grpc.status.RESOURCE_EXHAUSTED, message: 'queue full' });
      return;
    }
    callback(null, { accepted: true });
  },
});

try {
  await pool.query('SELECT 1');
  for (let i = 0; i < WORKERS; i++) worker(i).catch(err => console.error('Worker stopped', i, err));
  server.bindAsync('127.0.0.1:50051', grpc.ServerCredentials.createInsecure(), (err, port) => {
    if (err) { console.error(err); process.exitCode = 1; return; }
    console.log('Node gRPC server listening on port', port);
  });
} catch (err) {
  console.error('Startup failed:', err);
  process.exitCode = 1;
  await pool.end();
}
