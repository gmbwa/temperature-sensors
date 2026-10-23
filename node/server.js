import cluster from 'node:cluster';
import inspector from 'node:inspector';
import { writeFile } from 'node:fs/promises';
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
// Capture a worker CPU profile on demand without terminating the process.
// Start with PROFILE_CPU=1; after the load test send SIGUSR2 to each worker.
if (process.env.PROFILE_CPU === '1') {
  const session = new inspector.Session();
  session.connect();
  const post = (method, params = {}) => new Promise((resolve, reject) =>
    session.post(method, params, (err, result) => err ? reject(err) : resolve(result))
  );
  let capturing = false;
  const ready = (async () => {
    await post('Profiler.enable');
    await post('Profiler.start');
    console.log('CPU profiler started for worker', process.pid);
  })();
  process.on('SIGUSR2', async () => {
    if (capturing) return;
    capturing = true;
    try {
      await ready;
      const { profile } = await post('Profiler.stop');
      const filename = resolve(here, `worker-${process.pid}-${Date.now()}.cpuprofile`);
      await writeFile(filename, JSON.stringify(profile));
      console.log('CPU profile saved:', filename);
      await post('Profiler.start');
    } catch (err) {
      console.error('CPU profile capture failed:', err);
    } finally {
      capturing = false;
    }
  });
  ready.catch(err => console.error('CPU profiler startup failed:', err));
}

dotenv.config({ path: resolve(here, '../.env') });
const NO_DB = process.env.NO_DB === '1';
const NO_COPY = process.env.NO_COPY === '1';
if (NO_DB && NO_COPY) throw new Error('Use either NO_DB=1 or NO_COPY=1, not both');
const DATABASE_URL = process.env.DATABASE_URL;
if (!NO_DB && !DATABASE_URL) throw new Error('DATABASE_URL is required');
// Split the same total queue and database concurrency across two cluster workers.
const CLUSTERED = cluster.isWorker;
const CAPACITY = CLUSTERED ? 5000 : 10000;
const WORKERS = Number(process.env.WORKERS || (CLUSTERED ? 5 : 10));
const BATCH_SIZE = 2000, WAIT_MS = 600;
// Shared batch collector: one 50ms flush timer, 20 concurrent COPY workers.
const pool = new pg.Pool({ connectionString: DATABASE_URL, max: WORKERS });
let currentBatch = [];
const batchQueue = [];
const idleWorkers = [];
let pendingReadings = 0;
let accepted = 0, rejected = 0, persisted = 0, failed = 0;
let batchCount = 0, batchMs = 0, batchRows = 0;
let sizeFlushes = 0, sizeFlushRows = 0, timerFlushes = 0, timerFlushRows = 0;
let csvPrepMs = 0, copyMs = 0, dbAcquireMs = 0, timedBatches = 0;
const loopDelay = monitorEventLoopDelay({ resolution: 20 });
loopDelay.enable();
let previousELU = performance.eventLoopUtilization();

function depth() { return pendingReadings; }
function enqueue(reading) {
  if (pendingReadings >= CAPACITY) { rejected++; return false; }
  pendingReadings++;
  accepted++;
  currentBatch.push(reading);
  if (currentBatch.length >= BATCH_SIZE) flush('size');
  return true;
}
function flush(reason) {
  if (!currentBatch.length) return;
  if (reason === 'size') { sizeFlushes++; sizeFlushRows += currentBatch.length; }
  else { timerFlushes++; timerFlushRows += currentBatch.length; }
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
setInterval(() => flush('timer'), WAIT_MS).unref();

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
    sizeFlushes, timerFlushes,
    avgSizeFlushBatchSize: sizeFlushes ? Math.round(sizeFlushRows / sizeFlushes) : 0,
    avgTimerFlushBatchSize: timerFlushes ? Math.round(timerFlushRows / timerFlushes) : 0,
    queuedBatches: batchQueue.length,
    avgBatchMs: batchCount ? Math.round(batchMs / batchCount) : 0,
    avgDbAcquireMs: timedBatches ? Number((dbAcquireMs / timedBatches).toFixed(2)) : 0,
    avgCsvPrepMs: timedBatches ? Number((csvPrepMs / timedBatches).toFixed(2)) : 0,
    avgCopyMs: timedBatches ? Number((copyMs / timedBatches).toFixed(2)) : 0,
    eventLoopUtilization: Number(elu.utilization.toFixed(3)),
    eventLoopP95Ms,
  }));
  accepted = rejected = persisted = failed = batchCount = batchMs = batchRows = 0;
  sizeFlushes = sizeFlushRows = timerFlushes = timerFlushRows = 0;
  csvPrepMs = copyMs = dbAcquireMs = timedBatches = 0;
}, 10000).unref();

function csvField(value) {
  const s = String(value);
  return '"' + s.replaceAll('"', '""') + '"';
}

async function saveBatch(batch) {
  if (NO_COPY) return;
  const acquireStarted = performance.now();
  const client = await pool.connect();
  dbAcquireMs += performance.now() - acquireStarted;
  timedBatches++;
  try {
    const prepStarted = performance.now();
    const csv = batch.map(r =>
      csvField(r.device_id) + ',' + Number(r.temperature_c) + ',' +
      new Date(Number(r.timestamp) * 1000).toISOString() + '\n'
    ).join('');
    csvPrepMs += performance.now() - prepStarted;
    const copyStarted = performance.now();
    try {
      await pipeline(
      Readable.from([csv]),
      client.query(copyFrom('COPY temperature_readings (device_id, temperature_c, recorded_at) FROM STDIN WITH (FORMAT csv)'))
      );
    } finally {
      copyMs += performance.now() - copyStarted;
    }
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
      if (!NO_COPY) persisted += batch.length;
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
    if (NO_DB) {
      accepted++;
      callback(null, { accepted: true });
      return;
    }
    if (!enqueue(call.request)) {
      callback({ code: grpc.status.RESOURCE_EXHAUSTED, message: 'queue full' });
      return;
    }
    callback(null, { accepted: true });
  },
});

try {
  if (!NO_DB) {
    if (!NO_COPY) await pool.query('SELECT 1');
    for (let i = 0; i < WORKERS; i++) worker(i).catch(err => console.error('Worker stopped', i, err));
    if (NO_COPY) console.log('NO_COPY=1 benchmark mode: queue and workers active; database writes skipped');
  } else {
    console.log('NO_DB=1 benchmark mode: readings acknowledged without persistence');
  }
  server.bindAsync('127.0.0.1:50051', grpc.ServerCredentials.createInsecure(), (err, port) => {
    if (err) { console.error(err); process.exitCode = 1; return; }
    console.log('Node gRPC server listening on port', port);
  });
} catch (err) {
  console.error('Startup failed:', err);
  process.exitCode = 1;
  await pool.end();
}
