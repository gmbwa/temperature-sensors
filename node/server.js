import grpc from '@grpc/grpc-js';
import protoLoader from '@grpc/proto-loader';
import pg from 'pg';
import { from as copyFrom } from 'pg-copy-streams';
import { pipeline } from 'node:stream/promises';
import { Readable } from 'node:stream';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import dotenv from 'dotenv';

const here = dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: resolve(here, '../.env') });
const DATABASE_URL = process.env.DATABASE_URL;
if (!DATABASE_URL) throw new Error('DATABASE_URL is required');
const CAPACITY = 10000, WORKERS = 20, BATCH_SIZE = 500, WAIT_MS = 50;
// Bounded FIFO with amortized O(1) dequeue; no timer per reading.
const queue = [];
let head = 0;
const waiting = [];
const pool = new pg.Pool({ connectionString: DATABASE_URL, max: 20 });
let accepted = 0, rejected = 0, persisted = 0, failed = 0;
let batchCount = 0, batchMs = 0;

function depth() { return queue.length - head; }
function dequeue() {
  if (!depth()) return null;
  const item = queue[head++];
  if (head >= 4096 && head * 2 >= queue.length) {
    queue.splice(0, head);
    head = 0;
  }
  return item;
}
function enqueue(reading) {
  if (depth() >= CAPACITY) { rejected++; return false; }
  queue.push(reading);
  accepted++;
  if (waiting.length) waiting.shift()();
  return true;
}
function waitForData(ms) {
  if (depth()) return Promise.resolve();
  return new Promise(resolve => {
    const wake = () => { clearTimeout(timer); resolve(); };
    const timer = setTimeout(() => {
      const i = waiting.indexOf(wake);
      if (i >= 0) waiting.splice(i, 1);
      resolve();
    }, ms);
    waiting.push(wake);
  });
}
setInterval(() => {
  console.log(JSON.stringify({
    accepted, rejected, persisted, failed, queueDepth: depth(),
    dbActive: pool.totalCount - pool.idleCount,
    dbWaiting: pool.waitingCount,
    batches: batchCount,
    avgBatchMs: batchCount ? Math.round(batchMs / batchCount) : 0,
  }));
  accepted = rejected = persisted = failed = batchCount = batchMs = 0;
}, 1000).unref();

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
    await waitForData(WAIT_MS);
    const batch = [];
    while (batch.length < BATCH_SIZE) {
      const reading = dequeue();
      if (!reading) break;
      batch.push(reading);
    }
    if (!batch.length) continue;
    const started = performance.now();
    try {
      await saveBatch(batch);
      persisted += batch.length;
    } catch (err) {
      failed += batch.length;
      console.error('Worker', id, 'failed batch of', batch.length, err);
    } finally {
      batchCount++;
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
