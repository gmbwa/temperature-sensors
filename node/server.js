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
const queue = [];
const waiting = [];
const pool = new pg.Pool({ connectionString: DATABASE_URL, max: 20 });

function take(timeoutMs) {
  if (queue.length) return Promise.resolve(queue.shift());
  return new Promise(resolve => {
    const receive = value => {
      clearTimeout(timer);
      resolve(value);
    };
    const timer = setTimeout(() => {
      const index = waiting.indexOf(receive);
      if (index >= 0) waiting.splice(index, 1);
      resolve(null);
    }, timeoutMs);
    waiting.push(receive);
  });
}

function enqueue(reading) {
  if (waiting.length) waiting.shift()(reading);
  else if (queue.length < CAPACITY) queue.push(reading);
  else return false;
  return true;
}

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
  let batch = [];
  let deadline = Date.now() + WAIT_MS;
  while (true) {
    const remaining = Math.max(0, deadline - Date.now());
    const reading = await take(remaining);
    if (reading) batch.push(reading);
    if (batch.length >= BATCH_SIZE || Date.now() >= deadline) {
      if (batch.length) {
        try { await saveBatch(batch); }
        catch (err) { console.error('Worker', id, 'failed batch of', batch.length, err); }
        batch = [];
      }
      deadline = Date.now() + WAIT_MS;
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
