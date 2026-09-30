import grpc from 'k6/net/grpc';
import { check } from 'k6';

const client = new grpc.Client();
client.load(['proto'], 'temperature/v1/temperature.proto');

export const options = {
  scenarios: {
    temperature_readings: {
      executor: 'constant-arrival-rate',
      rate: 8333,
      timeUnit: '1s',
      duration: '60s',
      preAllocatedVUs: 100,
      maxVUs: 2000,
    },
  },
  thresholds: {
    checks: ['rate>0.99'],
    grpc_req_duration: ['p(95)<100'],
  },
};

export default function () {
  client.connect('127.0.0.1:50051', { plaintext: true });

  const response = client.invoke(
    'temperature.v1.TemperatureService/RecordTemperature',
    {
      device_id: `k6-device-${__VU}-${__ITER}`,
      temperature_c: 4.5,
      timestamp: Math.floor(Date.now() / 1000),
    },
  );

  check(response, {
    'gRPC status is OK': (r) => r && r.status === grpc.StatusOK,
    'reading accepted': (r) => r && r.message && r.message.accepted === true,
  });

  client.close();
}
