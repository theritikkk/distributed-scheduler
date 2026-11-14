// Example k6 smoke/load test. Copy to tests/production-test.js and set:
//   BASE_URL=https://your-host TOKEN=<jwt> k6 run tests/production-test.js
import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  stages: [
    { duration: '10s', target: 50 },
    { duration: '20s', target: 200 },
    { duration: '10s', target: 0 },
  ],
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<500', 'p(99)<1000'],
  },
};

const BASE_URL = __ENV.BASE_URL || 'http://localhost:3000';
const TOKEN = __ENV.TOKEN;
const DELAY_MS = Number(__ENV.DELAY_MS || 60_000);

if (!TOKEN) {
  throw new Error('Set TOKEN to a valid JWT before running this test.');
}

export default function () {
  const payload = JSON.stringify({
    task_name: `prod-load-${__VU}-${__ITER}`,
    command_payload: { command: 'echo stress' },
    schedule_type: 'one-time',
    next_execution_time: new Date(Date.now() + DELAY_MS).toISOString(),
  });

  const res = http.post(`${BASE_URL}/api/v1/tasks`, payload, {
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${TOKEN}`,
    },
  });

  check(res, {
    'status is 200 or 201': (r) => r.status === 200 || r.status === 201,
  });

  sleep(0.1);
}
