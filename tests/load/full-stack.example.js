// Smaller end-to-end load test for local full-stack baselines.
// Start the API, coordinator, and at least one worker, then:
//   BASE_URL=http://localhost:3100 TOKEN=<jwt> k6 run tests/load/full-stack.example.js
import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  scenarios: {
    submission: {
      executor: 'constant-vus',
      vus: Number(__ENV.VUS || 10),
      duration: __ENV.DURATION || '10s',
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<500'],
  },
};

const BASE_URL = __ENV.BASE_URL || 'http://localhost:3000';
const TOKEN = __ENV.TOKEN;
const DELAY_MS = Number(__ENV.DELAY_MS || 5000);

if (!TOKEN) {
  throw new Error('Set TOKEN to a valid JWT before running this test.');
}

export default function () {
  const payload = JSON.stringify({
    task_name: `full-stack-${__VU}-${__ITER}`,
    command_payload: { command: 'echo full-stack' },
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
