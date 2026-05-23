// production-test.js
import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  stages: [
    { duration: '10s', target: 50 },   // ramp up
    { duration: '20s', target: 200 },  // steady load
    { duration: '10s', target: 0 },    // ramp down
  ],
};

const TOKEN = "what-ever-yours-is";

export default function () {

  const payload = JSON.stringify(
    {
      task_name: "prod-load",
      command_payload: { msg: "stress" },
      schedule_type: "one-time",
      next_execution_time: "2000-01-01T00:00:00Z"
    }
  );

  const res = http.post(

    'http://43.204.30.206:3000/api/v1/tasks',

    payload,
    {
    
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${TOKEN}`,
      },

    }
  );

  check(
    res, 
    {
      'status is 200 or 201': (r) => r.status === 200 || r.status === 201,
    }
  );

  sleep( 0.1 ); // prevent total meltdown
}