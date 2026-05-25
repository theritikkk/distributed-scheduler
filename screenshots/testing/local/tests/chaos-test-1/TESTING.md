# Chaos Testing


``#!/bin/bash` is called a **shebang**.

It tells Linux/macOS which interpreter should execute the script.

In this case:
- `/bin/bash` -> execute the script using Bash

Without the shebang, the operating system would not know how to run the script directly.

This script simulates burst traffic against the distributed scheduler.
It is used to:
- stress test the API gateway
- generate queue spikes in RabbitMQ
- observe worker throughput
- trigger retries and DLQ flows
- validate Prometheus/Grafana metrics
- test alerting pipelines
- inspect distributed system behavior under load


---


## Prerequisites

Before running the script:

- Docker Compose services must be running
- API Gateway should be accessible on `localhost:3000`
- `jq` should be installed for JSON formatting
- A valid JWT token is required

Install jq (macOS):

```bash
brew install jq


```text
TOKEN=$1

if [ -z "$TOKEN" ]; then
  echo "Usage: ./monitor-test.sh <JWT_TOKEN>"
  exit 1
fi

echo "Flooding scheduler with demo tasks..."

for i in {1..200}
do
  curl -s -X POST http://localhost:3000/api/v1/tasks \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d "{
      \"task_name\": \"load-test-$i\",
      \"command_payload\": {
        \"command\": \"echo hello-$i\"
      },
      \"schedule_type\": \"one-time\",
      \"next_execution_time\": \"2026-05-25T06:00:00Z\"
    }" | jq .

  echo "Queued task $i"
done

echo "Done."
```

---

## Make Script Executable:
```text 
chmod +x scripts/monitor-test.sh
```


---


## Expected System Behavior:

During execution, the following behaviors should be observable:

- RabbitMQ queue depth increases temporarily
- Workers consume tasks concurrently
- Prometheus metrics increase
- Grafana dashboards show throughput spikes
- Loki receives worker execution logs
- Alertmanager may trigger backlog/retry alerts
- DLQ queue may receive poison messages during failure testing


---


## Failure Injection Testing

To test retries and DLQ behavior, intentionally submit invalid commands.

Example payload:

```json
{
  "command": "nonexistent_command"
}
```

Expected behavior:
- Worker processing fails
- Retry queue receives the task
- Exponential backoff is applied
- Task retries until maxRetries
- Task is routed to scheduler.tasks.dlq
- Alerts/logs/metrics are generated

This section makes the project feel much more production-oriented.

---

## Observability & Logs

During chaos testing, observe the following systems:

- RabbitMQ queues
- Prometheus metrics
- Grafana dashboards
- Loki logs
- Alertmanager alerts
- Worker execution logs

---

## Conclusion

This script helps validate:
- distributed task execution
- queue-based orchestration
- retry semantics
- DLQ routing
- observability pipelines
- monitoring and alerting behavior under load

It serves as a lightweight chaos-testing utility for the distributed scheduler platform.

---


## Important Notes

High-volume task floods may trigger:
- API rate limiting (`429 Too Many Requests`)
- worker memory pressure
- RabbitMQ connection resets
- Docker Desktop memory exhaustion on macOS

For local development:
- reduce worker replicas if containers crash
- increase API rate limit temporarily during stress testing