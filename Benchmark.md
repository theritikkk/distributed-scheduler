# Benchmark Results

Environment:
- AWS EC2 t3.small
- PostgreSQL 15
- RabbitMQ 3
- 3 Worker Replicas

Results:
- 7,300+ scheduled tasks processed
- Queue depth monitoring via Prometheus
- P95 latency tracking via Grafana
- Horizontal worker scaling using Docker Compose

Observability:
- Prometheus
- Grafana
- Loki