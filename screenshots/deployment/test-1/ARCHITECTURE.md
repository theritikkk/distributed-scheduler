```text
                ┌─────────────┐
                │ PostgreSQL  │
                └──────┬──────┘
                       │
                       │
             ┌─────────▼─────────┐
             │   Coordinator     │
             └─────────┬─────────┘
                       │
                       ▼
                ┌─────────────┐
                │ RabbitMQ    │
                └──────┬──────┘
                       │
        ┌──────────────┼──────────────┐
        ▼              ▼              ▼
    Worker-1      Worker-2      Worker-3

```

---

```text

Prometheus <-- Metrics
      ▲
      │
 API Gateway
 Coordinator
 Workers
 RabbitMQ Exporter
```

---

```text

Docker Logs
      │
      ▼
  Promtail
      │
      ▼
    Loki
      │
      ▼
   Grafana

```