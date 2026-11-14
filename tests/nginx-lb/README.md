# nginx load-balancer harness

Renders the real HA-lab nginx template and validates it against three disposable
HTTP backends. The backends stand in for the per-node HAProxy API listeners on
`:8080`; nginx listens on `:18081` to avoid the local E2E stack.

```bash
./tests/nginx-lb/run.sh
KEEP=1 ./tests/nginx-lb/run.sh
```

## What is asserted

1. **Rendered config is valid.** `nginx -t` accepts the output of
   `deploy/ha-lab/ha/nginx.conf.tmpl`.
2. **Distribution.** Twelve requests reach all three backends (least-connections).
3. **Passive failover.** After `backend1` stops, no request reaches it and both
   remaining backends receive traffic. `proxy_next_upstream` retries a failed node.
4. **Recovery.** After `backend1` restarts and `fail_timeout` (10 s) elapses, it returns
   to the upstream pool.

## How it maps to the lab

The template renders `__NODE1_IP__`, `__NODE2_IP__`, and `__NODE3_IP__` as the three
HAProxy addresses. In the lab, `deploy.sh` substitutes the EC2 private IPs, starts nginx
on node1 under the `lb` compose profile, and exposes `:8081` only on node1.

The harness does not test TLS, HTTP/2, WebSocket upgrades, or multiple nginx instances.
node1 remains a single point of failure for `:8081`; per-node HAProxy `:8080` is the
fallback. See [docs/internals/08](../../docs/internals/08-load-balancing.md).

## Requirements

- Docker with a running daemon
- `curl`
- `sed`/`bash` (standard on macOS/Linux)
