# Deployment Guide

This documents exactly how the distributed scheduler is deployed — on a single AWS EC2 instance using Docker Compose, Nginx, and the full observability stack.

---

## Infrastructure

| Resource | Value |
|----------|-------|
| Provider | AWS EC2 (ap-south-1) |
| Instance type | t3 series, Ubuntu 22.04 LTS |
| Deployment method | Docker Compose |
| Reverse proxy | Nginx |
| HTTPS | Let's Encrypt / Certbot (requires a domain) |

---

## What runs on the instance

13 containers, all managed by Docker Compose:

| Container | Ports (host) | Notes |
|-----------|-------------|-------|
| `api-gateway` | 3000 | Behind Nginx |
| `coordinator` | — | Internal only |
| `worker-1` | 9101 | Metrics |
| `worker-2` | 9102 | Metrics |
| `worker-3` | 9103 | Metrics |
| `postgres` | — (expose only) | Not reachable from host |
| `rabbitmq` | 15672 | Management UI only |
| `rabbitmq-exporter` | — (expose only) | Internal |
| `prometheus` | 9091 | |
| `alertmanager` | 9093 | |
| `grafana` | 3001 | |
| `loki` | — (expose only) | Internal |
| `promtail` | — | Reads Docker socket |

---

## EC2 Security Group

| Port | Source | Purpose |
|------|--------|---------|
| 22 | Your IP | SSH |
| 80 | 0.0.0.0/0 | Nginx HTTP |
| 443 | 0.0.0.0/0 | Nginx HTTPS |
| 3000 | 0.0.0.0/0 | API Gateway (direct) |
| 3001 | Your IP | Grafana |
| 9091 | Your IP | Prometheus |
| 15672 | Your IP | RabbitMQ Management UI |

Ports **5432**, **5672**, and **3100** are not in the security group — they are internal to the Docker network only.

---

## Initial Setup (run once)

### 1. Install Docker

```bash
sudo apt update && sudo apt upgrade -y
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER
newgrp docker
docker compose version
```

### 2. Clone the repo

```bash
git clone https://github.com/theritikkk/distributed-scheduler.git
cd distributed-scheduler
```

### 3. Configure environment

```bash
cp .env.example .env
nano .env
```

Required values to set:

```env
POSTGRES_PASSWORD=<strong password>
RABBITMQ_DEFAULT_PASS=<strong password>
JWT_SECRET=<run: openssl rand -hex 32>
GF_SECURITY_ADMIN_PASSWORD=<strong password>

# Update passwords inside these too:
DATABASE_URL=postgresql://scheduler:<password>@postgres:5432/distributed_scheduler
RABBITMQ_URL=amqp://scheduler:<password>@rabbitmq:5672
```

### 4. Start everything

```bash
docker compose up -d --build
docker compose ps   # all containers should show Up
```

### 5. Nginx reverse proxy

```bash
sudo apt install nginx -y

sudo tee /etc/nginx/sites-available/scheduler > /dev/null << 'NGINXEOF'
server {
    listen 80;
    server_name _;

    location /api/ {
        proxy_pass http://localhost:3000;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    }

    location /health {
        proxy_pass http://localhost:3000;
    }
}
NGINXEOF

sudo ln -s /etc/nginx/sites-available/scheduler /etc/nginx/sites-enabled/scheduler
sudo nginx -t && sudo systemctl restart nginx
```

### 6. Firewall

```bash
sudo ufw allow OpenSSH
sudo ufw allow 80
sudo ufw allow 443
sudo ufw enable
```

### 7. HTTPS (requires a domain pointed at the EC2 IP)

```bash
sudo apt install certbot python3-certbot-nginx -y
sudo certbot --nginx -d yourdomain.com
sudo systemctl status certbot.timer   # verify auto-renewal
```

> Note: HTTPS requires a real domain with an A record pointing to your EC2 IP. It will not work with a raw IP address. Let's Encrypt failed when run against `yourdomain.com` (placeholder) — replace with your actual domain.

---

## Verify the stack

```bash
# API health
curl http://YOUR_EC2_IP:3000/health
# {"status":"ok","service":"api-gateway"}

# All containers running
docker compose ps
```

Open in browser:

| Service | URL |
|---------|-----|
| Grafana | `http://YOUR_EC2_IP:3001` |
| Prometheus | `http://YOUR_EC2_IP:9091` |
| RabbitMQ UI | `http://YOUR_EC2_IP:15672` |

---

## Updating the deployment

```bash
cd distributed-scheduler
git pull
docker compose up -d --build
docker image prune -f
```

---

## Useful debugging commands

```bash
# Check all containers
docker compose ps

# Tail all logs
docker compose logs -f

# Logs for a specific service
docker compose logs -f worker-1
docker compose logs -f coordinator
docker compose logs -f api-gateway

# Resource usage
docker stats

# Shell into a container
docker exec -it worker-1 sh
docker exec -it distributed-scheduler-postgres-1 sh
```

---

## Security checklist

- [x] All passwords changed from `.env.example` defaults
- [x] `JWT_SECRET` generated with `openssl rand -hex 32`
- [x] `.env` in `.gitignore`, never committed
- [x] PostgreSQL not exposed publicly (`expose:` not `ports:`)
- [x] RabbitMQ AMQP port (5672) not exposed publicly
- [x] Loki (3100) not exposed publicly
- [x] Grafana login enabled, sign-up disabled
- [x] `restart: unless-stopped` on all services
- [ ] HTTPS enabled (requires a domain)
- [ ] Grafana and Prometheus restricted to your IP in security group