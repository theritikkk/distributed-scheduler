# Distributed Scheduler Deployment Guide

## Overview

This guide walks through deploying your distributed scheduler project using:

* Docker Compose
* A Linux VM (recommended: Ubuntu 22.04)
* Nginx reverse proxy
* Domain setup (optional)
* HTTPS with Let's Encrypt
* Production environment configuration
* Basic monitoring

Your stack already includes:

* PostgreSQL
* RabbitMQ
* API Gateway
* Coordinator
* Worker replicas
* Prometheus
* Grafana

This is already a strong infrastructure-style project.

---

# Recommended Deployment Architecture

```text
Internet
   |
   v
Nginx Reverse Proxy
   |
   +--> API Gateway (Node.js)
   +--> Grafana

Docker Network
   |
   +--> PostgreSQL
   +--> RabbitMQ
   +--> Coordinator
   +--> Workers
   +--> Prometheus
```

---

# Step 1 — Get a VM

Recommended providers:

* DigitalOcean
* Hetzner
* AWS EC2
* Azure VM
* Oracle Cloud Free Tier

Recommended specs:

| Resource | Minimum   |
| -------- | --------- |
| CPU      | 2 vCPU    |
| RAM      | 4 GB      |
| Storage  | 40 GB SSD |

Ubuntu 22.04 LTS is recommended.

---

# Step 2 — SSH Into the VM

```bash
ssh ubuntu@YOUR_SERVER_IP
```

---

# Step 3 — Install Docker

## Update packages

```bash
sudo apt update && sudo apt upgrade -y
```

## Install Docker

```bash
curl -fsSL https://get.docker.com | sh
```

## Add your user to docker group

```bash
sudo usermod -aG docker $USER
```

Logout and reconnect:

```bash
exit
```

SSH again.

---

# Step 4 — Install Docker Compose

Modern Docker includes compose plugin.

Verify:

```bash
docker compose version
```

---

# Step 5 — Clone Your Repository

```bash
git clone YOUR_GITHUB_REPO_URL
cd YOUR_REPO_NAME
```

Example:

```bash
git clone https://github.com/theritikkk/distributed-scheduler.git
cd distributed-scheduler
```

---

# Step 6 — Create Production `.env`

Create:

```bash
nano .env
```

Example:

```env
POSTGRES_USER=scheduler
POSTGRES_PASSWORD=VERY_STRONG_PASSWORD
POSTGRES_DB=distributed_scheduler

RABBITMQ_DEFAULT_USER=scheduler
RABBITMQ_DEFAULT_PASS=VERY_STRONG_PASSWORD

JWT_SECRET=SUPER_LONG_RANDOM_SECRET

DATABASE_URL=postgresql://scheduler:VERY_STRONG_PASSWORD@postgres:5432/distributed_scheduler
RABBITMQ_URL=amqp://scheduler:VERY_STRONG_PASSWORD@rabbitmq:5672

GF_SECURITY_ADMIN_USER=admin
GF_SECURITY_ADMIN_PASSWORD=CHANGE_THIS_PASSWORD
```

---

# Step 7 — Update Docker Compose for Production

## Remove unnecessary public ports

You should NOT expose internal services publicly.

Keep ONLY:

```yaml
ports:
  - "80:80"
  - "443:443"
```

through Nginx.

---

# Step 8 — Modify Services

## PostgreSQL

Remove:

```yaml
ports:
  - "5432:5432"
```

Use only:

```yaml
expose:
  - "5432"
```

---

## RabbitMQ

Keep management UI optional.

Production recommendation:

```yaml
ports:
  - "15672:15672"
```

Do NOT expose 5672 publicly.

---

## API Gateway

Keep:

```yaml
ports:
  - "3000:3000"
```

Nginx will reverse proxy to it.

---

# Step 9 — Build and Start Services

```bash
docker compose up -d --build
```

Verify:

```bash
docker ps
```

---

# Step 10 — Check Logs

## All services

```bash
docker compose logs -f
```

## Specific service

```bash
docker compose logs -f api-gateway
```

Examples:

```bash
docker compose logs -f worker

docker compose logs -f coordinator
```

---

# Step 11 — Install Nginx

```bash
sudo apt install nginx -y
```

---

# Step 12 — Configure Reverse Proxy

Create config:

```bash
sudo nano /etc/nginx/sites-available/distributed-scheduler
```

Example:

```nginx
server {
    listen 80;
    server_name YOUR_DOMAIN_OR_IP;

    location / {
        proxy_pass http://localhost:3000;

        proxy_http_version 1.1;

        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection 'upgrade';
        proxy_set_header Host $host;
        proxy_cache_bypass $http_upgrade;
    }
}
```

Enable site:

```bash
sudo ln -s /etc/nginx/sites-available/distributed-scheduler /etc/nginx/sites-enabled/
```

Test:

```bash
sudo nginx -t
```

Restart:

```bash
sudo systemctl restart nginx
```

---

# Step 13 — Configure Firewall

```bash
sudo ufw allow OpenSSH
sudo ufw allow 80
sudo ufw allow 443
sudo ufw enable
```

---

# Step 14 — Add HTTPS with Let's Encrypt

Install certbot:

```bash
sudo apt install certbot python3-certbot-nginx -y
```

Generate certificate:

```bash
sudo certbot --nginx -d yourdomain.com
```

Verify auto renewal:

```bash
sudo systemctl status certbot.timer
```

---

# Step 15 — Monitoring

## Prometheus

Access:

```text
http://SERVER_IP:9091
```

---

## Grafana

Access:

```text
http://SERVER_IP:3001
```

Login:

```text
admin
YOUR_PASSWORD
```

---

# Step 16 — Production Improvements

## Add restart policy

Already good:

```yaml
restart: unless-stopped
```

---

## Add resource limits

Already excellent.

This is good engineering practice.

---

## Add health checks

Already implemented well.

---

## Add centralized logging later

Future improvements:

* Loki
* ELK Stack
* OpenSearch

---

# Step 17 — Deployment Workflow

## Update code

```bash
git pull
```

## Rebuild

```bash
docker compose up -d --build
```

## Remove unused images

```bash
docker image prune -f
```

---

# Step 18 — Debugging Commands

## Check containers

```bash
docker ps
```

---

## Check container resource usage

```bash
docker stats
```

---

## Enter a container

```bash
docker exec -it CONTAINER_NAME sh
```

Example:

```bash
docker exec -it distributed-scheduler-postgres-1 sh
```

---

## Check network

```bash
docker network ls
```

---

# Step 19 — Optional Improvements

## CI/CD

You can later add:

* GitHub Actions
* Docker Hub image publishing
* Automated deployment pipeline

---

## Kubernetes

Future scaling path:

* Kubernetes
* Helm charts
* Horizontal autoscaling
* Service mesh

---

# Step 20 — Resume Value

This project demonstrates:

* Distributed systems concepts
* Async task processing
* RabbitMQ messaging
* Worker orchestration
* Docker orchestration
* Monitoring stack integration
* Infrastructure engineering
* Production deployment understanding
* Service health management
* Resource isolation

This is significantly stronger than typical CRUD resume projects.

---

# Suggested Next Improvements

## Best next upgrades for your project

### 1. Add Redis

Use cases:

* distributed locks
* caching
* rate limiting
* job deduplication
* websocket scaling

---

### 2. Add OpenTelemetry

For tracing.

---

### 3. Add Dead Letter Queues

Important distributed systems feature.

---

### 4. Add Retry Policies

Exponential backoff.

---

### 5. Add Horizontal Worker Autoscaling

Based on queue depth.

---

# Final Recommended Public URLs

| Service     | URL                                                              |
| ----------- | ---------------------------------------------------------------- |
| API         | [https://api.yourdomain.com](https://api.yourdomain.com)         |
| Grafana     | [https://grafana.yourdomain.com](https://grafana.yourdomain.com) |
| Prometheus  | Internal only                                                    |
| RabbitMQ UI | Internal only                                                    |

---

# Important Security Notes

Never:

* commit `.env`
* expose PostgreSQL publicly
* expose RabbitMQ publicly
* use default passwords
* expose Grafana without auth

Always:

* use HTTPS
* rotate secrets
* update containers regularly
* monitor logs
* backup databases
