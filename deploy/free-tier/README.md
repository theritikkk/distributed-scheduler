# Free-tier EC2 demo

This Terraform stack creates **one** EC2 instance (`m7i-flex.large` by default) for a
single-node scheduler demo. It does **not** create Phase 6 HA: PostgreSQL, RabbitMQ,
Redis, and the workers all run on the same host.

The account's AWS Free plan currently lists `m7i-flex.large` as free-tier eligible, but
the plan is credit-based. Check the remaining credit balance before leaving the instance
running for a long time:

```bash
aws freetier get-account-plan-state --region us-east-1
```

## What it creates

- One Amazon Linux 2023 EC2 instance with Docker and Compose installed.
- An IAM instance profile with `AmazonSSMManagedInstanceCore` (no SSH ingress).
- A security group exposing API, Grafana, Prometheus, and RabbitMQ management only to
  `allowed_cidr`.
- An encrypted 30 GB gp3 root volume and an S3 artifact bucket.

## Usage

```bash
cd deploy/free-tier
terraform init
terraform apply -var="allowed_cidr=$(curl -s https://checkip.amazonaws.com)/32"
```

Then build and deploy the current working tree over SSM:

```bash
export INSTANCE_ID="$(terraform output -raw instance_id)"
export BUCKET="$(terraform output -raw artifact_bucket)"
export PUBLIC_IP="$(terraform output -raw public_ip)"
./deploy.sh
```

`deploy.sh` generates a fresh production `.env`, uploads a source bundle to the
encrypted bucket, installs Docker/Compose/buildx over SSM, runs
`docker compose up -d --build`, waits for `/health`, and deletes the bundle.
It does not commit or print secrets. The Terraform state file is local and is
needed for `terraform destroy`; keep it out of git.

## Screenshots and benchmark

[`screenshots/`](screenshots/README.md) carries the latest v1.6.0 evidence:
API, PostgreSQL (including the Phase 7 schema), coordinator, worker, RabbitMQ,
Prometheus, Grafana, both architecture diagrams, and a 300-task burst
benchmark (300/300 completed, 0 failures, 163.1 req/s submission, 17.4 s
drain across ten workers).

These captures are from the **local Docker Compose E2E stack**, not from the
AWS instance. The AWS lab was not re-provisioned for this evidence run, so do
not present them as an AWS deployment.

## Cost warning

The instance, public IPv4 address, EBS volume, S3 storage, and data transfer consume the
account's free-plan credits. Stop or destroy the stack when it is not needed:

```bash
terraform destroy
```

This is a demo, not a production HA topology. For actual HA, see
[`docs/HA.md`](../../docs/HA.md).
