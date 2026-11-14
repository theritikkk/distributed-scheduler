terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

data "aws_subnet" "default" {
  for_each = toset(data.aws_subnets.default.ids)

  id = each.value
}

data "aws_ssm_parameter" "al2023" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
}

resource "random_id" "suffix" {
  byte_length = 4
}

locals {
  subnets_by_az = {
    for _, subnet in data.aws_subnet.default : subnet.availability_zone => subnet
  }
  selected_azs = slice(sort(keys(local.subnets_by_az)), 0, 3)
  node_names   = ["node1", "node2", "node3"]
  nodes = {
    for index, az in local.selected_azs : local.node_names[index] => local.subnets_by_az[az]
  }
  tags = {
    Project   = var.name
    ManagedBy = "terraform"
    Phase     = "6-ha-lab"
  }
}

resource "aws_iam_role" "instance" {
  name = "${var.name}-ssm"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "ec2.amazonaws.com"
      }
    }]
  })

  tags = local.tags
}

resource "aws_iam_role_policy_attachment" "ssm" {
  role       = aws_iam_role.instance.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "instance" {
  name = "${var.name}-ssm"
  role = aws_iam_role.instance.name
}

resource "aws_security_group" "nodes" {
  name        = "${var.name}-nodes"
  description = "Scheduler HA lab nodes: cluster traffic only"
  vpc_id      = data.aws_vpc.default.id

  # etcd, Patroni/PostgreSQL, RabbitMQ, Redis, HAProxy, metrics, API.
  ingress {
    description = "Cluster-internal traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    self        = true
  }

  ingress {
    description = "API target health checks and VPC clients"
    from_port   = 3000
    to_port     = 3000
    protocol    = "tcp"
    cidr_blocks = [data.aws_vpc.default.cidr_block]
  }

  ingress {
    description = "API clients through the per-node HAProxy"
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = [var.allowed_cidr]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = local.tags
}

resource "aws_security_group" "node1_operator" {
  name        = "${var.name}-node1-operator"
  description = "Operator access to the API load balancer, monitoring, and RabbitMQ management on node1"
  vpc_id      = data.aws_vpc.default.id

  dynamic "ingress" {
    for_each = var.exposed_ports
    content {
      description = "Operator access to port ${ingress.value}"
      from_port   = ingress.value
      to_port     = ingress.value
      protocol    = "tcp"
      cidr_blocks = [var.allowed_cidr]
    }
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(local.tags, { Name = "${var.name}-node1-operator" })
}

resource "aws_s3_bucket" "artifacts" {
  bucket        = "${var.name}-artifacts-${random_id.suffix.hex}"
  force_destroy = true

  tags = local.tags
}

resource "aws_s3_bucket_public_access_block" "artifacts" {
  bucket                  = aws_s3_bucket.artifacts.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_instance" "node" {
  for_each = local.nodes

  ami           = data.aws_ssm_parameter.al2023.value
  instance_type = var.instance_type
  subnet_id     = each.value.id
  vpc_security_group_ids = concat(
    [aws_security_group.nodes.id],
    each.key == "node1" ? [aws_security_group.node1_operator.id] : [],
  )
  iam_instance_profile        = aws_iam_instance_profile.instance.name
  associate_public_ip_address = true

  # The account's on-demand vCPU quota is already consumed by the existing
  # single-node demo and another workload. The lab uses the separate Spot
  # quota, which also keeps the 3-node run inexpensive.
  instance_market_options {
    market_type = "spot"

    spot_options {
      instance_interruption_behavior = "terminate"
      spot_instance_type             = "one-time"
    }
  }

  root_block_device {
    volume_size           = var.root_volume_gb
    volume_type           = "gp3"
    encrypted             = true
    delete_on_termination = true
  }

  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 2
  }

  user_data = <<-EOT
    #!/bin/bash
    set -euxo pipefail

    dnf install -y docker tar gzip
    systemctl enable --now docker
    usermod -aG docker ec2-user

    # The standalone curl package conflicts with Amazon Linux's curl-minimal.
    # Use curl-minimal for the release downloads below.
    mkdir -p /usr/local/lib/docker/cli-plugins
    curl -fsSL https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64 \
      -o /usr/local/lib/docker/cli-plugins/docker-compose
    chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
    ln -sf /usr/local/lib/docker/cli-plugins/docker-compose /usr/local/bin/docker-compose

    BUILDX_URL=$(curl -fsSL https://api.github.com/repos/docker/buildx/releases/latest \
      | grep browser_download_url | grep 'linux-amd64' | head -1 | cut -d '"' -f 4)
    curl -fsSL "$BUILDX_URL" -o /usr/local/lib/docker/cli-plugins/docker-buildx
    chmod +x /usr/local/lib/docker/cli-plugins/docker-buildx

    # Build headroom for PostgreSQL, RabbitMQ, Redis, and the application.
    if ! swapon --show=NAME --noheadings | grep -qx '/swapfile'; then
      if [ ! -f /swapfile ]; then
        fallocate -l 4G /swapfile || dd if=/dev/zero of=/swapfile bs=1M count=4096
        chmod 600 /swapfile
        mkswap /swapfile
      fi
      swapon /swapfile
    fi
    grep -q '^/swapfile ' /etc/fstab || echo '/swapfile none swap sw 0 0' >> /etc/fstab

    cat >/etc/sysctl.d/99-scheduler-ha.conf <<SYSCTL
    net.core.somaxconn=4096
    fs.file-max=200000
    vm.swappiness=10
    SYSCTL
    sysctl --system

    mkdir -p /home/ec2-user/app
    chown -R ec2-user:ec2-user /home/ec2-user/app
  EOT

  tags = merge(local.tags, {
    Name = "${var.name}-${each.key}"
  })

  depends_on = [aws_iam_instance_profile.instance]
}

