variable "aws_region" {
  description = "AWS region for the HA lab"
  type        = string
  default     = "ap-south-1"
}

variable "name" {
  description = "Resource name prefix"
  type        = string
  default     = "scheduler-ha-lab"
}

variable "instance_type" {
  description = "EC2 instance type for each HA node"
  type        = string
  default     = "m7i-flex.large"
}

variable "root_volume_gb" {
  description = "Encrypted gp3 root volume size per node"
  type        = number
  default     = 30
}

variable "allowed_cidr" {
  description = "CIDR allowed to reach the API, HAProxy stats, and monitoring ports"
  type        = string
  default     = "103.233.119.122/32"
}

variable "exposed_ports" {
  description = "Operator-facing ports exposed only on node1"
  type        = list(number)
  default     = [3001, 8081, 9091, 15672]
}
