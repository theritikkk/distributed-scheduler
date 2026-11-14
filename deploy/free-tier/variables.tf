variable "aws_region" {
  description = "AWS region for the demo"
  type        = string
  default     = "ap-south-1"
}

variable "name" {
  description = "Resource name prefix"
  type        = string
  default     = "scheduler-free-tier"
}

variable "instance_type" {
  description = "Free-tier-eligible EC2 instance type"
  type        = string
  default     = "m7i-flex.large"
}

variable "root_volume_gb" {
  description = "Encrypted gp3 root volume size"
  type        = number
  default     = 30
}

variable "allowed_cidr" {
  description = "CIDR allowed to reach API and monitoring ports"
  type        = string
  default     = "103.233.119.122/32"
}

variable "exposed_ports" {
  description = "Ports opened to allowed_cidr"
  type        = list(number)
  default     = [3000, 3001, 9091, 15672]
}
