output "node_ids" {
  value = { for name, node in aws_instance.node : name => node.id }
}

output "node_private_ips" {
  value = { for name, node in aws_instance.node : name => node.private_ip }
}

output "node_public_ips" {
  value = { for name, node in aws_instance.node : name => node.public_ip }
}

output "node_availability_zones" {
  value = { for name, node in aws_instance.node : name => node.availability_zone }
}

output "api_url" {
  value = "http://${aws_instance.node["node1"].public_ip}:8080"
}

output "api_urls" {
  value = {
    for name, node in aws_instance.node :
    name => "http://${node.public_ip}:8080"
  }
}

output "artifact_bucket" {
  value = aws_s3_bucket.artifacts.bucket
}

output "allowed_cidr" {
  value = var.allowed_cidr
}

output "ssm_commands" {
  value = {
    for name, node in aws_instance.node :
    name => "aws ssm start-session --target ${node.id} --region ${var.aws_region}"
  }
}
