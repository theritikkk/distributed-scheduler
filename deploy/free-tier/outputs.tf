output "instance_id" {
  value = aws_instance.app.id
}

output "public_ip" {
  value = aws_instance.app.public_ip
}

output "api_url" {
  value = "http://${aws_instance.app.public_ip}:3000"
}

output "artifact_bucket" {
  value = aws_s3_bucket.artifacts.bucket
}

output "allowed_cidr" {
  value = var.allowed_cidr
}

output "ssm_start_session" {
  value = "aws ssm start-session --target ${aws_instance.app.id} --region ${var.aws_region}"
}
