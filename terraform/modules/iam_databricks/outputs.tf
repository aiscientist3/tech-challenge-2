output "iam_user_name" {
  description = "Name of the IAM user for programmatic S3 access."
  value       = aws_iam_user.databricks_s3.name
}

output "iam_user_arn" {
  description = "ARN of the IAM user."
  value       = aws_iam_user.databricks_s3.arn
}

output "access_key_id" {
  description = "Access key ID for the IAM user."
  value       = aws_iam_access_key.databricks_s3.id
  sensitive   = true
}

output "secret_access_key" {
  description = "Secret access key for the IAM user."
  value       = aws_iam_access_key.databricks_s3.secret
  sensitive   = true
}

output "instance_profile_arn" {
  description = "ARN of the IAM instance profile."
  value       = aws_iam_instance_profile.databricks_s3.arn
}

output "instance_profile_name" {
  description = "Name of the IAM instance profile."
  value       = aws_iam_instance_profile.databricks_s3.name
}

output "iam_role_arn" {
  description = "ARN of the IAM role attached to the instance profile."
  value       = aws_iam_role.databricks_s3.arn
}
