output "bucket_name" {
  description = "Name of the datalake S3 bucket."
  value       = aws_s3_bucket.datalake.id
}

output "bucket_arn" {
  description = "ARN of the datalake S3 bucket."
  value       = aws_s3_bucket.datalake.arn
}

output "bronze_prefix" {
  description = "Bronze layer S3 key prefix."
  value       = var.bronze_prefix
}

output "silver_prefix" {
  description = "Silver layer S3 key prefix."
  value       = var.silver_prefix
}

output "gold_prefix" {
  description = "Gold layer S3 key prefix."
  value       = var.gold_prefix
}
