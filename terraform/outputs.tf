output "s3_bucket_name" {
  description = "Name of the datalake S3 bucket."
  value       = module.s3_datalake.bucket_name
}

output "s3_bucket_arn" {
  description = "ARN of the datalake S3 bucket."
  value       = module.s3_datalake.bucket_arn
}

output "bronze_prefix" {
  description = "S3 key prefix for the Bronze layer."
  value       = module.s3_datalake.bronze_prefix
}

output "iam_user_name" {
  description = "IAM user for Databricks Serverless S3 access (Secret Scope credentials)."
  value       = module.iam_databricks.iam_user_name
}

output "iam_user_arn" {
  description = "ARN of the IAM user for programmatic S3 access."
  value       = module.iam_databricks.iam_user_arn
}

output "access_key_id" {
  description = "AWS access key ID for the Databricks IAM user. Sync to Databricks Secret Scope 'aws/access-key-id'."
  value       = module.iam_databricks.access_key_id
  sensitive   = true
}

output "secret_access_key" {
  description = "AWS secret access key for the Databricks IAM user. Sync to Databricks Secret Scope 'aws/secret-access-key'."
  value       = module.iam_databricks.secret_access_key
  sensitive   = true
}

output "instance_profile_arn" {
  description = "ARN of the IAM instance profile for classic Databricks clusters."
  value       = module.iam_databricks.instance_profile_arn
}

output "instance_profile_name" {
  description = "Name of the IAM instance profile."
  value       = module.iam_databricks.instance_profile_name
}

output "databricks_job_id" {
  description = "ID of the Bronze batch ingestion Databricks job."
  value       = module.databricks_job.job_id
}

output "databricks_job_url" {
  description = "URL of the Bronze batch ingestion Databricks job in the workspace UI."
  value       = module.databricks_job.job_url
}

output "databricks_instance_profile_id" {
  description = "Databricks workspace ID of the registered instance profile (when enabled)."
  value       = module.databricks_job.databricks_instance_profile_id
}

output "secrets_sync_commands" {
  description = "Post-apply commands to sync non-GCP secrets into Databricks (GCP secrets remain manual)."
  value       = <<-EOT
    databricks secrets put --scope aws --key s3-bucket --string-value "${module.s3_datalake.bucket_name}"
    databricks secrets put --scope aws --key access-key-id --string-value "<run: terraform output -raw access_key_id>"
    databricks secrets put --scope aws --key secret-access-key --string-value "<run: terraform output -raw secret_access_key>"
    databricks secrets put --scope aws --key kafka-bootstrap-servers --string-value "<EC2_KAFKA_IP>:9092"
    databricks secrets put --scope aws --key kafka-topic --string-value "br-inep-alfabetizacao.alunos.performance"
  EOT
}

output "databricks_streaming_job_id" {
  description = "ID of the Bronze streaming ingestion Databricks job."
  value       = var.enable_streaming_job ? module.databricks_job_streaming[0].job_id : null
}

output "databricks_streaming_job_url" {
  description = "URL of the Bronze streaming ingestion Databricks job."
  value       = var.enable_streaming_job ? module.databricks_job_streaming[0].job_url : null
}

output "monitoring_sns_topic_arn" {
  description = "SNS topic ARN for pipeline alerts (when monitoring is enabled)."
  value       = var.enable_monitoring ? module.monitoring[0].sns_topic_arn : null
}

output "monitoring_dashboard_url" {
  description = "CloudWatch dashboard URL for batch ingestion metrics."
  value       = var.enable_monitoring ? module.monitoring[0].cloudwatch_dashboard_url : null
}
