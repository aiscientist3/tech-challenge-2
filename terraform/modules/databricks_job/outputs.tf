output "job_id" {
  description = "Databricks job ID."
  value       = databricks_job.bronze_batch.id
}

output "job_url" {
  description = "URL to open the job in the Databricks workspace."
  value       = databricks_job.bronze_batch.url
}

output "databricks_instance_profile_id" {
  description = "Databricks workspace ID of the registered instance profile."
  value       = var.register_instance_profile ? databricks_instance_profile.this[0].id : null
}
