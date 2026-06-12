variable "job_name" {
  type = string
}

variable "job_task_key" {
  type        = string
  default     = "bronze_ingestion"
  description = "Databricks task key identifier."
}

variable "git_repo_url" {
  type = string
}

variable "git_branch" {
  type = string
}

variable "git_provider" {
  type = string
}

variable "job_python_file" {
  type = string
}

variable "job_parameters" {
  type = list(string)
}

variable "job_timeout_seconds" {
  type = number
}

variable "job_pypi_dependencies" {
  type = list(string)
}

variable "enable_job_schedule" {
  type = bool
}

variable "job_schedule_cron" {
  type = string
}

variable "register_instance_profile" {
  type = bool
}

variable "instance_profile_arn" {
  type = string
}

variable "alert_emails" {
  type        = list(string)
  default     = []
  description = "Email addresses for Databricks job failure/success notifications."
}

variable "alert_on_success" {
  type        = bool
  default     = false
  description = "Send Databricks email notification when the job succeeds."
}

variable "job_duration_warning_seconds" {
  type        = number
  default     = 0
  description = "Trigger Databricks duration warning when runtime exceeds this value (seconds). 0 disables."
}

variable "job_environment_version" {
  type        = string
  default     = "2"
  description = "Serverless environment version. Use 2+; avoid deprecated client=1."
}

locals {
  environment_key = "Default"
}
