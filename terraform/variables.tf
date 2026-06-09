variable "project_name" {
  description = "Project name used as prefix for AWS resources."
  type        = string
  default     = "tech-challenge-2"
}

variable "environment" {
  description = "Deployment environment (e.g. dev, prod)."
  type        = string
  default     = "dev"
}

variable "aws_region" {
  description = "AWS region for the datalake bucket and IAM resources."
  type        = string
  default     = "us-east-1"
}

variable "bucket_name" {
  description = "Optional explicit S3 bucket name. When empty, a name is derived from project_name and environment."
  type        = string
  default     = ""
}

variable "bronze_prefix" {
  description = "S3 key prefix for the Bronze layer."
  type        = string
  default     = "bronze/br_inep_alfabetizacao"
}

variable "silver_prefix" {
  description = "S3 key prefix for the Silver layer."
  type        = string
  default     = "silver"
}

variable "gold_prefix" {
  description = "S3 key prefix for the Gold layer."
  type        = string
  default     = "gold"
}

variable "bronze_transition_ia_days" {
  description = "Days before Bronze objects transition to STANDARD_IA."
  type        = number
  default     = 90
}

variable "enable_bronze_expiration" {
  description = "Whether to expire Bronze objects after bronze_expiration_days."
  type        = bool
  default     = false
}

variable "bronze_expiration_days" {
  description = "Days before Bronze objects are permanently deleted (only when enable_bronze_expiration is true)."
  type        = number
  default     = 365
}

variable "databricks_host" {
  description = "Databricks workspace URL (e.g. https://xxx.cloud.databricks.com)."
  type        = string
}

variable "databricks_token" {
  description = "Databricks personal access token. Pass via TF_VAR_databricks_token — never commit."
  type        = string
  sensitive   = true
}

variable "git_repo_url" {
  description = "Git repository URL for the batch ingestion Python script."
  type        = string
}

variable "git_branch" {
  description = "Git branch used by the Databricks job."
  type        = string
  default     = "main"
}

variable "git_provider" {
  description = "Git provider for Databricks git_source (gitHub, gitLab, bitbucketCloud, azureDevOpsServices)."
  type        = string
  default     = "gitHub"
}

variable "job_name" {
  description = "Name of the Databricks batch ingestion job."
  type        = string
  default     = "bronze-batch-ingestion"
}

variable "job_parameters" {
  description = "Flat list of CLI parameters passed to ingestion/batch/main.py."
  type        = list(string)
  default     = ["--sources", "all", "--years", "2023,2024"]
}

variable "job_python_file" {
  description = "Relative path to the Python entry point inside the Git repository."
  type        = string
  default     = "ingestion/batch/main.py"
}

variable "job_timeout_seconds" {
  description = "Maximum runtime for the Databricks job task."
  type        = number
  default     = 3600
}

variable "job_pypi_dependencies" {
  description = "PyPI packages installed in the Serverless job environment."
  type        = list(string)
  default = [
    # Pin numeric stack first — avoids numpy/pyarrow clash with Databricks runtime
    "numpy==1.26.4",
    "pyarrow==15.0.2",
    "pandas==2.2.3",
    "db-dtypes==1.3.1",
    "google-auth>=2.29.0",
    "google-cloud-bigquery>=3.25.0",
    "basedosdados>=2.0.0",
    "deltalake>=0.18.0",
  ]
}

variable "job_environment_version" {
  description = "Serverless environment version for the Databricks job (use 2+, not deprecated client=1)."
  type        = string
  default     = "2"
}

variable "enable_job_schedule" {
  description = "Whether to enable a cron schedule for the Databricks job."
  type        = bool
  default     = false
}

variable "job_schedule_cron" {
  description = "Quartz cron expression for scheduled job runs."
  type        = string
  default     = "0 0 6 * * ?"
}

variable "register_instance_profile" {
  description = "Register the IAM instance profile in the Databricks workspace. Disable for Serverless-only setups."
  type        = bool
  default     = false
}

variable "tags" {
  description = "Additional tags applied to all AWS resources."
  type        = map(string)
  default     = {}
}
