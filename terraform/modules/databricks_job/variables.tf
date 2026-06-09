variable "job_name" {
  type = string
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

variable "job_environment_version" {
  type        = string
  default     = "2"
  description = "Serverless environment version. Use 2+; avoid deprecated client=1."
}

locals {
  environment_key = "Default"
}
