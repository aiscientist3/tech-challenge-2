variable "project_name" {
  type = string
}

variable "environment" {
  type = string
}

variable "aws_region" {
  type = string
}

variable "bucket_name" {
  type = string
}

variable "alert_email" {
  type = string
}

variable "metric_namespace" {
  type = string
}

variable "job_duration_alarm_seconds" {
  type = number
}

variable "enable_s3_error_alarm" {
  type = bool
}

variable "tags" {
  type = map(string)
}

locals {
  name_prefix = "${var.project_name}-${var.environment}"
}
