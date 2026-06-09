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
  type    = string
  default = ""
}

variable "bronze_prefix" {
  type = string
}

variable "silver_prefix" {
  type = string
}

variable "gold_prefix" {
  type = string
}

variable "bronze_transition_ia_days" {
  type = number
}

variable "enable_bronze_expiration" {
  type = bool
}

variable "bronze_expiration_days" {
  type = number
}

variable "tags" {
  type = map(string)
}

locals {
  bucket_name = var.bucket_name != "" ? var.bucket_name : "${var.project_name}-datalake-${var.environment}"
}
