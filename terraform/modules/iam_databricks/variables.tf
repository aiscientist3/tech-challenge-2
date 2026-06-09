variable "project_name" {
  type = string
}

variable "environment" {
  type = string
}

variable "bucket_arn" {
  type = string
}

variable "bucket_name" {
  type = string
}

variable "tags" {
  type = map(string)
}

locals {
  name_prefix = "${var.project_name}-${var.environment}"

  s3_policy_document = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "DatabricksS3Access"
        Effect = "Allow"
        Action = [
          "s3:ListBucket",
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
        ]
        Resource = [
          var.bucket_arn,
          "${var.bucket_arn}/*",
        ]
      },
    ]
  })
}
