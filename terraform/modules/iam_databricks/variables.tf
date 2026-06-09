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

variable "enable_cloudwatch_metrics" {
  type        = bool
  default     = false
  description = "Allow the IAM user to publish custom CloudWatch metrics from the batch job."
}

locals {
  name_prefix = "${var.project_name}-${var.environment}"

  cloudwatch_policy_statement = var.enable_cloudwatch_metrics ? [
    {
      Sid    = "CloudWatchPutMetrics"
      Effect = "Allow"
      Action = ["cloudwatch:PutMetricData"]
      Resource = "*"
    },
  ] : []

  s3_policy_document = jsonencode({
    Version = "2012-10-17"
    Statement = concat(
      [
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
      ],
      local.cloudwatch_policy_statement,
    )
  })
}
