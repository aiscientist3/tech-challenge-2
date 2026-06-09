resource "aws_sns_topic" "pipeline_alerts" {
  name = "${local.name_prefix}-pipeline-alerts"

  tags = merge(var.tags, {
    Name = "${local.name_prefix}-pipeline-alerts"
  })
}

resource "aws_sns_topic_subscription" "email" {
  topic_arn = aws_sns_topic.pipeline_alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

resource "aws_s3_bucket_metric" "datalake" {
  count = var.enable_s3_error_alarm ? 1 : 0

  bucket = var.bucket_name
  name   = "EntireBucket"
}

resource "aws_cloudwatch_metric_alarm" "job_failure" {
  alarm_name          = "${local.name_prefix}-batch-ingestion-failure"
  alarm_description   = "Bronze batch ingestion reported a failure (custom metric JobFailure >= 1)."
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "JobFailure"
  namespace           = var.metric_namespace
  period              = 300
  statistic           = "Maximum"
  threshold           = 1
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.pipeline_alerts.arn]
  ok_actions          = [aws_sns_topic.pipeline_alerts.arn]

  tags = var.tags
}

resource "aws_cloudwatch_metric_alarm" "job_duration" {
  alarm_name          = "${local.name_prefix}-batch-ingestion-slow"
  alarm_description   = "Bronze batch ingestion exceeded the expected duration threshold."
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "DurationSeconds"
  namespace           = var.metric_namespace
  period              = 300
  statistic           = "Maximum"
  threshold           = var.job_duration_alarm_seconds
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.pipeline_alerts.arn]

  tags = var.tags
}

resource "aws_cloudwatch_metric_alarm" "zero_records" {
  alarm_name          = "${local.name_prefix}-batch-ingestion-zero-records"
  alarm_description   = "Bronze batch ingestion completed with zero records ingested."
  comparison_operator = "LessThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "RecordsIngested"
  namespace           = var.metric_namespace
  period              = 300
  statistic           = "Maximum"
  threshold           = 0
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.pipeline_alerts.arn]

  tags = var.tags
}

resource "aws_cloudwatch_metric_alarm" "s3_5xx_errors" {
  count = var.enable_s3_error_alarm ? 1 : 0

  alarm_name          = "${local.name_prefix}-s3-5xx-errors"
  alarm_description   = "S3 datalake bucket is returning server-side errors."
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "5xxErrors"
  namespace           = "AWS/S3"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.pipeline_alerts.arn]

  dimensions = {
    BucketName = var.bucket_name
    FilterId   = aws_s3_bucket_metric.datalake[0].name
  }

  tags = var.tags
}

resource "aws_cloudwatch_dashboard" "batch_ingestion" {
  dashboard_name = "${local.name_prefix}-batch-ingestion"

  dashboard_body = jsonencode({
    widgets = [
      {
        type   = "metric"
        x      = 0
        y      = 0
        width  = 12
        height = 6
        properties = {
          title  = "Batch Ingestion — Records & Duration"
          region = var.aws_region
          metrics = [
            [var.metric_namespace, "RecordsIngested", "Environment", var.environment],
            [".", "DurationSeconds", ".", "."],
            [".", "SourcesProcessed", ".", "."],
          ]
          stat   = "Maximum"
          period = 300
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 0
        width  = 12
        height = 6
        properties = {
          title  = "Batch Ingestion — Failures"
          region = var.aws_region
          metrics = [
            [var.metric_namespace, "JobFailure", "Environment", var.environment],
          ]
          stat   = "Maximum"
          period = 300
        }
      },
    ]
  })
}
