output "sns_topic_arn" {
  description = "ARN of the SNS topic used for pipeline alerts."
  value       = aws_sns_topic.pipeline_alerts.arn
}

output "sns_topic_name" {
  description = "Name of the SNS topic used for pipeline alerts."
  value       = aws_sns_topic.pipeline_alerts.name
}

output "cloudwatch_dashboard_name" {
  description = "CloudWatch dashboard name for batch ingestion metrics."
  value       = aws_cloudwatch_dashboard.batch_ingestion.dashboard_name
}

output "cloudwatch_dashboard_url" {
  description = "URL to open the CloudWatch dashboard in the AWS console."
  value       = "https://${var.aws_region}.console.aws.amazon.com/cloudwatch/home?region=${var.aws_region}#dashboards:name=${aws_cloudwatch_dashboard.batch_ingestion.dashboard_name}"
}
