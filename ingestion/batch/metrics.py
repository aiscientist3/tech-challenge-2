"""
CloudWatch custom metrics for batch ingestion monitoring.

Metrics are published to the namespace configured via CLOUDWATCH_METRIC_NAMESPACE
(default: TechChallenge2/BatchIngestion). Alarms and dashboards are provisioned
by Terraform (terraform/modules/monitoring).
"""

from __future__ import annotations

import logging
import os
from typing import Mapping

logger = logging.getLogger(__name__)

DEFAULT_METRIC_NAMESPACE = "TechChallenge2/BatchIngestion"


def publish_ingestion_metrics(
    *,
    batch_id: str,
    environment: str,
    record_counts: Mapping[str, int],
    duration_seconds: float,
    success: bool,
) -> None:
    """
    Publish batch ingestion metrics to CloudWatch.

    Failures are logged but never interrupt the ingestion pipeline.
    """
    if os.getenv("DISABLE_CLOUDWATCH_METRICS", "").lower() in {"1", "true", "yes"}:
        logger.debug("CloudWatch metrics publishing disabled.")
        return

    namespace = os.getenv("CLOUDWATCH_METRIC_NAMESPACE", DEFAULT_METRIC_NAMESPACE)
    region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
    total_records = sum(record_counts.values())
    sources_processed = len(record_counts)

    try:
        import boto3

        cloudwatch = boto3.client("cloudwatch", region_name=region)
        timestamp = None  # use current time

        metric_data = [
            {
                "MetricName": "RecordsIngested",
                "Dimensions": [{"Name": "Environment", "Value": environment}],
                "Value": float(total_records),
                "Unit": "Count",
            },
            {
                "MetricName": "DurationSeconds",
                "Dimensions": [{"Name": "Environment", "Value": environment}],
                "Value": float(duration_seconds),
                "Unit": "Seconds",
            },
            {
                "MetricName": "SourcesProcessed",
                "Dimensions": [{"Name": "Environment", "Value": environment}],
                "Value": float(sources_processed),
                "Unit": "Count",
            },
            {
                "MetricName": "JobFailure",
                "Dimensions": [{"Name": "Environment", "Value": environment}],
                "Value": 0.0 if success else 1.0,
                "Unit": "Count",
            },
        ]

        for source_name, count in record_counts.items():
            metric_data.append(
                {
                    "MetricName": "RecordsBySource",
                    "Dimensions": [
                        {"Name": "Environment", "Value": environment},
                        {"Name": "Source", "Value": source_name},
                    ],
                    "Value": float(count),
                    "Unit": "Count",
                }
            )

        cloudwatch.put_metric_data(
            Namespace=namespace,
            MetricData=metric_data,
        )
        logger.info(
            "CloudWatch metrics published: namespace=%s batch_id=%s records=%d duration=%.1fs success=%s",
            namespace,
            batch_id,
            total_records,
            duration_seconds,
            success,
        )
    except Exception as exc:
        logger.warning("Could not publish CloudWatch metrics: %s", exc)
