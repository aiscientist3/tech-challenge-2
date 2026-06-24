"""External connections: BigQuery, S3/Spark, and AWS credentials.

Import submodules directly (e.g. ``aws_credentials``, ``bigquery_client``).
Do not eagerly import BigQuery/Spark here — Gold/Silver jobs on Databricks
Serverless may not install google-cloud-bigquery.
"""

from ingestion.batch.connections.aws_credentials import (
    resolve_aws_storage_options,
    resolve_kafka_config,
    resolve_s3_bucket,
)

__all__ = [
    "resolve_aws_storage_options",
    "resolve_kafka_config",
    "resolve_s3_bucket",
]
