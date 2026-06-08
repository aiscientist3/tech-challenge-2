"""External connections: BigQuery, S3/Spark, and AWS credentials."""

from ingestion.batch.connections.aws_credentials import resolve_aws_storage_options
from ingestion.batch.connections.bigquery_client import create_bigquery_client
from ingestion.batch.connections.spark_s3 import get_or_create_spark_session

__all__ = [
    "create_bigquery_client",
    "get_or_create_spark_session",
    "resolve_aws_storage_options",
]
