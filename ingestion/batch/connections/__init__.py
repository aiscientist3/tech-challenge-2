"""External connections: BigQuery and S3/Spark."""

from ingestion.batch.connections.bigquery_client import create_bigquery_client
from ingestion.batch.connections.spark_s3 import get_or_create_spark_session

__all__ = ["create_bigquery_client", "get_or_create_spark_session"]
