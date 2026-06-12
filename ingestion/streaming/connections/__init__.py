"""Connections for the streaming ingestion pipeline."""

from ingestion.streaming.connections.aws_credentials import (
    resolve_kafka_config,
    resolve_s3_bucket,
)
from ingestion.streaming.connections.bigquery_client import create_bigquery_client
from ingestion.streaming.connections.spark_s3 import get_or_create_spark_session

__all__ = [
    "create_bigquery_client",
    "get_or_create_spark_session",
    "resolve_kafka_config",
    "resolve_s3_bucket",
]
