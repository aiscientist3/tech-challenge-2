"""Streaming-only configuration (Kafka paths, producer limits)."""

from __future__ import annotations

import os

from ingestion.batch.config import (
    BIGQUERY_PUBLIC_DATASET,
    BRONZE_PREFIX,
    CHECKPOINT_PREFIX,
    DEFAULT_KAFKA_TOPIC,
    DEFAULT_YEARS,
    DEV_ROW_LIMIT,
)

ALUNOS_BQ_TABLE: str = (
    f"{BIGQUERY_PUBLIC_DATASET}.br_inep_avaliacao_alfabetizacao.alunos"
)
DEFAULT_STREAM_SOURCE: str = "alunos"
EVENT_TYPE_PERFORMANCE: str = "performance_measurement"

PRODUCER_MAX_RETRIES: int = int(os.getenv("PRODUCER_MAX_RETRIES", "3"))
TEST_EVENT_LIMIT: int = int(os.getenv("TEST_EVENT_LIMIT", "5"))

# Unity Catalog Volume base for Structured Streaming checkpoints on Databricks
# Serverless (DBFS root disabled). Create once in SQL Editor:
#   CREATE SCHEMA IF NOT EXISTS workspace.streaming;
#   CREATE VOLUME IF NOT EXISTS workspace.streaming.streaming_checkpoints;
STREAMING_CHECKPOINT_VOLUME: str = os.getenv(
    "STREAMING_CHECKPOINT_VOLUME",
    "/Volumes/workspace/streaming/streaming_checkpoints",
)


def bronze_table_path(bucket: str, source_name: str, bronze_prefix: str = BRONZE_PREFIX) -> str:
    """S3 URI for a Bronze Delta table (same layout as batch)."""
    return f"s3://{bucket}/{bronze_prefix}/{source_name}"


def checkpoint_path(
    bucket: str,
    stream_name: str,
    checkpoint_prefix: str = CHECKPOINT_PREFIX,
) -> str:
    """S3 URI for a Structured Streaming checkpoint (local dev)."""
    return f"s3://{bucket}/{checkpoint_prefix}/{stream_name}"


def checkpoint_path_for_runtime(
    bucket: str,
    stream_name: str,
    checkpoint_prefix: str = CHECKPOINT_PREFIX,
) -> str:
    """Checkpoint path for Structured Streaming.

    Databricks Serverless: Unity Catalog Volume (DBFS root and Spark S3A blocked).
    Local dev: S3 under the datalake bucket.
    Override with STREAMING_CHECKPOINT_PATH.
    """
    override = os.getenv("STREAMING_CHECKPOINT_PATH")
    if override:
        return override.rstrip("/")
    if os.getenv("DATABRICKS_RUNTIME_VERSION") is not None:
        return f"{STREAMING_CHECKPOINT_VOLUME.rstrip('/')}/{stream_name}"
    return checkpoint_path(bucket, stream_name, checkpoint_prefix)
