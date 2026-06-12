"""
Configuration for the streaming ingestion pipeline (Kafka → Bronze).

Sensitive values are resolved at runtime via Databricks Secret Scopes.
"""

from __future__ import annotations

import os
from typing import Optional

# ---------------------------------------------------------------------------
# BigQuery
# ---------------------------------------------------------------------------
BIGQUERY_PUBLIC_DATASET: str = "basedosdados"
ALUNOS_BQ_TABLE: str = (
    f"{BIGQUERY_PUBLIC_DATASET}.br_inep_avaliacao_alfabetizacao.alunos"
)

# ---------------------------------------------------------------------------
# Databricks Secret Scope — GCP
# ---------------------------------------------------------------------------
DATABRICKS_SECRET_SCOPE: str = os.getenv("DATABRICKS_SECRET_SCOPE", "gcp")
DATABRICKS_SECRET_KEY: str = os.getenv("DATABRICKS_SECRET_KEY", "service-account-json")
GCP_PROJECT_ID_SECRET_KEY: str = os.getenv("GCP_PROJECT_ID_SECRET_KEY", "project-id")

# ---------------------------------------------------------------------------
# Databricks Secret Scope — AWS / Kafka
# ---------------------------------------------------------------------------
AWS_SECRET_SCOPE: str = os.getenv("AWS_SECRET_SCOPE", "aws")
AWS_ACCESS_KEY_ID_SECRET: str = os.getenv("AWS_ACCESS_KEY_ID_SECRET", "access-key-id")
AWS_SECRET_ACCESS_KEY_SECRET: str = os.getenv(
    "AWS_SECRET_ACCESS_KEY_SECRET", "secret-access-key"
)
AWS_S3_BUCKET_SECRET_KEY: str = os.getenv("AWS_S3_BUCKET_SECRET_KEY", "s3-bucket")
KAFKA_BOOTSTRAP_SERVERS_SECRET: str = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS_SECRET", "kafka-bootstrap-servers"
)
KAFKA_TOPIC_SECRET: str = os.getenv("KAFKA_TOPIC_SECRET", "kafka-topic")

# ---------------------------------------------------------------------------
# S3 paths
# ---------------------------------------------------------------------------
BRONZE_PREFIX: str = os.getenv("BRONZE_PREFIX", "bronze/br_inep_alfabetizacao")
CHECKPOINT_PREFIX: str = os.getenv("CHECKPOINT_PREFIX", "_checkpoints/br_inep_alfabetizacao")

# ---------------------------------------------------------------------------
# Kafka
# ---------------------------------------------------------------------------
DEFAULT_KAFKA_TOPIC: str = os.getenv(
    "KAFKA_TOPIC", "br-inep-alfabetizacao.alunos.performance"
)
DEFAULT_STREAM_SOURCE: str = "alunos_stream"
EVENT_TYPE_PERFORMANCE: str = "performance_measurement"

# ---------------------------------------------------------------------------
# Runtime defaults
# ---------------------------------------------------------------------------
DEFAULT_YEARS: list[int] = [2023, 2024]
PRODUCER_MAX_RETRIES: int = int(os.getenv("PRODUCER_MAX_RETRIES", "3"))
TEST_EVENT_LIMIT: int = int(os.getenv("TEST_EVENT_LIMIT", "5"))
DEV_ROW_LIMIT: Optional[int] = (
    int(os.getenv("DEV_ROW_LIMIT")) if os.getenv("DEV_ROW_LIMIT") else None
)


def bronze_table_path(bucket: str, source_name: str, bronze_prefix: str = BRONZE_PREFIX) -> str:
    """S3 URI for a Bronze Delta table."""
    return f"s3://{bucket}/{bronze_prefix}/{source_name}"


def checkpoint_path(
    bucket: str,
    stream_name: str,
    checkpoint_prefix: str = CHECKPOINT_PREFIX,
) -> str:
    """S3 URI for a Structured Streaming checkpoint directory."""
    return f"s3://{bucket}/{checkpoint_prefix}/{stream_name}"
