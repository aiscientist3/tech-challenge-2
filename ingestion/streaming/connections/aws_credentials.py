"""AWS and Kafka configuration resolution for streaming ingestion."""

from __future__ import annotations

import logging
import os
from typing import Any

from ingestion.streaming.config import (
    AWS_ACCESS_KEY_ID_SECRET,
    AWS_S3_BUCKET_SECRET_KEY,
    AWS_SECRET_ACCESS_KEY_SECRET,
    AWS_SECRET_SCOPE,
    DEFAULT_KAFKA_TOPIC,
    KAFKA_BOOTSTRAP_SERVERS_SECRET,
    KAFKA_TOPIC_SECRET,
)

logger = logging.getLogger(__name__)


def _get_dbutils() -> Any | None:
    try:
        from pyspark.dbutils import DBUtils  # type: ignore[import-untyped]
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        if spark is not None:
            return DBUtils(spark)
    except (ImportError, AttributeError, RuntimeError):
        pass

    try:
        import IPython  # type: ignore[import-untyped]

        return IPython.get_ipython().user_ns.get("dbutils")
    except Exception:
        return None


def resolve_s3_bucket() -> str:
    """Resolve the S3 bucket name from Databricks Secrets or environment variable."""
    dbutils = _get_dbutils()
    if dbutils is not None:
        try:
            bucket = dbutils.secrets.get(
                scope=AWS_SECRET_SCOPE, key=AWS_S3_BUCKET_SECRET_KEY
            )
            logger.info(
                "S3 bucket loaded from Databricks Secret Scope '%s'.",
                AWS_SECRET_SCOPE,
            )
            return bucket
        except Exception as exc:
            logger.warning("Could not load S3 bucket from Secret Scope: %s", exc)

    bucket = os.getenv("S3_BUCKET")
    if bucket:
        logger.info("S3 bucket loaded from S3_BUCKET environment variable.")
        return bucket

    raise RuntimeError(
        "S3 bucket name not found. Configure the Databricks Secret Scope "
        f"('{AWS_SECRET_SCOPE}/{AWS_S3_BUCKET_SECRET_KEY}') or set "
        "the S3_BUCKET environment variable."
    )


def resolve_kafka_config() -> tuple[str, str]:
    """Resolve Kafka bootstrap servers and topic from Databricks Secrets or env vars."""
    dbutils = _get_dbutils()
    if dbutils is not None:
        try:
            bootstrap = dbutils.secrets.get(
                scope=AWS_SECRET_SCOPE, key=KAFKA_BOOTSTRAP_SERVERS_SECRET
            )
            topic = dbutils.secrets.get(
                scope=AWS_SECRET_SCOPE, key=KAFKA_TOPIC_SECRET
            )
            logger.info(
                "Kafka config loaded from Databricks Secret Scope '%s'.",
                AWS_SECRET_SCOPE,
            )
            return bootstrap, topic
        except Exception as exc:
            logger.warning("Could not load Kafka config from Secret Scope: %s", exc)

    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    topic = os.getenv("KAFKA_TOPIC", DEFAULT_KAFKA_TOPIC)
    if bootstrap:
        logger.info("Kafka config loaded from environment variables.")
        return bootstrap, topic

    raise RuntimeError(
        "Kafka bootstrap servers not found. Configure the Databricks Secret Scope "
        f"('{AWS_SECRET_SCOPE}/{KAFKA_BOOTSTRAP_SERVERS_SECRET}') or set "
        "the KAFKA_BOOTSTRAP_SERVERS environment variable."
    )
