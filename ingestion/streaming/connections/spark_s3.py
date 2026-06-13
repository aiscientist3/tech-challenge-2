"""SparkSession factory with S3 read/write support for streaming ingestion."""

from __future__ import annotations

import logging
import os
from typing import Any

from pyspark.sql import SparkSession

from ingestion.streaming.config import (
    AWS_ACCESS_KEY_ID_SECRET,
    AWS_SECRET_ACCESS_KEY_SECRET,
    AWS_SECRET_SCOPE,
)

logger = logging.getLogger(__name__)


def _get_dbutils() -> Any | None:
    try:
        from pyspark.dbutils import DBUtils  # type: ignore[import-untyped]

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


def _is_databricks_runtime() -> bool:
    """True when running inside a Databricks job or notebook."""
    return os.getenv("DATABRICKS_RUNTIME_VERSION") is not None


def _resolve_aws_credentials() -> tuple[str, str] | None:
    dbutils = _get_dbutils()
    if dbutils is not None:
        try:
            access_key = dbutils.secrets.get(
                scope=AWS_SECRET_SCOPE,
                key=AWS_ACCESS_KEY_ID_SECRET,
            )
            secret_key = dbutils.secrets.get(
                scope=AWS_SECRET_SCOPE,
                key=AWS_SECRET_ACCESS_KEY_SECRET,
            )
            logger.info(
                "AWS credentials loaded from Databricks Secret Scope '%s'.",
                AWS_SECRET_SCOPE,
            )
            return access_key, secret_key
        except Exception as exc:
            logger.warning(
                "Could not load AWS credentials from Secret Scope: %s", exc
            )

    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    if access_key and secret_key:
        logger.info("AWS credentials loaded from environment variables.")
        return access_key, secret_key

    logger.info(
        "No explicit AWS credentials found — falling back to Default Credential Provider Chain."
    )
    return None


def _inject_aws_env_credentials(credentials: tuple[str, str]) -> None:
    """Expose AWS keys to the Hadoop/S3 SDK via process environment variables."""
    access_key, secret_key = credentials
    os.environ["AWS_ACCESS_KEY_ID"] = access_key
    os.environ["AWS_SECRET_ACCESS_KEY"] = secret_key
    os.environ.setdefault("AWS_DEFAULT_REGION", os.getenv("AWS_DEFAULT_REGION", "us-east-1"))


def _configure_s3_spark_conf(
    spark: SparkSession,
    credentials: tuple[str, str],
) -> None:
    """Inject S3A settings via Spark conf (local/dev only — blocked on Serverless Connect)."""
    access_key, secret_key = credentials
    spark.conf.set("spark.hadoop.fs.s3a.access.key", access_key)
    spark.conf.set("spark.hadoop.fs.s3a.secret.key", secret_key)
    spark.conf.set(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )
    spark.conf.set(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem",
    )


def s3_path_for_spark(path: str) -> str:
    """
    Normalize S3 URIs for Spark Hadoop writes.

    Spark Structured Streaming on Databricks expects the s3a:// scheme.
    """
    if path.startswith("s3://"):
        return "s3a://" + path[len("s3://") :]
    return path


def get_or_create_spark_session(
    app_name: str = "tech-challenge-streaming-bronze",
) -> SparkSession:
    """Return the active SparkSession (Databricks) or create a new one (local/dev)."""
    credentials = _resolve_aws_credentials()
    aws_region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

    if credentials is not None:
        _inject_aws_env_credentials(credentials)

    active = SparkSession.getActiveSession()
    if active is not None:
        logger.info("Reusing active Databricks SparkSession.")
        if credentials is not None and not _is_databricks_runtime():
            _configure_s3_spark_conf(active, credentials)
        elif credentials is not None:
            logger.info(
                "Databricks Serverless detected — AWS credentials set via environment "
                "(spark.hadoop fs.s3a.* conf is not available on Spark Connect)."
            )
        return active

    logger.info("No active session found — creating new SparkSession for local/dev.")

    provider = (
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        if credentials
        else "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
    )

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", provider)
        .config("spark.hadoop.fs.s3a.endpoint.region", aws_region)
    )

    if credentials:
        access_key, secret_key = credentials
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.access.key", access_key)
            .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        )

    s3_endpoint = os.getenv("S3_ENDPOINT")
    if s3_endpoint:
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
        )
        logger.info("Using custom S3 endpoint: %s", s3_endpoint)

    return builder.getOrCreate()
