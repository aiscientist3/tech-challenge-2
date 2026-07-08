"""
SparkSession factory with S3 read/write support.

Credential resolution order for AWS:
1. Databricks Secret Scope (production — Serverless / no Instance Profile)
2. AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY env vars (CI / local dev)
3. Default Credential Provider Chain (IAM Instance Profile on EC2/EMR)

On Databricks, the active session is reused and credentials are injected
via Spark hadoop configuration. On local/dev, a new session is created
with Delta Lake and s3a support.
"""

from __future__ import annotations

import logging
import os
from typing import Any

from pyspark.sql import SparkSession

from ingestion.common.dbutils import get_dbutils
from ingestion.batch.config import (
    AWS_ACCESS_KEY_ID_SECRET,
    AWS_SECRET_ACCESS_KEY_SECRET,
    AWS_SECRET_SCOPE,
)

logger = logging.getLogger(__name__)


def _resolve_aws_credentials() -> tuple[str, str] | None:
    """
    Resolve AWS credentials from Databricks Secrets or environment variables.

    Returns:
        (access_key_id, secret_access_key) tuple, or None to fall back
        to the Default Credential Provider Chain (IAM Instance Profile).
    """
    dbutils = get_dbutils()
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


def _configure_s3_credentials(
    spark: SparkSession,
    credentials: tuple[str, str] | None,
) -> None:
    """
    Inject AWS credentials into an existing SparkSession via spark.conf.set().

    Uses the Spark Connect-compatible API (no sparkContext/_jsc access),
    which is required for Databricks Serverless.
    """
    if credentials is None:
        return

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


def get_or_create_spark_session(
    app_name: str = "tech-challenge-bronze-ingestion",
) -> SparkSession:
    """
    Return the active SparkSession (Databricks) or create a new one (local/dev).
    AWS credentials are injected automatically from Databricks Secrets or env vars.

    Args:
        app_name: Application name used when creating a new session.
    """
    credentials = _resolve_aws_credentials()
    aws_region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

    active = SparkSession.getActiveSession()
    if active is not None:
        logger.info("Reusing active Databricks SparkSession.")
        _configure_s3_credentials(active, credentials)
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

    # Allow pointing at MinIO or LocalStack in development
    s3_endpoint = os.getenv("S3_ENDPOINT")
    if s3_endpoint:
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
        )
        logger.info("Using custom S3 endpoint: %s", s3_endpoint)

    return builder.getOrCreate()
