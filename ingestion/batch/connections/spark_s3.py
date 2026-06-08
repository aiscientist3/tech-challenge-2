"""
SparkSession factory with S3 read/write support via IAM Instance Profile.

On Databricks, the active session is reused as-is.
On local/dev environments, a new session is created with Delta Lake and s3a support.
AWS credentials are resolved through the Default Credential Provider Chain
(IAM Instance Profile on EC2/Databricks) — no access keys in code.
"""

from __future__ import annotations

import logging
import os

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def get_or_create_spark_session(
    app_name: str = "tech-challenge-bronze-ingestion",
) -> SparkSession:
    """
    Return the active SparkSession (Databricks) or create a new one (local/dev).

    Args:
        app_name: Application name used when creating a new session.
    """
    active = SparkSession.getActiveSession()
    if active is not None:
        logger.info("Reusing active Databricks SparkSession.")
        return active

    logger.info("No active session found — creating new SparkSession for local/dev.")

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        )
    )

    aws_region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
    builder = builder.config("spark.hadoop.fs.s3a.endpoint.region", aws_region)

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
