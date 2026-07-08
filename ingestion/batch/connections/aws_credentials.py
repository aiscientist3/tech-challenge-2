"""
AWS credential and configuration resolution for use with the deltalake library.

Credential resolution order:
1. Databricks Secret Scope  (production — Serverless)
2. AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY env vars  (CI / local dev)
3. Default AWS credential chain  (``aws configure`` / ~/.aws/credentials)

Returns a storage_options dict consumed directly by deltalake.write_deltalake().
"""

from __future__ import annotations

import logging
import os

from ingestion.common.dbutils import get_dbutils
from ingestion.batch.config import (
    AWS_ACCESS_KEY_ID_SECRET,
    AWS_S3_BUCKET_SECRET_KEY,
    AWS_SECRET_ACCESS_KEY_SECRET,
    AWS_SECRET_SCOPE,
    DEFAULT_KAFKA_TOPIC,
    KAFKA_BOOTSTRAP_SERVERS_SECRET,
    KAFKA_TOPIC_SECRET,
)

logger = logging.getLogger(__name__)


def resolve_s3_bucket() -> str:
    """
    Resolve the S3 bucket name from Databricks Secrets or environment variable.

    Raises:
        RuntimeError: When the bucket name cannot be resolved.
    """
    dbutils = get_dbutils()
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


def resolve_aws_storage_options() -> dict[str, str]:
    """
    Return a storage_options dict suitable for deltalake.write_deltalake().

    Keys follow the convention expected by the delta-rs / object_store crate:
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION.

    Raises:
        RuntimeError: When no AWS credentials are found.
    """
    aws_region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

    dbutils = get_dbutils()
    if dbutils is not None:
        try:
            access_key = dbutils.secrets.get(
                scope=AWS_SECRET_SCOPE, key=AWS_ACCESS_KEY_ID_SECRET
            )
            secret_key = dbutils.secrets.get(
                scope=AWS_SECRET_SCOPE, key=AWS_SECRET_ACCESS_KEY_SECRET
            )
            logger.info(
                "AWS credentials loaded from Databricks Secret Scope '%s'.",
                AWS_SECRET_SCOPE,
            )
            return {
                "AWS_ACCESS_KEY_ID": access_key,
                "AWS_SECRET_ACCESS_KEY": secret_key,
                "AWS_REGION": aws_region,
            }
        except Exception as exc:
            logger.warning(
                "Could not load AWS credentials from Secret Scope: %s", exc
            )

    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    if access_key and secret_key:
        logger.info("AWS credentials loaded from environment variables.")
        return {
            "AWS_ACCESS_KEY_ID": access_key,
            "AWS_SECRET_ACCESS_KEY": secret_key,
            "AWS_REGION": aws_region,
        }

    profile = os.getenv("AWS_PROFILE")
    try:
        import boto3

        session = boto3.Session(profile_name=profile) if profile else boto3.Session()
        credentials = session.get_credentials()
        if credentials is not None:
            frozen = credentials.get_frozen_credentials()
            region = session.region_name or aws_region
            source = f"AWS profile '{profile}'" if profile else "default AWS credential chain"
            logger.info("AWS credentials loaded from %s.", source)
            return {
                "AWS_ACCESS_KEY_ID": frozen.access_key,
                "AWS_SECRET_ACCESS_KEY": frozen.secret_key,
                "AWS_REGION": region,
            }
    except Exception as exc:
        logger.warning("Could not load AWS credentials from default chain: %s", exc)

    raise RuntimeError(
        "AWS credentials not found. Configure the Databricks Secret Scope "
        f"('{AWS_SECRET_SCOPE}/{AWS_ACCESS_KEY_ID_SECRET}'), set "
        "AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY environment variables, "
        "or run `aws configure`."
    )


def resolve_kafka_config() -> tuple[str, str]:
    """Resolve Kafka bootstrap servers and topic from Databricks Secrets or env vars."""
    dbutils = get_dbutils()
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
        "KAFKA_BOOTSTRAP_SERVERS."
    )
