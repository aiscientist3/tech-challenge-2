"""
AWS credential resolution for use with the deltalake library.

Credential resolution order:
1. Databricks Secret Scope  (production — Serverless)
2. AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY env vars  (CI / local dev)

Returns a storage_options dict consumed directly by deltalake.write_deltalake().
"""

from __future__ import annotations

import logging
import os
from typing import Any

from ingestion.batch.config import (
    AWS_ACCESS_KEY_ID_SECRET,
    AWS_SECRET_ACCESS_KEY_SECRET,
    AWS_SECRET_SCOPE,
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


def resolve_aws_storage_options() -> dict[str, str]:
    """
    Return a storage_options dict suitable for deltalake.write_deltalake().

    Keys follow the convention expected by the delta-rs / object_store crate:
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION.

    Raises:
        RuntimeError: When no AWS credentials are found.
    """
    aws_region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

    dbutils = _get_dbutils()
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

    raise RuntimeError(
        "AWS credentials not found. Configure the Databricks Secret Scope "
        f"('{AWS_SECRET_SCOPE}/{AWS_ACCESS_KEY_ID_SECRET}') or set "
        "AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY environment variables."
    )
