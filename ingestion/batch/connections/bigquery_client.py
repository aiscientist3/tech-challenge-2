"""
Authenticated BigQuery client factory.

Credential resolution order:
1. Databricks Secret Scope  (production)
2. GCP_SERVICE_ACCOUNT_JSON env var  (CI / local inline)
3. GOOGLE_APPLICATION_CREDENTIALS env var  (local file path)
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Optional

from google.cloud import bigquery
from google.oauth2 import service_account

from ingestion.batch.config import (
    BIGQUERY_BILLING_PROJECT,
    DATABRICKS_SECRET_KEY,
    DATABRICKS_SECRET_SCOPE,
)

logger = logging.getLogger(__name__)


def _get_dbutils() -> Any | None:
    """Return Databricks dbutils if running inside a Databricks cluster."""
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


def _credentials_from_databricks_secrets() -> service_account.Credentials | None:
    """Load service account JSON from the configured Databricks Secret Scope."""
    dbutils = _get_dbutils()
    if dbutils is None:
        return None

    try:
        secret_json = dbutils.secrets.get(
            scope=DATABRICKS_SECRET_SCOPE,
            key=DATABRICKS_SECRET_KEY,
        )
        info = json.loads(secret_json)
        logger.info(
            "GCP credentials loaded from Databricks Secret Scope '%s'.",
            DATABRICKS_SECRET_SCOPE,
        )
        return service_account.Credentials.from_service_account_info(info)
    except Exception as exc:
        logger.warning("Could not load credentials from Secret Scope: %s", exc)
        return None


def _credentials_from_env() -> service_account.Credentials | None:
    """Fallback credential resolution for local development."""
    inline_json = os.getenv("GCP_SERVICE_ACCOUNT_JSON")
    if inline_json:
        info = json.loads(inline_json)
        logger.info("GCP credentials loaded from GCP_SERVICE_ACCOUNT_JSON env var.")
        return service_account.Credentials.from_service_account_info(info)

    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if credentials_path and os.path.exists(credentials_path):
        logger.info(
            "GCP credentials loaded from file: %s", credentials_path
        )
        return service_account.Credentials.from_service_account_file(credentials_path)

    return None


def create_bigquery_client(project_id: Optional[str] = None) -> bigquery.Client:
    """
    Build and return an authenticated BigQuery client.

    Args:
        project_id: Billing project override. Defaults to BIGQUERY_BILLING_PROJECT.

    Raises:
        RuntimeError: When no valid credentials are found.
    """
    billing_project = project_id or BIGQUERY_BILLING_PROJECT

    credentials = _credentials_from_databricks_secrets()
    if credentials is None:
        credentials = _credentials_from_env()

    if credentials is None:
        raise RuntimeError(
            "GCP credentials not found. Configure the Databricks Secret Scope "
            f"('{DATABRICKS_SECRET_SCOPE}/{DATABRICKS_SECRET_KEY}'), "
            "GCP_SERVICE_ACCOUNT_JSON, or GOOGLE_APPLICATION_CREDENTIALS."
        )

    client = bigquery.Client(credentials=credentials, project=billing_project)
    logger.info("BigQuery client ready. Billing project: %s", billing_project)
    return client
