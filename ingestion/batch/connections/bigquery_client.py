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

from ingestion.common.dbutils import get_dbutils
from ingestion.batch.config import (
    DATABRICKS_SECRET_KEY,
    DATABRICKS_SECRET_SCOPE,
    GCP_PROJECT_ID_SECRET_KEY,
)

logger = logging.getLogger(__name__)


def _credentials_from_databricks_secrets() -> service_account.Credentials | None:
    """Load service account JSON from the configured Databricks Secret Scope."""
    dbutils = get_dbutils()
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


def _resolve_gcp_project_id() -> str | None:
    """Resolve GCP project ID from Databricks Secrets or environment variable."""
    dbutils = get_dbutils()
    if dbutils is not None:
        try:
            project_id = dbutils.secrets.get(
                scope=DATABRICKS_SECRET_SCOPE, key=GCP_PROJECT_ID_SECRET_KEY
            )
            logger.info(
                "GCP project ID loaded from Databricks Secret Scope '%s'.",
                DATABRICKS_SECRET_SCOPE,
            )
            return project_id
        except Exception as exc:
            logger.warning("Could not load GCP project ID from Secret Scope: %s", exc)

    return os.getenv("GCP_PROJECT_ID")


def create_bigquery_client(project_id: Optional[str] = None) -> bigquery.Client:
    """
    Build and return an authenticated BigQuery client.

    Args:
        project_id: Billing project override. Resolved from Databricks Secrets
                    or GCP_PROJECT_ID env var when not provided.

    Raises:
        RuntimeError: When no valid credentials or project ID are found.
    """
    billing_project = project_id or _resolve_gcp_project_id()

    if not billing_project:
        raise RuntimeError(
            "GCP project ID not found. Configure the Databricks Secret Scope "
            f"('{DATABRICKS_SECRET_SCOPE}/{GCP_PROJECT_ID_SECRET_KEY}') or set "
            "the GCP_PROJECT_ID environment variable."
        )

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
