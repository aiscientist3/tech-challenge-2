"""
Centralised configuration for the batch ingestion pipeline (Bronze layer).

Sensitive values (GCP project ID, S3 bucket) are resolved at runtime via
Databricks Secret Scopes rather than hardcoded defaults.
Static values (secret scope names, prefixes, retry settings) are read from
environment variables with safe non-sensitive defaults.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Optional


# ---------------------------------------------------------------------------
# BigQuery public dataset (never changes)
# ---------------------------------------------------------------------------
BIGQUERY_PUBLIC_DATASET: str = "basedosdados"

# ---------------------------------------------------------------------------
# Databricks Secret Scope — GCP
# ---------------------------------------------------------------------------
DATABRICKS_SECRET_SCOPE: str = os.getenv("DATABRICKS_SECRET_SCOPE", "gcp")
DATABRICKS_SECRET_KEY: str = os.getenv("DATABRICKS_SECRET_KEY", "service-account-json")
GCP_PROJECT_ID_SECRET_KEY: str = os.getenv("GCP_PROJECT_ID_SECRET_KEY", "project-id")

# ---------------------------------------------------------------------------
# Databricks Secret Scope — AWS
# ---------------------------------------------------------------------------
AWS_SECRET_SCOPE: str = os.getenv("AWS_SECRET_SCOPE", "aws")
AWS_ACCESS_KEY_ID_SECRET: str = os.getenv("AWS_ACCESS_KEY_ID_SECRET", "access-key-id")
AWS_SECRET_ACCESS_KEY_SECRET: str = os.getenv("AWS_SECRET_ACCESS_KEY_SECRET", "secret-access-key")
AWS_S3_BUCKET_SECRET_KEY: str = os.getenv("AWS_S3_BUCKET_SECRET_KEY", "s3-bucket")

# ---------------------------------------------------------------------------
# S3 path config (non-sensitive)
# ---------------------------------------------------------------------------
BRONZE_PREFIX: str = os.getenv("BRONZE_PREFIX", "bronze/br_inep_alfabetizacao")

# ---------------------------------------------------------------------------
# Runtime defaults
# ---------------------------------------------------------------------------
DEFAULT_YEARS: list[int] = [2023, 2024]
DEFAULT_RETRY_ATTEMPTS: int = 3
DEFAULT_RETRY_DELAY_SECONDS: float = 5.0

DEV_ROW_LIMIT: Optional[int] = (
    int(os.getenv("DEV_ROW_LIMIT")) if os.getenv("DEV_ROW_LIMIT") else None
)

# Names of all sources in ingestion order
ALL_SOURCE_NAMES: tuple[str, ...] = (
    "uf",
    "municipio",
    "meta_brasil",
    "meta_uf",
    "meta_municipio",
    "alunos",
)


@dataclass(frozen=True)
class SourceConfig:
    """Metadata describing a single data source for Bronze ingestion."""

    name: str
    bq_table: str
    bronze_path: str
    partition_by: Optional[str] = "ano"
    filter_by_year: bool = True
    required_columns: tuple[str, ...] = ()
    description: str = ""


def build_source_configs(
    bucket: str,
    bronze_prefix: str = BRONZE_PREFIX,
) -> dict[str, SourceConfig]:
    """
    Build SOURCE_CONFIGS at runtime with the resolved S3 bucket name.

    Args:
        bucket:        S3 bucket name (resolved from Databricks Secrets).
        bronze_prefix: S3 key prefix for the Bronze layer.
    """

    def path(source_name: str) -> str:
        return f"s3://{bucket}/{bronze_prefix}/{source_name}"

    return {
        "uf": SourceConfig(
            name="uf",
            bq_table=f"{BIGQUERY_PUBLIC_DATASET}.br_bd_diretorios_brasil.uf",
            bronze_path=path("uf"),
            partition_by=None,
            filter_by_year=False,
            required_columns=("sigla", "nome"),
            description="State (UF) reference directory.",
        ),
        "municipio": SourceConfig(
            name="municipio",
            bq_table=f"{BIGQUERY_PUBLIC_DATASET}.br_bd_diretorios_brasil.municipio",
            bronze_path=path("municipio"),
            partition_by=None,
            filter_by_year=False,
            required_columns=("id_municipio", "nome"),
            description="Municipality territorial reference directory.",
        ),
        "meta_brasil": SourceConfig(
            name="meta_brasil",
            bq_table=f"{BIGQUERY_PUBLIC_DATASET}.br_inep_avaliacao_alfabetizacao.brasil",
            bronze_path=path("meta_brasil"),
            partition_by="ano",
            filter_by_year=True,
            required_columns=("ano",),
            description="National literacy target and indicator.",
        ),
        "meta_uf": SourceConfig(
            name="meta_uf",
            bq_table=f"{BIGQUERY_PUBLIC_DATASET}.br_inep_avaliacao_alfabetizacao.uf",
            bronze_path=path("meta_uf"),
            partition_by="ano",
            filter_by_year=True,
            required_columns=("ano",),
            description="Literacy target and indicator per state (UF).",
        ),
        "meta_municipio": SourceConfig(
            name="meta_municipio",
            bq_table=f"{BIGQUERY_PUBLIC_DATASET}.br_inep_avaliacao_alfabetizacao.municipio",
            bronze_path=path("meta_municipio"),
            partition_by="ano",
            filter_by_year=True,
            required_columns=("ano",),
            description="Literacy target and indicator per municipality.",
        ),
        "alunos": SourceConfig(
            name="alunos",
            bq_table=f"{BIGQUERY_PUBLIC_DATASET}.br_inep_avaliacao_alfabetizacao.microdados",
            bronze_path=path("alunos"),
            partition_by="ano",
            filter_by_year=True,
            required_columns=("ano",),
            description="Student microdata — Criança Alfabetizada indicator.",
        ),
    }


@dataclass
class IngestionRunConfig:
    """Runtime parameters for a single ingestion execution."""

    years: list[int] = field(default_factory=lambda: list(DEFAULT_YEARS))
    sources: list[str] = field(default_factory=lambda: list(ALL_SOURCE_NAMES))
    batch_id: Optional[str] = None
    row_limit: Optional[int] = DEV_ROW_LIMIT
    overwrite: bool = True
