"""
Centralised configuration for the batch ingestion pipeline (Bronze layer).

All values can be overridden via environment variables or CLI arguments.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Optional


# ---------------------------------------------------------------------------
# GCP / BigQuery
# ---------------------------------------------------------------------------
GCP_PROJECT_ID: str = os.getenv("GCP_PROJECT_ID", "fase-2-tech-challenge-498820")
BIGQUERY_BILLING_PROJECT: str = os.getenv("BIGQUERY_BILLING_PROJECT", GCP_PROJECT_ID)
BIGQUERY_PUBLIC_DATASET: str = "basedosdados"

# ---------------------------------------------------------------------------
# AWS / S3
# ---------------------------------------------------------------------------
S3_BUCKET: str = os.getenv("S3_BUCKET", "tech-challenge-2-datalake-781863100038-us-east-1-an")
BRONZE_PREFIX: str = os.getenv("BRONZE_PREFIX", "bronze/br_inep_alfabetizacao")

# ---------------------------------------------------------------------------
# Databricks Secret Scope — GCP
# ---------------------------------------------------------------------------
DATABRICKS_SECRET_SCOPE: str = os.getenv("DATABRICKS_SECRET_SCOPE", "gcp")
DATABRICKS_SECRET_KEY: str = os.getenv("DATABRICKS_SECRET_KEY", "service-account-json")

# ---------------------------------------------------------------------------
# Databricks Secret Scope — AWS
# ---------------------------------------------------------------------------
AWS_SECRET_SCOPE: str = os.getenv("AWS_SECRET_SCOPE", "aws")
AWS_ACCESS_KEY_ID_SECRET: str = os.getenv("AWS_ACCESS_KEY_ID_SECRET", "access-key-id")
AWS_SECRET_ACCESS_KEY_SECRET: str = os.getenv("AWS_SECRET_ACCESS_KEY_SECRET", "secret-access-key")

# ---------------------------------------------------------------------------
# Runtime defaults
# ---------------------------------------------------------------------------
DEFAULT_YEARS: list[int] = [2023, 2024]
DEFAULT_RETRY_ATTEMPTS: int = 3
DEFAULT_RETRY_DELAY_SECONDS: float = 5.0

# Optional row limit to reduce BigQuery cost during development (None = no limit)
DEV_ROW_LIMIT: Optional[int] = (
    int(os.getenv("DEV_ROW_LIMIT")) if os.getenv("DEV_ROW_LIMIT") else None
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


def bronze_s3_path(source_name: str) -> str:
    """Return the full S3 path for a Bronze source."""
    return f"s3://{S3_BUCKET}/{BRONZE_PREFIX}/{source_name}"


SOURCE_CONFIGS: dict[str, SourceConfig] = {
    "uf": SourceConfig(
        name="uf",
        bq_table=f"{BIGQUERY_PUBLIC_DATASET}.br_bd_diretorios_brasil.uf",
        bronze_path=bronze_s3_path("uf"),
        partition_by=None,
        filter_by_year=False,
        required_columns=("sigla", "nome"),
        description="State (UF) reference directory.",
    ),
    "municipio": SourceConfig(
        name="municipio",
        bq_table=f"{BIGQUERY_PUBLIC_DATASET}.br_bd_diretorios_brasil.municipio",
        bronze_path=bronze_s3_path("municipio"),
        partition_by=None,
        filter_by_year=False,
        required_columns=("id_municipio", "nome", "sigla_uf"),
        description="Municipality territorial reference directory.",
    ),
    "meta_brasil": SourceConfig(
        name="meta_brasil",
        bq_table=f"{BIGQUERY_PUBLIC_DATASET}.br_inep_avaliacao_alfabetizacao.brasil",
        bronze_path=bronze_s3_path("meta_brasil"),
        partition_by="ano",
        filter_by_year=True,
        required_columns=("ano",),
        description="National literacy target and indicator.",
    ),
    "meta_uf": SourceConfig(
        name="meta_uf",
        bq_table=f"{BIGQUERY_PUBLIC_DATASET}.br_inep_avaliacao_alfabetizacao.uf",
        bronze_path=bronze_s3_path("meta_uf"),
        partition_by="ano",
        filter_by_year=True,
        required_columns=("ano", "sigla_uf"),
        description="Literacy target and indicator per state (UF).",
    ),
    "meta_municipio": SourceConfig(
        name="meta_municipio",
        bq_table=f"{BIGQUERY_PUBLIC_DATASET}.br_inep_avaliacao_alfabetizacao.municipio",
        bronze_path=bronze_s3_path("meta_municipio"),
        partition_by="ano",
        filter_by_year=True,
        required_columns=("ano", "id_municipio"),
        description="Literacy target and indicator per municipality.",
    ),
    "alunos": SourceConfig(
        name="alunos",
        bq_table=f"{BIGQUERY_PUBLIC_DATASET}.br_inep_avaliacao_alfabetizacao.microdados",
        bronze_path=bronze_s3_path("alunos"),
        partition_by="ano",
        filter_by_year=True,
        required_columns=("ano",),
        description="Student microdata — Criança Alfabetizada indicator.",
    ),
}

ALL_SOURCE_NAMES: tuple[str, ...] = tuple(SOURCE_CONFIGS.keys())


@dataclass
class IngestionRunConfig:
    """Runtime parameters for a single ingestion execution."""

    years: list[int] = field(default_factory=lambda: list(DEFAULT_YEARS))
    sources: list[str] = field(default_factory=lambda: list(ALL_SOURCE_NAMES))
    batch_id: Optional[str] = None
    row_limit: Optional[int] = DEV_ROW_LIMIT
    overwrite: bool = True
