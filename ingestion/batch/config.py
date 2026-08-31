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
KAFKA_BOOTSTRAP_SERVERS_SECRET: str = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS_SECRET", "kafka-bootstrap-servers"
)
KAFKA_TOPIC_SECRET: str = os.getenv("KAFKA_TOPIC_SECRET", "kafka-topic")

# ---------------------------------------------------------------------------
# S3 path config (non-sensitive)
# ---------------------------------------------------------------------------
BRONZE_PREFIX: str = os.getenv("BRONZE_PREFIX", "bronze/br_inep_alfabetizacao")
CHECKPOINT_PREFIX: str = os.getenv("CHECKPOINT_PREFIX", "_checkpoints/br_inep_alfabetizacao")

# ---------------------------------------------------------------------------
# Kafka (streaming producer / consumer)
# ---------------------------------------------------------------------------
DEFAULT_KAFKA_TOPIC: str = os.getenv(
    "KAFKA_TOPIC", "br-inep-alfabetizacao.alunos.performance"
)

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
    "populacao_municipio",
    "pib_municipio",
    "socioeconomico_municipio",
    "municipio_indicadores",
    "uf_indicadores",
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
    year_lookback: int = 0


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
            bq_table=f"{BIGQUERY_PUBLIC_DATASET}.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_brasil",
            bronze_path=path("meta_brasil"),
            partition_by="ano",
            filter_by_year=True,
            required_columns=("ano",),
            description="National literacy target and indicator.",
        ),
        "meta_uf": SourceConfig(
            name="meta_uf",
            bq_table=f"{BIGQUERY_PUBLIC_DATASET}.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_uf",
            bronze_path=path("meta_uf"),
            partition_by="ano",
            filter_by_year=True,
            required_columns=("ano",),
            description="Literacy target and indicator per state (UF).",
        ),
        "meta_municipio": SourceConfig(
            name="meta_municipio",
            bq_table=f"{BIGQUERY_PUBLIC_DATASET}.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_municipio",
            bronze_path=path("meta_municipio"),
            partition_by="ano",
            filter_by_year=True,
            required_columns=("ano",),
            description="Literacy target and indicator per municipality.",
        ),
        "populacao_municipio": SourceConfig(
            name="populacao_municipio",
            bq_table=f"{BIGQUERY_PUBLIC_DATASET}.br_ibge_populacao.municipio",
            bronze_path=path("populacao_municipio"),
            partition_by="ano",
            filter_by_year=True,
            year_lookback=10,
            required_columns=("ano", "id_municipio", "populacao"),
            description="IBGE municipal population estimates (as-of lookback for Gold).",
        ),
        "pib_municipio": SourceConfig(
            name="pib_municipio",
            bq_table=f"{BIGQUERY_PUBLIC_DATASET}.br_ibge_pib.municipio",
            bronze_path=path("pib_municipio"),
            partition_by="ano",
            filter_by_year=True,
            year_lookback=10,
            required_columns=("ano", "id_municipio"),
            description="IBGE municipal GDP (as-of lookback for Gold).",
        ),
        "socioeconomico_municipio": SourceConfig(
            name="socioeconomico_municipio",
            bq_table=f"{BIGQUERY_PUBLIC_DATASET}.br_ipea_avs.municipio",
            bronze_path=path("socioeconomico_municipio"),
            partition_by="ano",
            filter_by_year=False,
            required_columns=("ano", "id_municipio"),
            description="IPEA Atlas da Vulnerabilidade Social (municipal snapshot).",
        ),
        "municipio_indicadores": SourceConfig(
            name="municipio_indicadores",
            bq_table=f"{BIGQUERY_PUBLIC_DATASET}.br_inep_avaliacao_alfabetizacao.municipio",
            bronze_path=path("municipio_indicadores"),
            partition_by="ano",
            filter_by_year=True,
            year_lookback=1,
            required_columns=("ano", "id_municipio"),
            description="INEP literacy assessment indicators per municipality (lagged in Gold).",
        ),
        "uf_indicadores": SourceConfig(
            name="uf_indicadores",
            bq_table=f"{BIGQUERY_PUBLIC_DATASET}.br_inep_avaliacao_alfabetizacao.uf",
            bronze_path=path("uf_indicadores"),
            partition_by="ano",
            filter_by_year=True,
            year_lookback=1,
            required_columns=("ano", "sigla_uf"),
            description="INEP literacy assessment indicators per UF (lagged in Gold).",
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
