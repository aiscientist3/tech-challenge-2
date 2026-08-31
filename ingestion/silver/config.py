"""
Centralised configuration for the Silver layer pipeline.

Reads Bronze Delta tables from S3, applies standardisation, deduplication
and territorial joins, then writes treated tables to the Silver prefix.

S3 bucket name is resolved at runtime via Databricks Secret Scope or S3_BUCKET.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Optional


# ---------------------------------------------------------------------------
# S3 path config (non-sensitive)
# ---------------------------------------------------------------------------
BRONZE_PREFIX: str = os.getenv("BRONZE_PREFIX", "bronze/br_inep_alfabetizacao")
SILVER_PREFIX: str = os.getenv("SILVER_PREFIX", "silver/br_inep_alfabetizacao")
QUARANTINE_PREFIX: str = os.getenv(
    "QUARANTINE_PREFIX", "quarantine/br_inep_alfabetizacao"
)

# ---------------------------------------------------------------------------
# Runtime defaults
# ---------------------------------------------------------------------------
DEFAULT_YEARS: list[int] = [2023, 2024]

# Processing order — reference tables must run before entities that join them.
ALL_ENTITY_NAMES: tuple[str, ...] = (
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
    "alunos",
)

# Natural keys for deduplication (from docs/catalog/entities/*.yaml).
NATURAL_KEYS: dict[str, tuple[str, ...]] = {
    "uf": ("sigla",),
    "municipio": ("id_municipio",),
    "meta_brasil": ("ano", "rede"),
    "meta_uf": ("ano", "sigla_uf", "rede"),
    "meta_municipio": ("ano", "id_municipio", "rede"),
    "populacao_municipio": ("ano", "id_municipio"),
    "pib_municipio": ("ano", "id_municipio"),
    "socioeconomico_municipio": ("ano", "id_municipio"),
    "municipio_indicadores": ("ano", "id_municipio", "serie", "rede"),
    "uf_indicadores": ("ano", "sigla_uf", "serie", "rede"),
    "alunos": ("ano", "id_aluno"),
}

# Territorial enrichment: entity → (reference_table, left_key, right_key).
ENRICHMENT_JOINS: dict[str, tuple[str, str, str]] = {
    "meta_uf": ("uf", "sigla_uf", "sigla"),
    "meta_municipio": ("municipio", "id_municipio", "id_municipio"),
}

def quarantine_table_path(
    bucket: str,
    entity_name: str,
    *,
    layer: str = "silver",
    quarantine_prefix: str = QUARANTINE_PREFIX,
) -> str:
    """S3 URI for quarantined rows (append-only Delta)."""
    return f"s3://{bucket}/{quarantine_prefix}/{layer}/{entity_name}"


@dataclass(frozen=True)
class EntityConfig:
    """Metadata describing a single entity for Silver processing."""

    name: str
    bronze_path: str
    silver_path: str
    partition_by: Optional[str] = "ano"
    filter_by_year: bool = True
    natural_key: tuple[str, ...] = ()
    enrichment_ref: Optional[str] = None
    description: str = ""


def build_entity_configs(
    bucket: str,
    bronze_prefix: str = BRONZE_PREFIX,
    silver_prefix: str = SILVER_PREFIX,
) -> dict[str, EntityConfig]:
    """
    Build entity configs at runtime with the resolved S3 bucket name.

    Args:
        bucket:        S3 bucket name (resolved from Databricks Secrets or env).
        bronze_prefix: S3 key prefix for the Bronze layer.
        silver_prefix: S3 key prefix for the Silver layer.
    """

    def bronze_path(entity_name: str) -> str:
        return f"s3://{bucket}/{bronze_prefix}/{entity_name}"

    def silver_path(entity_name: str) -> str:
        return f"s3://{bucket}/{silver_prefix}/{entity_name}"

    def enrichment_ref(entity_name: str) -> Optional[str]:
        join = ENRICHMENT_JOINS.get(entity_name)
        return join[0] if join else None

    return {
        "uf": EntityConfig(
            name="uf",
            bronze_path=bronze_path("uf"),
            silver_path=silver_path("uf"),
            partition_by=None,
            filter_by_year=False,
            natural_key=NATURAL_KEYS["uf"],
            description="State (UF) reference directory — standardised.",
        ),
        "municipio": EntityConfig(
            name="municipio",
            bronze_path=bronze_path("municipio"),
            silver_path=silver_path("municipio"),
            partition_by=None,
            filter_by_year=False,
            natural_key=NATURAL_KEYS["municipio"],
            description="Municipality territorial reference — standardised.",
        ),
        "meta_brasil": EntityConfig(
            name="meta_brasil",
            bronze_path=bronze_path("meta_brasil"),
            silver_path=silver_path("meta_brasil"),
            partition_by="ano",
            filter_by_year=True,
            natural_key=NATURAL_KEYS["meta_brasil"],
            description="National literacy target and indicator — standardised.",
        ),
        "meta_uf": EntityConfig(
            name="meta_uf",
            bronze_path=bronze_path("meta_uf"),
            silver_path=silver_path("meta_uf"),
            partition_by="ano",
            filter_by_year=True,
            natural_key=NATURAL_KEYS["meta_uf"],
            enrichment_ref=enrichment_ref("meta_uf"),
            description="Literacy target per UF — enriched with territorial data.",
        ),
        "meta_municipio": EntityConfig(
            name="meta_municipio",
            bronze_path=bronze_path("meta_municipio"),
            silver_path=silver_path("meta_municipio"),
            partition_by="ano",
            filter_by_year=True,
            natural_key=NATURAL_KEYS["meta_municipio"],
            enrichment_ref=enrichment_ref("meta_municipio"),
            description="Literacy target per municipality — enriched with territorial data.",
        ),
        "populacao_municipio": EntityConfig(
            name="populacao_municipio",
            bronze_path=bronze_path("populacao_municipio"),
            silver_path=silver_path("populacao_municipio"),
            partition_by="ano",
            filter_by_year=False,
            natural_key=NATURAL_KEYS["populacao_municipio"],
            description="IBGE municipal population — kept with lookback years for as-of joins.",
        ),
        "pib_municipio": EntityConfig(
            name="pib_municipio",
            bronze_path=bronze_path("pib_municipio"),
            silver_path=silver_path("pib_municipio"),
            partition_by="ano",
            filter_by_year=False,
            natural_key=NATURAL_KEYS["pib_municipio"],
            description="IBGE municipal GDP — kept with lookback years for as-of joins.",
        ),
        "socioeconomico_municipio": EntityConfig(
            name="socioeconomico_municipio",
            bronze_path=bronze_path("socioeconomico_municipio"),
            silver_path=silver_path("socioeconomico_municipio"),
            partition_by="ano",
            filter_by_year=False,
            natural_key=NATURAL_KEYS["socioeconomico_municipio"],
            description="IPEA AVS municipal snapshot — latest year broadcast in Gold.",
        ),
        "municipio_indicadores": EntityConfig(
            name="municipio_indicadores",
            bronze_path=bronze_path("municipio_indicadores"),
            silver_path=silver_path("municipio_indicadores"),
            partition_by="ano",
            filter_by_year=False,
            natural_key=NATURAL_KEYS["municipio_indicadores"],
            description="INEP municipal literacy indicators — lagged one year in Gold.",
        ),
        "uf_indicadores": EntityConfig(
            name="uf_indicadores",
            bronze_path=bronze_path("uf_indicadores"),
            silver_path=silver_path("uf_indicadores"),
            partition_by="ano",
            filter_by_year=False,
            natural_key=NATURAL_KEYS["uf_indicadores"],
            description="INEP UF literacy indicators — lagged one year in Gold.",
        ),
        "alunos": EntityConfig(
            name="alunos",
            bronze_path=bronze_path("alunos"),
            silver_path=silver_path("alunos"),
            partition_by="ano",
            filter_by_year=True,
            natural_key=NATURAL_KEYS["alunos"],
            description="Student microdata — standardised and deduplicated.",
        ),
    }


@dataclass
class SilverRunConfig:
    """Runtime parameters for a single Silver execution."""

    years: list[int] = field(default_factory=lambda: list(DEFAULT_YEARS))
    entities: list[str] = field(default_factory=lambda: list(ALL_ENTITY_NAMES))
    batch_id: Optional[str] = None
    overwrite: bool = True
