"""
Centralised configuration for the Gold layer pipeline.

Reads Silver Delta tables from S3 and builds analytical datasets ready for BI.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Optional


SILVER_PREFIX: str = os.getenv("SILVER_PREFIX", "silver/br_inep_alfabetizacao")
GOLD_PREFIX: str = os.getenv("GOLD_PREFIX", "gold/br_inep_alfabetizacao")

DEFAULT_YEARS: list[int] = [2023, 2024]
META_GOAL_YEARS: tuple[int, ...] = tuple(range(2024, 2031))

ALL_DATASET_NAMES: tuple[str, ...] = (
    "indicador_crianca_alfabetizada_municipio",
    "indicador_crianca_alfabetizada_uf",
)


@dataclass(frozen=True)
class GoldDatasetConfig:
    """Metadata describing a Gold analytical dataset."""

    name: str
    gold_path: str
    partition_by: Optional[str] = "ano"
    description: str = ""


def build_gold_configs(
    bucket: str,
    gold_prefix: str = GOLD_PREFIX,
) -> dict[str, GoldDatasetConfig]:
    """Build Gold dataset configs at runtime with the resolved S3 bucket."""

    def gold_path(dataset_name: str) -> str:
        return f"s3://{bucket}/{gold_prefix}/{dataset_name}"

    return {
        "indicador_crianca_alfabetizada_municipio": GoldDatasetConfig(
            name="indicador_crianca_alfabetizada_municipio",
            gold_path=gold_path("indicador_crianca_alfabetizada_municipio"),
            partition_by="ano",
            description=(
                "Indicador Criança Alfabetizada por município e rede, "
                "com gap vs metas INEP."
            ),
        ),
        "indicador_crianca_alfabetizada_uf": GoldDatasetConfig(
            name="indicador_crianca_alfabetizada_uf",
            gold_path=gold_path("indicador_crianca_alfabetizada_uf"),
            partition_by="ano",
            description=(
                "Indicador Criança Alfabetizada por UF e rede, "
                "com gap vs metas INEP."
            ),
        ),
    }


def silver_table_path(
    bucket: str,
    entity: str,
    silver_prefix: str = SILVER_PREFIX,
) -> str:
    """S3 URI for a Silver Delta table."""
    return f"s3://{bucket}/{silver_prefix}/{entity}"


@dataclass
class GoldRunConfig:
    """Runtime parameters for a single Gold execution."""

    years: list[int] = field(default_factory=lambda: list(DEFAULT_YEARS))
    datasets: list[str] = field(default_factory=lambda: list(ALL_DATASET_NAMES))
    batch_id: Optional[str] = None
    overwrite: bool = True
