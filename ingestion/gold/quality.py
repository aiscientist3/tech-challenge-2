"""
Gold layer cross-table quality checks — rules driven by docs/catalog/gold/*.yaml.

Supported rule types: faixa, referencial.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from ingestion.common.quality_referential import referencial_failure_mask

logger = logging.getLogger(__name__)

_CATALOG_DIR = Path(__file__).resolve().parents[2] / "docs" / "catalog" / "gold"

_OFFICIAL_RATE_COL = "taxa_alfabetizacao"


@dataclass(frozen=True)
class GoldQualityRule:
    """Single quality rule loaded from Gold catalog YAML."""

    id: str
    tipo: str
    descricao: str
    coluna: str | None = None
    min_value: float | None = None
    max_value: float | None = None
    referencia_tabela: str | None = None
    referencia_coluna: str | None = None


@dataclass
class GoldQualityResult:
    """Outcome of validating a Gold indicator DataFrame."""

    valid_df: pd.DataFrame
    quarantine_df: pd.DataFrame
    summary: dict[str, int] = field(default_factory=dict)
    total_input: int = 0
    quarantine_count: int = 0


def _parse_rule(raw: dict[str, Any]) -> GoldQualityRule:
    referencia = raw.get("referencia")
    referencia_tabela = None
    referencia_coluna = None
    if isinstance(referencia, str) and "." in referencia:
        referencia_tabela, referencia_coluna = referencia.split(".", 1)

    return GoldQualityRule(
        id=str(raw["id"]),
        tipo=str(raw["tipo"]),
        descricao=str(raw.get("descricao", "")),
        coluna=raw.get("coluna"),
        min_value=raw.get("min"),
        max_value=raw.get("max"),
        referencia_tabela=referencia_tabela,
        referencia_coluna=referencia_coluna,
    )


def load_gold_quality_rules(dataset_name: str) -> list[GoldQualityRule]:
    """Load quality rules from docs/catalog/gold/{dataset_name}.yaml."""
    path = _CATALOG_DIR / f"{dataset_name}.yaml"
    if not path.is_file():
        logger.warning("Gold quality catalog not found for '%s' at %s", dataset_name, path)
        return []

    try:
        import yaml
    except ImportError as exc:
        raise ImportError(
            "PyYAML is required for Gold quality rule loading. Install with: pip install pyyaml"
        ) from exc

    with path.open(encoding="utf-8") as handle:
        document = yaml.safe_load(handle)

    raw_rules = document.get("qualidade", {}).get("regras", [])
    return [_parse_rule(rule) for rule in raw_rules]


def _faixa_mask(df: pd.DataFrame, rule: GoldQualityRule) -> pd.Series:
    if not rule.coluna or rule.coluna not in df.columns:
        return pd.Series(False, index=df.index)

    numeric = pd.to_numeric(df[rule.coluna], errors="coerce")
    present = numeric.notna()
    min_value = 0.0 if rule.min_value is None else float(rule.min_value)
    max_value = 100.0 if rule.max_value is None else float(rule.max_value)
    return present & ((numeric < min_value) | (numeric > max_value))


def _referencial_mask(
    df: pd.DataFrame,
    rule: GoldQualityRule,
    references: dict[str, pd.DataFrame],
) -> pd.Series:
    if not rule.coluna or not rule.referencia_tabela or not rule.referencia_coluna:
        return pd.Series(False, index=df.index)

    return referencial_failure_mask(
        df,
        entity_col=rule.coluna,
        ref_table=rule.referencia_tabela,
        ref_col=rule.referencia_coluna,
        references=references,
        rule_id=rule.id,
        logger=logger,
    )


def _rule_failure_mask(
    df: pd.DataFrame,
    rule: GoldQualityRule,
    references: dict[str, pd.DataFrame],
) -> pd.Series:
    if rule.tipo == "faixa":
        return _faixa_mask(df, rule)
    if rule.tipo == "referencial":
        return _referencial_mask(df, rule, references)
    logger.warning("Unknown Gold quality rule type '%s' for rule '%s'.", rule.tipo, rule.id)
    return pd.Series(False, index=df.index)


def _attach_quarantine_metadata(
    df: pd.DataFrame,
    rule_ids: list[str],
    messages: list[str],
) -> pd.DataFrame:
    output = df.copy()
    output["_quality_rule_ids"] = ",".join(rule_ids)
    output["_quality_messages"] = " | ".join(messages)
    output["_quarantined_at"] = datetime.now(timezone.utc).isoformat()
    return output.reset_index(drop=True)


def validate_indicator(
    indicator: pd.DataFrame,
    dataset_name: str,
    references: dict[str, pd.DataFrame],
) -> GoldQualityResult:
    """Validate a Gold indicator using catalog rules and split valid vs quarantine."""
    if indicator.empty:
        return GoldQualityResult(
            valid_df=indicator.copy(),
            quarantine_df=indicator.copy(),
            summary={},
            total_input=0,
            quarantine_count=0,
        )

    rules = load_gold_quality_rules(dataset_name)
    failure_mask = pd.Series(False, index=indicator.index)
    rule_ids_by_index: dict[Any, list[str]] = {idx: [] for idx in indicator.index}
    messages_by_index: dict[Any, list[str]] = {idx: [] for idx in indicator.index}
    summary: dict[str, int] = {}

    for rule in rules:
        fails = _rule_failure_mask(indicator, rule, references)
        count = int(fails.sum())
        summary[rule.id] = count
        failure_mask |= fails
        for idx in indicator.index[fails]:
            rule_ids_by_index[idx].append(rule.id)
            if rule.descricao:
                messages_by_index[idx].append(rule.descricao)

    valid_df = indicator.loc[~failure_mask].copy().reset_index(drop=True)
    quarantine_parts: list[pd.DataFrame] = []
    for idx in indicator.index[failure_mask]:
        quarantine_parts.append(
            _attach_quarantine_metadata(
                indicator.loc[[idx]],
                rule_ids_by_index[idx],
                messages_by_index[idx],
            )
        )

    quarantine_df = (
        pd.concat(quarantine_parts, ignore_index=True)
        if quarantine_parts
        else pd.DataFrame()
    )

    result = GoldQualityResult(
        valid_df=valid_df,
        quarantine_df=quarantine_df,
        summary=summary,
        total_input=len(indicator),
        quarantine_count=len(quarantine_df),
    )
    logger.info(
        "Gold quality %s: %d quarantined / %d total (%s)",
        dataset_name,
        result.quarantine_count,
        result.total_input,
        ", ".join(f"{k}: {v}" for k, v in summary.items() if v > 0) or "no failures",
    )
    return result


def validate_indicador_municipio(
    indicator: pd.DataFrame,
    meta_municipio: pd.DataFrame,
    municipio: pd.DataFrame,
) -> GoldQualityResult:
    """Cross-table checks for the municipal Criança Alfabetizada indicator."""
    return validate_indicator(
        indicator,
        "indicador_crianca_alfabetizada_municipio",
        {"municipio": municipio, "meta_municipio": meta_municipio},
    )


def validate_indicador_uf(
    indicator: pd.DataFrame,
    meta_uf: pd.DataFrame,
    municipio: pd.DataFrame,
) -> GoldQualityResult:
    """Cross-table checks for the UF-level Criança Alfabetizada indicator."""
    return validate_indicator(
        indicator,
        "indicador_crianca_alfabetizada_uf",
        {"municipio": municipio, "meta_uf": meta_uf},
    )


def log_meta_coverage_warning(
    indicator: pd.DataFrame,
    *,
    dataset_name: str,
    official_rate_col: str = _OFFICIAL_RATE_COL,
) -> None:
    """Log rows missing official INEP rate (warning only, no quarantine)."""
    if indicator.empty or official_rate_col not in indicator.columns:
        return

    missing = int(indicator[official_rate_col].isna().sum())
    if missing > 0:
        logger.warning(
            "Gold %s: %d / %d rows without official INEP rate.",
            dataset_name,
            missing,
            len(indicator),
        )
