"""
Silver layer data quality — validators driven by docs/catalog/entities/*.yaml.

Splits each entity DataFrame into valid rows (Silver) and quarantined rows.
Supported rule types: completude, dominio, referencial.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from ingestion.common.quality_referential import referencial_failure_mask

logger = logging.getLogger(__name__)

_CATALOG_DIR = Path(__file__).resolve().parents[2] / "docs" / "catalog" / "entities"

_DOMAIN_PATTERNS: dict[str, re.Pattern[str]] = {
    "id_municipio": re.compile(r"^\d{7}$"),
}


@dataclass(frozen=True)
class QualityRule:
    """Single quality rule loaded from entity catalog YAML."""

    id: str
    tipo: str
    descricao: str
    coluna: str | None = None
    referencia_tabela: str | None = None
    referencia_coluna: str | None = None


@dataclass
class QualityResult:
    """Outcome of validating an entity DataFrame."""

    valid_df: pd.DataFrame
    quarantine_df: pd.DataFrame
    summary: dict[str, int] = field(default_factory=dict)
    total_input: int = 0
    quarantine_count: int = 0


def _parse_rule(raw: dict[str, Any]) -> QualityRule:
    referencia = raw.get("referencia")
    referencia_tabela = None
    referencia_coluna = None
    if isinstance(referencia, str) and "." in referencia:
        referencia_tabela, referencia_coluna = referencia.split(".", 1)

    return QualityRule(
        id=str(raw["id"]),
        tipo=str(raw["tipo"]),
        descricao=str(raw.get("descricao", "")),
        coluna=raw.get("coluna"),
        referencia_tabela=referencia_tabela,
        referencia_coluna=referencia_coluna,
    )


def load_quality_rules(entity_name: str) -> list[QualityRule]:
    """Load quality rules from docs/catalog/entities/{entity_name}.yaml."""
    path = _CATALOG_DIR / f"{entity_name}.yaml"
    if not path.is_file():
        logger.warning("Quality catalog not found for '%s' at %s", entity_name, path)
        return []

    try:
        import yaml
    except ImportError as exc:
        raise ImportError(
            "PyYAML is required for quality rule loading. Install with: pip install pyyaml"
        ) from exc

    with path.open(encoding="utf-8") as handle:
        document = yaml.safe_load(handle)

    raw_rules = document.get("qualidade", {}).get("regras", [])
    return [_parse_rule(rule) for rule in raw_rules]


def _completude_mask(df: pd.DataFrame, rule: QualityRule) -> pd.Series:
    if not rule.coluna:
        raise ValueError(f"Rule '{rule.id}' (completude) missing coluna.")
    if rule.coluna not in df.columns:
        return pd.Series(True, index=df.index)
    return df[rule.coluna].isna()


def _dominio_mask(df: pd.DataFrame, rule: QualityRule) -> pd.Series:
    if not rule.coluna:
        return pd.Series(False, index=df.index)
    if rule.coluna not in df.columns:
        return pd.Series(True, index=df.index)

    series = df[rule.coluna].astype("string")
    non_null = series.notna() & (series.str.strip() != "")
    pattern = _DOMAIN_PATTERNS.get(rule.coluna)
    if pattern is None:
        return pd.Series(False, index=df.index)

    valid = series.str.match(pattern, na=False)
    return non_null & ~valid


def _referencial_mask(
    df: pd.DataFrame,
    rule: QualityRule,
    references: dict[str, pd.DataFrame],
) -> pd.Series:
    if not rule.referencia_tabela or not rule.referencia_coluna or not rule.coluna:
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
    rule: QualityRule,
    references: dict[str, pd.DataFrame],
) -> pd.Series:
    if rule.tipo == "completude":
        return _completude_mask(df, rule)
    if rule.tipo == "dominio":
        return _dominio_mask(df, rule)
    if rule.tipo == "referencial":
        return _referencial_mask(df, rule, references)
    logger.warning("Unknown quality rule type '%s' for rule '%s'.", rule.tipo, rule.id)
    return pd.Series(False, index=df.index)


def validate_entity(
    df: pd.DataFrame,
    entity_name: str,
    references: dict[str, pd.DataFrame] | None = None,
) -> QualityResult:
    """
    Validate a Silver-stage DataFrame and split valid vs quarantined rows.

    Rows failing any rule are quarantined with audit columns.
    """
    refs = references or {}
    if df.empty:
        return QualityResult(
            valid_df=df.copy(),
            quarantine_df=df.copy(),
            summary={},
            total_input=0,
            quarantine_count=0,
        )

    rules = load_quality_rules(entity_name)
    failure_mask = pd.Series(False, index=df.index)
    rule_ids_by_index: dict[Any, list[str]] = {idx: [] for idx in df.index}
    messages_by_index: dict[Any, list[str]] = {idx: [] for idx in df.index}
    summary: dict[str, int] = {}

    for rule in rules:
        try:
            fails = _rule_failure_mask(df, rule, refs)
        except ValueError as exc:
            logger.warning("Skipping rule '%s': %s", rule.id, exc)
            continue

        count = int(fails.sum())
        summary[rule.id] = count
        failure_mask |= fails
        for idx in df.index[fails]:
            rule_ids_by_index[idx].append(rule.id)
            if rule.descricao:
                messages_by_index[idx].append(rule.descricao)

    valid_df = df.loc[~failure_mask].copy().reset_index(drop=True)
    quarantine_df = df.loc[failure_mask].copy()
    if not quarantine_df.empty:
        quarantine_df["_quality_rule_ids"] = [
            ",".join(rule_ids_by_index[idx]) for idx in quarantine_df.index
        ]
        quarantine_df["_quality_messages"] = [
            " | ".join(messages_by_index[idx]) for idx in quarantine_df.index
        ]
        quarantine_df["_quarantined_at"] = datetime.now(timezone.utc).isoformat()
        quarantine_df = quarantine_df.reset_index(drop=True)

    result = QualityResult(
        valid_df=valid_df,
        quarantine_df=quarantine_df,
        summary=summary,
        total_input=len(df),
        quarantine_count=len(quarantine_df),
    )
    logger.info(
        "Quality %s: %d quarantined / %d total (%s)",
        entity_name,
        result.quarantine_count,
        result.total_input,
        ", ".join(f"{k}: {v}" for k, v in summary.items() if v > 0) or "no failures",
    )
    return result
