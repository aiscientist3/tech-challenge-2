"""
Gold contract quality checks — rede, id_municipio and join coverage.
"""

from __future__ import annotations

import logging
import re

import pandas as pd

from ingestion.silver.transforms import REDE_CANONICAL

logger = logging.getLogger(__name__)

_ID_MUNICIPIO_RE = re.compile(r"^\d{7}$")

# Minimum join coverage for alunos_features before failing the Gold job.
MIN_JOIN_COVERAGE = 0.80


class GoldContractError(ValueError):
    """Raised when a Gold dataset violates the published contract."""


def _check_rede(df: pd.DataFrame, dataset: str) -> list[str]:
    if "rede" not in df.columns or df.empty:
        return []
    values = set(df["rede"].dropna().astype(str).str.lower().unique())
    invalid = sorted(values - set(REDE_CANONICAL))
    if invalid:
        return [
            f"{dataset}: rede has non-canonical values {invalid}. "
            f"Expected subset of {sorted(REDE_CANONICAL)}."
        ]
    # codes that slipped through
    if any(v.isdigit() for v in values):
        return [f"{dataset}: rede still contains numeric codes {sorted(values)}."]
    return []


def _check_id_municipio(df: pd.DataFrame, dataset: str) -> list[str]:
    if "id_municipio" not in df.columns or df.empty:
        return []
    series = df["id_municipio"].dropna().astype(str)
    bad = series[~series.str.match(_ID_MUNICIPIO_RE)]
    if len(bad):
        sample = bad.head(5).tolist()
        return [
            f"{dataset}: id_municipio must be 7-digit strings "
            f"({len(bad)} invalid, e.g. {sample})."
        ]
    return []


def _check_join_coverage(df: pd.DataFrame, dataset: str) -> list[str]:
    if dataset != "alunos_features" or df.empty:
        return []
    if "_join_match" in df.columns:
        coverage = float(df["_join_match"].fillna(False).mean())
    elif "nome_municipio" in df.columns:
        coverage = float(df["nome_municipio"].notna().mean())
    else:
        return [f"{dataset}: cannot evaluate join coverage (missing _join_match)."]

    if coverage < MIN_JOIN_COVERAGE:
        return [
            f"{dataset}: aluno→contexto join coverage {coverage:.1%} "
            f"below minimum {MIN_JOIN_COVERAGE:.0%}."
        ]
    return []


def _check_target(df: pd.DataFrame, dataset: str) -> list[str]:
    if dataset not in {"alunos_features", "alunos_analytic"}:
        return []
    if "alfabetizado" not in df.columns:
        return [f"{dataset}: missing target column 'alfabetizado'."]
    return []


def validate_gold_contract(dataset: str, df: pd.DataFrame) -> list[str]:
    """Return a list of contract violations (empty means OK)."""
    if df.empty:
        return [f"{dataset}: DataFrame is empty."]

    errors: list[str] = []
    errors.extend(_check_rede(df, dataset))
    errors.extend(_check_id_municipio(df, dataset))
    errors.extend(_check_join_coverage(df, dataset))
    errors.extend(_check_target(df, dataset))
    return errors


def assert_gold_contract(dataset: str, df: pd.DataFrame) -> None:
    """Raise ``GoldContractError`` when the dataset violates the contract."""
    errors = validate_gold_contract(dataset, df)
    if errors:
        for message in errors:
            logger.error("Gold contract violation: %s", message)
        raise GoldContractError("; ".join(errors))
    logger.info("Gold contract OK for '%s' (%d rows).", dataset, len(df))
