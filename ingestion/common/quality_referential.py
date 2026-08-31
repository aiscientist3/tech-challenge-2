"""Shared referential (FK) quality checks for Silver and Gold validators."""

from __future__ import annotations

import logging

import pandas as pd


def referencial_failure_mask(
    df: pd.DataFrame,
    *,
    entity_col: str,
    ref_table: str,
    ref_col: str,
    references: dict[str, pd.DataFrame],
    rule_id: str,
    logger: logging.Logger,
) -> pd.Series:
    """
    Return True for rows that fail a referential integrity check.

    When the reference table is unavailable, logs a warning and returns no
    failures (lean mode — avoids mass quarantine on misconfigured references).
    """
    if entity_col not in df.columns:
        return pd.Series(True, index=df.index)

    ref_df = references.get(ref_table)
    if ref_df is None or ref_col not in ref_df.columns:
        logger.warning(
            "Referential rule '%s': reference '%s' unavailable — skipping check (lean mode).",
            rule_id,
            ref_table,
        )
        return pd.Series(False, index=df.index)

    valid_keys = set(ref_df[ref_col].dropna().astype("string"))
    present = df[entity_col].notna()
    values = df.loc[present, entity_col].astype("string")
    invalid = ~values.isin(valid_keys)
    mask = pd.Series(False, index=df.index)
    mask.loc[present] = invalid.values
    return mask
