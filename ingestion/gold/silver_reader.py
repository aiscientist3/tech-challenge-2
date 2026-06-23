"""
Silver layer reader — loads treated Delta tables from S3 into pandas DataFrames.
"""

from __future__ import annotations

import logging

import pandas as pd
from deltalake import DeltaTable

logger = logging.getLogger(__name__)


def read_silver(
    path: str,
    storage_options: dict[str, str],
    years: list[int] | None = None,
    partition_col: str | None = "ano",
) -> pd.DataFrame:
    """
    Read a Silver Delta table from S3.

    Returns an empty DataFrame when the table is missing or has no rows.
    """
    logger.info("Reading Silver table: %s", path)

    try:
        table = DeltaTable(path, storage_options=storage_options)
        df = table.to_pandas()
    except Exception as exc:
        logger.warning("Could not read Silver table at %s: %s", path, exc)
        return pd.DataFrame()

    if df.empty:
        logger.warning("Silver table is empty: %s", path)
        return df

    if years and partition_col and partition_col in df.columns:
        before = len(df)
        df = df[df[partition_col].isin(years)].copy()
        logger.info(
            "Filtered by %s in %s — %d → %d records.",
            years,
            partition_col,
            before,
            len(df),
        )

    logger.info("Silver read complete: %s — %d records.", path, len(df))
    return df
