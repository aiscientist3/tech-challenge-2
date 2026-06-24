"""
Silver layer reader — loads treated Delta tables from S3 into pandas DataFrames.
"""

from __future__ import annotations

import logging

import pandas as pd

from ingestion.delta_read import read_delta_to_pandas

logger = logging.getLogger(__name__)


def read_silver(
    path: str,
    storage_options: dict[str, str],
    years: list[int] | None = None,
    partition_col: str | None = "ano",
) -> pd.DataFrame:
    """
    Read a Silver Delta table from S3.

    Raises:
        RuntimeError: When the table exists but cannot be read.
    """
    logger.info("Reading Silver table: %s", path)

    try:
        df = read_delta_to_pandas(
            path,
            storage_options,
            years=years,
            partition_col=partition_col,
        )
    except Exception as exc:
        raise RuntimeError(f"Could not read Silver table at {path}: {exc}") from exc

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
