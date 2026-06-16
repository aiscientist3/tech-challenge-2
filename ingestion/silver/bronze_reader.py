"""
Bronze layer reader — loads Delta tables from S3 into pandas DataFrames.

Uses deltalake (delta-rs), compatible with Databricks Serverless and local dev.
"""

from __future__ import annotations

import logging

import pandas as pd
from deltalake import DeltaTable

logger = logging.getLogger(__name__)


def read_bronze(
    path: str,
    storage_options: dict[str, str],
    years: list[int] | None = None,
    partition_col: str | None = "ano",
) -> pd.DataFrame:
    """
    Read a Bronze Delta table from S3.

    Args:
        path:             S3 URI of the Bronze table (e.g. s3://bucket/bronze/.../uf).
        storage_options:  AWS credentials dict for deltalake.
        years:            Optional list of years to filter when partition_col is set.
        partition_col:    Partition column name (default ``ano``). Pass None to skip filter.

    Returns:
        pandas DataFrame with Bronze records. Empty DataFrame when the table has no data.
    """
    logger.info("Reading Bronze table: %s", path)

    try:
        table = DeltaTable(path, storage_options=storage_options)
        df = table.to_pandas()
    except Exception as exc:
        logger.warning("Could not read Bronze table at %s: %s", path, exc)
        return pd.DataFrame()

    if df.empty:
        logger.warning("Bronze table is empty: %s", path)
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

    logger.info("Bronze read complete: %s — %d records.", path, len(df))
    return df
