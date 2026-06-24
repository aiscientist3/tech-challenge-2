"""
Silver layer reader — loads treated Delta tables from S3 into pandas DataFrames.
"""

from __future__ import annotations

import logging

import pandas as pd
from deltalake import DeltaTable

logger = logging.getLogger(__name__)


def _delta_to_pandas(
    table: DeltaTable,
    *,
    years: list[int] | None,
    partition_col: str | None,
) -> pd.DataFrame:
    """
    Read a Delta table into pandas with partition-aware fallbacks.

    Databricks Serverless can fail on full scans of hive-partitioned tables when
    pyarrow/deltalake versions are mismatched (``field() takes at least 2 ...``).
    """
    if years and partition_col:
        try:
            return table.to_pandas(filters=[(partition_col, "in", years)])
        except Exception as filtered_exc:
            logger.warning(
                "Filtered Delta read failed (%s); retrying partition-by-partition.",
                filtered_exc,
            )
            frames: list[pd.DataFrame] = []
            for year in years:
                part = _read_single_partition(table, partition_col, year)
                if not part.empty:
                    frames.append(part)
            if frames:
                return pd.concat(frames, ignore_index=True)
            raise filtered_exc

    return table.to_pandas()


def _read_single_partition(
    table: DeltaTable,
    partition_col: str,
    year: int,
) -> pd.DataFrame:
    """Read one hive partition using the most compatible deltalake API available."""
    for kwargs in (
        {"partitions": [(partition_col, str(year))]},
        {"filters": [(partition_col, "=", year)]},
        {"filters": [(partition_col, "in", [year])]},
    ):
        try:
            return table.to_pandas(**kwargs)
        except Exception:
            continue

    try:
        arrow = table.to_pyarrow_table(filters=[(partition_col, "=", year)])
        return arrow.to_pandas(timestamp_as_object=True)
    except Exception:
        return pd.DataFrame()


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
        table = DeltaTable(path, storage_options=storage_options)
        df = _delta_to_pandas(table, years=years, partition_col=partition_col)
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
