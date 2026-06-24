"""
Shared Delta → pandas reader with Databricks Spark fallback.

deltalake + pyarrow can fail on hive-partitioned tables in Databricks Serverless
(``field() takes at least 2 positional arguments``). Spark Delta reads are native
on Databricks and bypass that code path.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd
from deltalake import DeltaTable

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


def _read_via_spark(
    path: str,
    *,
    years: list[int] | None,
    partition_col: str | None,
) -> pd.DataFrame | None:
    """Read a Delta table with Spark when a session is available (Databricks)."""
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import col
    except ImportError:
        return None

    spark = SparkSession.getActiveSession()
    if spark is None:
        return None

    try:
        from ingestion.batch.connections.spark_s3 import (
            _configure_s3_credentials,
            _resolve_aws_credentials,
        )

        _configure_s3_credentials(spark, _resolve_aws_credentials())
    except Exception as exc:
        logger.warning("Could not configure Spark S3 credentials: %s", exc)

    logger.info("Reading Delta via Spark: %s", path)
    load_paths = [path]
    if path.startswith("s3://"):
        load_paths.append(path.replace("s3://", "s3a://", 1))

    last_exc: Exception | None = None
    for load_path in load_paths:
        try:
            spark_df = spark.read.format("delta").load(load_path)
            if years and partition_col:
                spark_df = spark_df.filter(col(partition_col).isin(years))
            pdf = spark_df.toPandas()
            logger.info(
                "Spark Delta read complete: %s — %d records.",
                load_path,
                len(pdf),
            )
            return pdf
        except Exception as exc:
            last_exc = exc
            logger.warning("Spark Delta read failed for %s: %s", load_path, exc)

    logger.warning(
        "Spark Delta read unavailable; falling back to deltalake. Last error: %s",
        last_exc,
    )
    return None


def _read_single_partition_deltalake(
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


def _read_via_deltalake(
    table: DeltaTable,
    *,
    years: list[int] | None,
    partition_col: str | None,
) -> pd.DataFrame:
    """Read via deltalake (local dev and non-Databricks runtimes)."""
    if years and partition_col:
        try:
            return table.to_pandas(filters=[(partition_col, "in", years)])
        except Exception as filtered_exc:
            logger.warning(
                "deltalake filtered read failed (%s); retrying partition-by-partition.",
                filtered_exc,
            )
            frames: list[pd.DataFrame] = []
            for year in years:
                part = _read_single_partition_deltalake(table, partition_col, year)
                if not part.empty:
                    frames.append(part)
            if frames:
                return pd.concat(frames, ignore_index=True)
            raise filtered_exc

    return table.to_pandas()


def read_delta_to_pandas(
    path: str,
    storage_options: dict[str, str],
    *,
    years: list[int] | None = None,
    partition_col: str | None = "ano",
) -> pd.DataFrame:
    """
    Read a Delta table from S3 into pandas.

    Raises:
        RuntimeError: When the table cannot be read by any backend.
    """
    errors: list[str] = []

    spark_df = _read_via_spark(path, years=years, partition_col=partition_col)
    if spark_df is not None:
        return spark_df

    try:
        table = DeltaTable(path, storage_options=storage_options)
        return _read_via_deltalake(table, years=years, partition_col=partition_col)
    except Exception as exc:
        errors.append(f"deltalake: {exc}")

    raise RuntimeError(
        f"Could not read Delta table at {path}. Attempts: {'; '.join(errors)}"
    )
