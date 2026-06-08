"""
Bronze layer writer — persists raw DataFrames to S3 in Delta Lake format.

Uses the `deltalake` Python library (delta-rs) which writes directly to S3
via boto3/object_store, bypassing Spark's S3A filesystem entirely.
This approach is compatible with Databricks Serverless (Spark Connect).

Responsibilities:
- Validate required columns before writing
- Attach ingestion metadata (_ingestion_timestamp, _source_table, _batch_id)
- Partition by year when applicable
- Write idempotently via overwrite mode
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
from deltalake import write_deltalake

from ingestion.batch.config import SourceConfig

logger = logging.getLogger(__name__)

METADATA_COLUMNS = (
    "_ingestion_timestamp",
    "_source_table",
    "_batch_id",
)


class BronzeWriter:
    """
    Writes raw pandas DataFrames to the Bronze layer in Delta Lake format on S3.

    Uses deltalake (delta-rs) for writing — no Spark S3A dependency.
    """

    def __init__(self, storage_options: dict[str, str]) -> None:
        """
        Args:
            storage_options: AWS credential dict for deltalake
                             (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION).
        """
        self.storage_options = storage_options

    def validate(self, df: pd.DataFrame, source_config: SourceConfig) -> None:
        """
        Assert that all required columns are present.

        Skips validation for empty DataFrames (logged as a warning).

        Raises:
            ValueError: When required columns are missing.
        """
        if df.empty:
            logger.warning(
                "Empty DataFrame for source '%s'. Write will be skipped.",
                source_config.name,
            )
            return

        missing = [c for c in source_config.required_columns if c not in df.columns]
        if missing:
            raise ValueError(
                f"Source '{source_config.name}': missing required columns {missing}."
            )

    def write(
        self,
        df: pd.DataFrame,
        source_config: SourceConfig,
        batch_id: str,
        overwrite: bool = True,
    ) -> Optional[str]:
        """
        Write a pandas DataFrame to the Bronze layer.

        Args:
            df:            Raw data extracted from BigQuery.
            source_config: Metadata for the source (S3 path, partition col, etc.).
            batch_id:      Unique identifier for this ingestion run.
            overwrite:     Use overwrite mode when True, append otherwise.

        Returns:
            S3 destination path, or None if the DataFrame was empty.
        """
        self.validate(df, source_config)
        if df.empty:
            return None

        destination = source_config.bronze_path
        ingestion_ts = datetime.now(timezone.utc).isoformat()

        df = df.copy()
        df["_ingestion_timestamp"] = ingestion_ts
        df["_source_table"] = source_config.bq_table
        df["_batch_id"] = batch_id

        write_mode = "overwrite" if overwrite else "append"

        partition_col = source_config.partition_by
        partition_by = (
            [partition_col]
            if partition_col and partition_col in df.columns
            else None
        )

        if partition_by:
            logger.info(
                "Writing '%s' → %s  (partitionBy=%s, mode=%s)",
                source_config.name,
                destination,
                partition_by,
                write_mode,
            )
        else:
            logger.info(
                "Writing '%s' → %s  (no partition, mode=%s)",
                source_config.name,
                destination,
                write_mode,
            )

        write_deltalake(
            table_or_uri=destination,
            data=df,
            mode=write_mode,
            partition_by=partition_by,
            storage_options=self.storage_options,
            schema_mode="overwrite",
        )

        logger.info(
            "Bronze write complete: '%s' — %d records at %s.",
            source_config.name,
            len(df),
            destination,
        )
        return destination
