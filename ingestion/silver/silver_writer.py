"""
Silver layer writer — persists treated DataFrames to S3 in Delta Lake format.

Uses deltalake (delta-rs), matching the Bronze writer approach.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
from deltalake import write_deltalake

from ingestion.silver.config import EntityConfig

logger = logging.getLogger(__name__)


class SilverWriter:
    """Writes treated pandas DataFrames to the Silver layer in Delta Lake format on S3."""

    def __init__(self, storage_options: dict[str, str]) -> None:
        self.storage_options = storage_options

    def write(
        self,
        df: pd.DataFrame,
        entity_config: EntityConfig,
        batch_id: str,
        overwrite: bool = True,
    ) -> Optional[str]:
        """
        Write a pandas DataFrame to the Silver layer.

        Returns:
            S3 destination path, or None if the DataFrame was empty.
        """
        if df.empty:
            logger.warning(
                "Empty DataFrame for entity '%s'. Write will be skipped.",
                entity_config.name,
            )
            return None

        destination = entity_config.silver_path
        write_mode = "overwrite" if overwrite else "append"

        output = df.copy()
        output["_silver_processed_at"] = datetime.now(timezone.utc).isoformat()
        output["_silver_batch_id"] = batch_id

        partition_col = entity_config.partition_by
        partition_by = (
            [partition_col]
            if partition_col and partition_col in output.columns
            else None
        )

        if partition_by:
            logger.info(
                "Writing '%s' → %s  (partitionBy=%s, mode=%s)",
                entity_config.name,
                destination,
                partition_by,
                write_mode,
            )
        else:
            logger.info(
                "Writing '%s' → %s  (no partition, mode=%s)",
                entity_config.name,
                destination,
                write_mode,
            )

        write_deltalake(
            table_or_uri=destination,
            data=output,
            mode=write_mode,
            partition_by=partition_by,
            storage_options=self.storage_options,
            schema_mode="overwrite",
        )

        logger.info(
            "Silver write complete: '%s' — %d records at %s.",
            entity_config.name,
            len(output),
            destination,
        )
        return destination
