"""
Quarantine writer — persists rejected Silver/Gold rows to Delta on S3 (append).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
from deltalake import write_deltalake

from ingestion.silver.config import EntityConfig, quarantine_table_path

logger = logging.getLogger(__name__)


class QuarantineWriter:
    """Writes quarantined pandas DataFrames to the quarantine Delta prefix on S3."""

    def __init__(self, storage_options: dict[str, str], bucket: str) -> None:
        self.storage_options = storage_options
        self.bucket = bucket

    def write(
        self,
        df: pd.DataFrame,
        *,
        entity_name: str,
        batch_id: str,
        partition_by: str | None = "ano",
        layer: str = "silver",
    ) -> Optional[str]:
        """
        Append quarantined rows to the entity quarantine table.

        Returns:
            S3 destination path, or None if the DataFrame was empty.
        """
        if df.empty:
            return None

        destination = quarantine_table_path(self.bucket, entity_name, layer=layer)
        output = df.copy()
        metadata_col = f"_{layer}_batch_id"
        output[metadata_col] = batch_id
        output["_quarantine_layer"] = layer

        partition_cols = (
            [partition_by]
            if partition_by and partition_by in output.columns
            else None
        )

        logger.info(
            "Writing quarantine '%s' → %s  (partitionBy=%s, mode=append)",
            entity_name,
            destination,
            partition_cols,
        )

        write_deltalake(
            table_or_uri=destination,
            data=output,
            mode="append",
            partition_by=partition_cols,
            storage_options=self.storage_options,
            schema_mode="merge",
        )

        logger.info(
            "Quarantine write complete: '%s' — %d records at %s.",
            entity_name,
            len(output),
            destination,
        )
        return destination

    def write_entity(
        self,
        df: pd.DataFrame,
        entity_config: EntityConfig,
        batch_id: str,
    ) -> Optional[str]:
        """Append quarantined rows for a Silver entity using its partition config."""
        return self.write(
            df,
            entity_name=entity_config.name,
            batch_id=batch_id,
            partition_by=entity_config.partition_by,
            layer="silver",
        )
