"""
Gold layer writer — persists analytical DataFrames to S3 in Delta Lake format.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
from deltalake import write_deltalake

from ingestion.gold.config import GoldDatasetConfig

logger = logging.getLogger(__name__)

_STRING_COLUMNS = frozenset(
    {
        "id_municipio",
        "rede",
        "sigla_uf",
        "nome_municipio",
        "nome_uf",
        "_gold_processed_at",
        "_gold_batch_id",
    }
)


def _prepare_for_delta(df: pd.DataFrame) -> pd.DataFrame:
    """Coerce column dtypes so delta-rs can infer a valid Arrow schema."""
    output = df.copy()

    for column in output.columns:
        if column == "ano":
            output[column] = pd.to_numeric(output[column], errors="coerce").astype("Int64")
        elif column in _STRING_COLUMNS:
            output[column] = output[column].astype("string")
        else:
            output[column] = pd.to_numeric(output[column], errors="coerce")

    return output


class GoldWriter:
    """Writes analytical pandas DataFrames to the Gold layer in Delta Lake format on S3."""

    def __init__(self, storage_options: dict[str, str]) -> None:
        self.storage_options = storage_options

    def write(
        self,
        df: pd.DataFrame,
        dataset_config: GoldDatasetConfig,
        batch_id: str,
        overwrite: bool = True,
    ) -> Optional[str]:
        """Write a pandas DataFrame to the Gold layer."""
        if df.empty:
            logger.warning(
                "Empty DataFrame for dataset '%s'. Write will be skipped.",
                dataset_config.name,
            )
            return None

        destination = dataset_config.gold_path
        write_mode = "overwrite" if overwrite else "append"

        output = df.copy()
        output["_gold_processed_at"] = datetime.now(timezone.utc).isoformat()
        output["_gold_batch_id"] = batch_id
        output = _prepare_for_delta(output)

        partition_col = dataset_config.partition_by
        partition_by = (
            [partition_col]
            if partition_col and partition_col in output.columns
            else None
        )

        logger.info(
            "Writing '%s' → %s  (partitionBy=%s, mode=%s)",
            dataset_config.name,
            destination,
            partition_by,
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
            "Gold write complete: '%s' — %d records at %s.",
            dataset_config.name,
            len(output),
            destination,
        )
        return destination
