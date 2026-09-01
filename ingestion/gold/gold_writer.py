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
        "id_aluno",
        "rede",
        "serie",
        "sigla_uf",
        "nome_municipio",
        "nome_uf",
        "nome_regiao",
        "regiao_uf",
        "regiao_municipio",
        "nome_mesorregiao",
        "nome_microrregiao",
        "_gold_processed_at",
        "_gold_batch_id",
        "_quality_rule_ids",
        "_quality_messages",
    }
)


def _is_string_column(name: str) -> bool:
    if name in _STRING_COLUMNS or name.startswith(("_gold_", "_quality_")):
        return True
    if name.startswith(("nome_", "id_")):
        return True
    if "regiao" in name:
        return True
    return False


def _prepare_for_delta(df: pd.DataFrame, *, copy: bool = True) -> pd.DataFrame:
    """Coerce column dtypes so delta-rs can infer a valid Arrow schema."""
    output = df.copy() if copy else df

    for column in output.columns:
        if column == "ano":
            output[column] = pd.to_numeric(output[column], errors="coerce").astype("Int64")
        elif _is_string_column(column):
            output[column] = output[column].astype("string")
        else:
            original = output[column]
            numeric = pd.to_numeric(original, errors="coerce")
            if original.dtype == object or pd.api.types.is_string_dtype(original):
                non_null = int(original.notna().sum())
                numeric_ok = int(numeric.notna().sum())
                if non_null > 0 and numeric_ok < non_null:
                    output[column] = original.astype("string")
                    continue
            output[column] = numeric

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
        _prepare_for_delta(output, copy=False)

        partition_col = dataset_config.partition_by
        partition_by = (
            [partition_col]
            if partition_col and partition_col in output.columns
            else None
        )

        logger.info(
            "Writing '%s' → %s  (partitionBy=%s, mode=%s, rows=%d)",
            dataset_config.name,
            destination,
            partition_by,
            write_mode,
            len(output),
        )

        write_kwargs: dict = {
            "table_or_uri": destination,
            "data": output,
            "mode": write_mode,
            "partition_by": partition_by,
            "storage_options": self.storage_options,
        }
        if write_mode == "overwrite":
            write_kwargs["schema_mode"] = "overwrite"

        write_deltalake(**write_kwargs)

        logger.info(
            "Gold write complete: '%s' — %d records at %s.",
            dataset_config.name,
            len(output),
            destination,
        )
        return destination

    def write_by_partitions(
        self,
        frames: list[pd.DataFrame],
        dataset_config: GoldDatasetConfig,
        batch_id: str,
        *,
        overwrite: bool = True,
    ) -> Optional[str]:
        """Write multiple partition slices sequentially to limit peak memory."""
        non_empty = [frame for frame in frames if frame is not None and not frame.empty]
        if not non_empty:
            logger.warning(
                "No non-empty partitions for dataset '%s'. Write will be skipped.",
                dataset_config.name,
            )
            return None

        destination: Optional[str] = None
        for index, frame in enumerate(non_empty):
            mode_overwrite = overwrite and index == 0
            destination = self.write(
                frame,
                dataset_config,
                batch_id,
                overwrite=mode_overwrite,
            )
        return destination
