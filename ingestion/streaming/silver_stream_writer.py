"""
Silver layer writer for streaming alunos — reads from Bronze, never from Kafka.

Medallion rule: Kafka → Bronze (only) → Silver.

After each Bronze micro-batch is persisted, Silver transforms that Bronze slice
and upserts into the Silver Delta table by (ano, id_aluno).
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from ingestion.silver.transforms import standardize_common
from ingestion.streaming.config import (
    ALUNOS_SILVER_MERGE_KEYS,
    ALUNOS_SILVER_MERGE_PRESERVE_COLUMNS,
    ALUNOS_SILVER_PARTITION_BY,
)

if TYPE_CHECKING:
    import pandas as pd

logger = logging.getLogger(__name__)


def load_bronze_microbatch_for_silver(bronze_written_pdf: pd.DataFrame) -> pd.DataFrame:
    """
    Return the Bronze slice used as Silver input (medallion: Silver ← Bronze).

    The micro-batch must already be merged into Bronze before calling this.
    The returned DataFrame is the authoritative Bronze content for these keys.
    """
    return bronze_written_pdf.copy()


def prepare_alunos_silver_batch(
    pdf: pd.DataFrame,
    *,
    stream_batch_id: str,
    ingestion_ts: str,
) -> pd.DataFrame:
    """Apply Silver standardisation and attach streaming metadata."""
    if pdf.empty:
        return pdf

    treated = standardize_common(pdf)
    treated = treated.copy()
    treated["_silver_processed_at"] = ingestion_ts
    treated["_silver_ingestion_mode"] = "stream"
    treated["_silver_stream_sink"] = "bronze"
    treated["_silver_stream_batch_id"] = stream_batch_id
    return treated


def merge_upsert_to_silver(
    pdf: pd.DataFrame,
    *,
    silver_path: str,
    storage_options: dict[str, str],
    merge_keys: tuple[str, ...] = ALUNOS_SILVER_MERGE_KEYS,
    preserve_columns: tuple[str, ...] = ALUNOS_SILVER_MERGE_PRESERVE_COLUMNS,
    partition_by: str | None = ALUNOS_SILVER_PARTITION_BY,
) -> int:
    """Upsert rows into Silver Delta using the shared Delta merge helper."""
    from ingestion.streaming.bronze_stream_writer import merge_upsert_to_bronze

    return merge_upsert_to_bronze(
        pdf,
        bronze_path=silver_path,
        storage_options=storage_options,
        merge_keys=merge_keys,
        preserve_columns=preserve_columns,
        partition_by=partition_by,
    )


def process_bronze_to_silver_microbatch(
    bronze_written_pdf: pd.DataFrame,
    *,
    silver_path: str,
    storage_options: dict[str, str],
    stream_batch_id: str | None = None,
    ingestion_ts: str | None = None,
    merge_keys: tuple[str, ...] = ALUNOS_SILVER_MERGE_KEYS,
    partition_by: str | None = ALUNOS_SILVER_PARTITION_BY,
) -> int:
    """
    Transform a Bronze micro-batch and upsert into Silver.

    Must be called only after the same rows were successfully merged into Bronze.
    """
    resolved_ts = ingestion_ts or datetime.now(timezone.utc).isoformat()
    resolved_batch_id = stream_batch_id or str(uuid.uuid4())

    bronze_slice = load_bronze_microbatch_for_silver(bronze_written_pdf)
    if bronze_slice.empty:
        return 0

    silver_pdf = prepare_alunos_silver_batch(
        bronze_slice,
        stream_batch_id=resolved_batch_id,
        ingestion_ts=resolved_ts,
    )
    merged_rows = merge_upsert_to_silver(
        silver_pdf,
        silver_path=silver_path,
        storage_options=storage_options,
        merge_keys=merge_keys,
        partition_by=partition_by,
    )
    logger.info(
        "Silver micro-batch upserted from Bronze slice: %d records → %s",
        merged_rows,
        silver_path,
    )
    return merged_rows
