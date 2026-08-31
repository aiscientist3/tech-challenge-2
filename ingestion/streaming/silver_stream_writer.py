"""
Silver layer writer for streaming alunos — reads from Bronze, never from Kafka.

Medallion rule: Kafka → Bronze (MERGE) → Silver (MERGE).

After each Bronze micro-batch is persisted, Silver transforms that Bronze slice,
projects business columns (FinOps), validates quality, quarantines invalid rows,
and upserts only valid rows into Silver by (ano, id_aluno) — no duplicate keys.

Kafka / event lineage columns stay on Bronze only.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from ingestion.silver.quality import validate_entity
from ingestion.silver.quarantine_writer import QuarantineWriter
from ingestion.silver.transforms import (
    deduplicate,
    project_alunos_for_silver,
    standardize_common,
)
from ingestion.streaming.bronze_stream_writer import merge_upsert_to_delta_table
from ingestion.streaming.config import (
    ALUNOS_NATURAL_KEYS,
    ALUNOS_SILVER_PARTITION_BY,
)

if TYPE_CHECKING:
    import pandas as pd

logger = logging.getLogger(__name__)


def load_bronze_microbatch_for_silver(bronze_written_pdf: pd.DataFrame) -> pd.DataFrame:
    """Return the Bronze slice used as Silver input (medallion: Silver ← Bronze)."""
    return bronze_written_pdf.copy()


def prepare_alunos_silver_batch(
    pdf: pd.DataFrame,
    *,
    stream_batch_id: str,
    ingestion_ts: str,
) -> pd.DataFrame:
    """Standardise, dedupe, project columns and attach Silver streaming metadata."""
    if pdf.empty:
        return pdf

    treated = standardize_common(pdf)
    treated = deduplicate(treated, ALUNOS_NATURAL_KEYS, entity_name="alunos")
    treated = treated.copy()
    treated["_silver_processed_at"] = ingestion_ts
    treated["_silver_stream_batch_id"] = stream_batch_id
    treated = project_alunos_for_silver(treated)
    return treated


def merge_upsert_to_silver(
    pdf: pd.DataFrame,
    *,
    silver_path: str,
    storage_options: dict[str, str],
    merge_keys: tuple[str, ...] = ALUNOS_NATURAL_KEYS,
    partition_by: str | None = ALUNOS_SILVER_PARTITION_BY,
) -> int:
    """Upsert rows into Silver Delta (no duplicate keys)."""
    return merge_upsert_to_delta_table(
        pdf,
        table_path=silver_path,
        storage_options=storage_options,
        merge_keys=merge_keys,
        preserve_columns=(),
        partition_by=partition_by,
    )


def process_bronze_to_silver_microbatch(
    bronze_written_pdf: pd.DataFrame,
    *,
    silver_path: str,
    storage_options: dict[str, str],
    stream_batch_id: str | None = None,
    ingestion_ts: str | None = None,
    partition_by: str | None = ALUNOS_SILVER_PARTITION_BY,
    references: dict[str, pd.DataFrame] | None = None,
    bucket: str | None = None,
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
    quality = validate_entity(silver_pdf, "alunos", references or {})

    if not quality.quarantine_df.empty and bucket:
        QuarantineWriter(storage_options, bucket).write(
            quality.quarantine_df,
            entity_name="alunos",
            batch_id=resolved_batch_id,
            partition_by=partition_by,
            layer="silver",
        )

    if quality.valid_df.empty:
        logger.warning(
            "Silver micro-batch: all %d rows quarantined — skipping MERGE.",
            quality.quarantine_count,
        )
        return 0

    merged_rows = merge_upsert_to_silver(
        quality.valid_df,
        silver_path=silver_path,
        storage_options=storage_options,
        partition_by=partition_by,
    )
    logger.info(
        "Silver micro-batch upserted from Bronze slice: %d records → %s "
        "(%d quarantined)",
        merged_rows,
        silver_path,
        quality.quarantine_count,
    )
    return merged_rows
