"""
Bronze layer writer for Kafka streaming ingestion (Spark Structured Streaming).

Medallion flow for alunos:
  Kafka → Bronze Delta (MERGE, 1 row/aluno) → Silver Delta (MERGE, 1 row/aluno)

Delta MERGE upserts by (ano, id_aluno) — no duplicate keys. New Parquet files may
still be written by Delta internally; the table stays deduplicated at row level.

Checkpoint must live on a Unity Catalog Volume on Databricks Serverless (DBFS root
and Spark S3A checkpoints are blocked). See streaming/config.py.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from deltalake import DeltaTable, write_deltalake
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.streaming import StreamingQuery

from ingestion.streaming.bronze_dlq import (
    expand_kafka_microbatch,
    record_has_natural_keys,
)
from ingestion.streaming.config import (
    ALUNOS_BQ_TABLE,
    ALUNOS_BRONZE_PARTITION_BY,
    ALUNOS_BRONZE_PRESERVE_COLUMNS,
    ALUNOS_NATURAL_KEYS,
    DEFAULT_STREAM_SOURCE,
)
from ingestion.streaming.event_schema import KAFKA_EVENT_SCHEMA

if TYPE_CHECKING:
    import pandas as pd

logger = logging.getLogger(__name__)


def build_merge_predicate(
    merge_keys: tuple[str, ...],
    *,
    target_alias: str = "target",
    source_alias: str = "source",
) -> str:
    """Build a Delta MERGE predicate for the given natural key columns."""
    return " AND ".join(
        f"{target_alias}.`{key}` = {source_alias}.`{key}`" for key in merge_keys
    )


def build_matched_update_set(
    columns: list[str],
    *,
    merge_keys: tuple[str, ...],
    preserve_columns: tuple[str, ...] = (),
    source_alias: str = "source",
) -> dict[str, str]:
    """Columns to refresh from source on MERGE match (keys and preserved excluded)."""
    skip = set(merge_keys) | set(preserve_columns)
    return {
        column: f"{source_alias}.`{column}`"
        for column in columns
        if column not in skip
    }


def dedupe_microbatch(
    pdf: pd.DataFrame,
    natural_keys: tuple[str, ...],
) -> pd.DataFrame:
    """Keep one row per natural key within a micro-batch (last event wins)."""
    missing = [key for key in natural_keys if key not in pdf.columns]
    if missing:
        raise ValueError(f"Cannot write Delta: missing key columns {missing}.")
    if pdf.empty:
        return pdf
    return pdf.drop_duplicates(subset=list(natural_keys), keep="last").reset_index(drop=True)


def merge_upsert_to_delta_table(
    pdf: pd.DataFrame,
    *,
    table_path: str,
    storage_options: dict[str, str],
    merge_keys: tuple[str, ...] = ALUNOS_NATURAL_KEYS,
    preserve_columns: tuple[str, ...] = (),
    partition_by: str | None = ALUNOS_BRONZE_PARTITION_BY,
) -> int:
    """
    Upsert rows into a Delta table: update matched keys, insert new ones.

    Ensures at most one row per merge key in the table (no duplicates).
    """
    pdf = dedupe_microbatch(pdf, merge_keys)
    if pdf.empty:
        return 0

    partition_cols = (
        [partition_by]
        if partition_by and partition_by in pdf.columns
        else None
    )

    if not DeltaTable.is_deltatable(table_path, storage_options=storage_options):
        write_deltalake(
            table_or_uri=table_path,
            data=pdf,
            mode="append",
            partition_by=partition_cols,
            storage_options=storage_options,
            schema_mode="merge",
        )
        return len(pdf)

    delta_table = DeltaTable(table_path, storage_options=storage_options)
    update_set = build_matched_update_set(
        list(pdf.columns),
        merge_keys=merge_keys,
        preserve_columns=preserve_columns,
    )
    merger = delta_table.merge(
        source=pdf,
        predicate=build_merge_predicate(merge_keys),
        source_alias="source",
        target_alias="target",
        merge_schema=True,
    )
    if update_set:
        merger = merger.when_matched_update(update_set)
    (
        merger.when_not_matched_insert_all()
        .execute()
    )
    return len(pdf)


def merge_upsert_to_bronze(
    pdf: pd.DataFrame,
    *,
    bronze_path: str,
    storage_options: dict[str, str],
    merge_keys: tuple[str, ...] = ALUNOS_NATURAL_KEYS,
    preserve_columns: tuple[str, ...] = ALUNOS_BRONZE_PRESERVE_COLUMNS,
    partition_by: str | None = ALUNOS_BRONZE_PARTITION_BY,
) -> int:
    """Upsert alunos into Bronze Delta (preserves legacy _batch_id on update)."""
    return merge_upsert_to_delta_table(
        pdf,
        table_path=bronze_path,
        storage_options=storage_options,
        merge_keys=merge_keys,
        preserve_columns=preserve_columns,
        partition_by=partition_by,
    )


def read_kafka_stream(
    spark: SparkSession,
    *,
    bootstrap_servers: str,
    topic: str,
    starting_offsets: str = "earliest",
) -> DataFrame:
    """Build a Structured Streaming DataFrame from a Kafka topic."""
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_kafka_envelope(kafka_df: DataFrame) -> DataFrame:
    """Parse Kafka value bytes into event envelope fields plus Kafka metadata."""
    return (
        kafka_df.select(
            from_json(col("value").cast("string"), KAFKA_EVENT_SCHEMA).alias("event"),
            col("topic"),
            col("partition"),
            col("offset"),
        )
        .select(
            col("event.event_id"),
            col("event.event_type"),
            col("event.event_timestamp"),
            col("event.ano"),
            col("event.payload"),
            col("topic"),
            col("partition"),
            col("offset"),
        )
        .filter(col("event_id").isNotNull() & col("payload").isNotNull())
    )


def expand_payload_to_bronze_records(
    envelope_df: DataFrame,
    *,
    source_table: str = ALUNOS_BQ_TABLE,
    natural_keys: tuple[str, ...] = ALUNOS_NATURAL_KEYS,
    ingestion_ts: str | None = None,
) -> list[dict]:
    """Expand JSON payloads into row dicts with streaming metadata."""
    resolved_ts = ingestion_ts or datetime.now(timezone.utc).isoformat()
    records: list[dict] = []
    for row in envelope_df.collect():
        if not row.payload:
            continue
        try:
            record = json.loads(row.payload)
        except json.JSONDecodeError as exc:
            logger.warning(
                "Skipping event %s: invalid payload JSON: %s",
                row.event_id,
                exc,
            )
            continue
        if record.get("ano") is None and row.ano is not None:
            record["ano"] = int(row.ano)
        if not record_has_natural_keys(record, natural_keys):
            logger.warning(
                "Skipping event %s: missing natural keys %s.",
                row.event_id,
                natural_keys,
            )
            continue
        record["_event_id"] = row.event_id
        record["_event_type"] = row.event_type
        record["_event_timestamp"] = row.event_timestamp
        record["_kafka_topic"] = row.topic
        record["_kafka_partition"] = row.partition
        record["_kafka_offset"] = row.offset
        record["_ingestion_timestamp"] = resolved_ts
        record["_ingestion_mode"] = "stream"
        record["_stream_sink"] = "kafka"
        record["_source_table"] = source_table
        records.append(record)
    return records


def write_stream_to_bronze(
    spark: SparkSession,
    kafka_df: DataFrame,
    *,
    bronze_path: str,
    checkpoint_path: str,
    storage_options: dict[str, str],
    silver_path: str | None = None,
    source_table: str = ALUNOS_BQ_TABLE,
    partition_by: str = ALUNOS_BRONZE_PARTITION_BY,
    natural_keys: tuple[str, ...] = ALUNOS_NATURAL_KEYS,
    silver_references: dict | None = None,
    bucket: str | None = None,
) -> StreamingQuery:
    """Run Structured Streaming (Trigger.AvailableNow) to Bronze Delta (+ Silver)."""

    def _process_batch(batch_df: DataFrame, micro_batch_id: int) -> None:
        if batch_df.isEmpty():
            logger.info("Micro-batch %d is empty — skipping.", micro_batch_id)
            return

        ingestion_ts = datetime.now(timezone.utc).isoformat()
        stream_batch_id = f"stream-{micro_batch_id}-{ingestion_ts}"

        kafka_rows = batch_df.select("value", "topic", "partition", "offset").collect()
        expansion = expand_kafka_microbatch(
            kafka_rows,
            source_table=source_table,
            natural_keys=natural_keys,
            ingestion_ts=ingestion_ts,
        )

        if expansion.dlq_records:
            logger.warning(
                "Micro-batch %d: %d malformed Kafka events skipped (lean mode — log only).",
                micro_batch_id,
                len(expansion.dlq_records),
            )

        if not expansion.records:
            logger.info(
                "Micro-batch %d produced no Bronze rows (%d DLQ).",
                micro_batch_id,
                len(expansion.dlq_records),
            )
            return

        import pandas as pd

        pdf = pd.DataFrame(expansion.records)
        merged_rows = merge_upsert_to_bronze(
            pdf,
            bronze_path=bronze_path,
            storage_options=storage_options,
            merge_keys=natural_keys,
            partition_by=partition_by,
        )
        logger.info(
            "Micro-batch %d upserted to Bronze: %d records → %s (keys=%s)",
            micro_batch_id,
            merged_rows,
            bronze_path,
            natural_keys,
        )

        if silver_path and merged_rows > 0:
            from ingestion.streaming.silver_stream_writer import (
                process_bronze_to_silver_microbatch,
            )

            process_bronze_to_silver_microbatch(
                pdf,
                silver_path=silver_path,
                storage_options=storage_options,
                stream_batch_id=stream_batch_id,
                ingestion_ts=ingestion_ts,
                references=silver_references,
                bucket=bucket,
            )
            logger.info(
                "Micro-batch %d propagated Bronze → Silver: %s",
                micro_batch_id,
                silver_path,
            )

    query = (
        kafka_df.writeStream.foreachBatch(_process_batch)
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .start()
    )

    logger.info(
        "Streaming query started (availableNow). Bronze: %s | Checkpoint: %s",
        bronze_path,
        checkpoint_path,
    )
    return query


def _load_streaming_silver_references(
    bucket: str,
    storage_options: dict[str, str],
) -> dict:
    """Load Silver reference tables needed for streaming alunos quality checks."""
    from ingestion.gold.silver_reader import read_silver
    from ingestion.silver.config import SILVER_PREFIX

    municipio_path = f"s3://{bucket}/{SILVER_PREFIX}/municipio"
    try:
        municipio = read_silver(
            municipio_path,
            storage_options,
            years=None,
            partition_col=None,
        )
        logger.info(
            "Loaded municipio reference for streaming quality: %d rows.",
            len(municipio),
        )
        return {"municipio": municipio}
    except Exception as exc:
        logger.warning(
            "Could not load municipio Silver reference for quality checks: %s",
            exc,
        )
        return {}


def run_kafka_to_bronze(
    spark: SparkSession,
    *,
    bootstrap_servers: str,
    topic: str,
    bronze_path: str,
    checkpoint_path: str,
    storage_options: dict[str, str],
    silver_path: str | None = None,
    starting_offsets: str = "earliest",
    source_table: str = ALUNOS_BQ_TABLE,
    partition_by: str = ALUNOS_BRONZE_PARTITION_BY,
    natural_keys: tuple[str, ...] = ALUNOS_NATURAL_KEYS,
    bucket: str | None = None,
) -> None:
    """End-to-end: Kafka → Bronze Delta upsert (+ optional Silver from Bronze)."""
    silver_references: dict | None = None
    if silver_path and bucket:
        silver_references = _load_streaming_silver_references(bucket, storage_options)

    kafka_df = read_kafka_stream(
        spark,
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        starting_offsets=starting_offsets,
    )
    query = write_stream_to_bronze(
        spark,
        kafka_df,
        bronze_path=bronze_path,
        checkpoint_path=checkpoint_path,
        storage_options=storage_options,
        silver_path=silver_path,
        source_table=source_table,
        partition_by=partition_by,
        natural_keys=natural_keys,
        silver_references=silver_references,
        bucket=bucket,
    )
    query.awaitTermination()
    logger.info(
        "Streaming ingestion to '%s' completed (Silver enabled=%s).",
        DEFAULT_STREAM_SOURCE,
        silver_path is not None,
    )
