"""
Bronze layer writer for Kafka streaming ingestion (Spark Structured Streaming).

Reads from Kafka via Spark readStream; writes Bronze Delta via deltalake in
foreachBatch — Serverless-compatible for S3 writes (same as batch bronze_writer).

Checkpoint must live on a Unity Catalog Volume on Databricks Serverless (DBFS root
and Spark S3A checkpoints are blocked). See streaming/config.py.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from deltalake import write_deltalake
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.streaming import StreamingQuery

from ingestion.streaming.config import ALUNOS_BQ_TABLE, DEFAULT_STREAM_SOURCE
from ingestion.streaming.event_schema import KAFKA_EVENT_SCHEMA

logger = logging.getLogger(__name__)


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
        .filter(col("event_id").isNotNull())
    )


def expand_payload_to_bronze_records(envelope_df: DataFrame) -> list[dict]:
    """Expand JSON payloads into row dicts with streaming metadata."""
    ingestion_ts = datetime.now(timezone.utc).isoformat()
    records: list[dict] = []
    for row in envelope_df.collect():
        if not row.payload:
            continue
        record = json.loads(row.payload)
        record["_event_id"] = row.event_id
        record["_event_type"] = row.event_type
        record["_event_timestamp"] = row.event_timestamp
        record["_kafka_topic"] = row.topic
        record["_kafka_partition"] = row.partition
        record["_kafka_offset"] = row.offset
        record["_ingestion_timestamp"] = ingestion_ts
        record["_ingestion_mode"] = "stream"
        record["_stream_sink"] = "kafka"
        record["_source_table"] = ALUNOS_BQ_TABLE
        records.append(record)
    return records


def write_stream_to_bronze(
    spark: SparkSession,
    kafka_df: DataFrame,
    *,
    bronze_path: str,
    checkpoint_path: str,
    storage_options: dict[str, str],
    partition_by: str = "ano",
) -> StreamingQuery:
    """Run Structured Streaming (Trigger.AvailableNow) to Bronze Delta."""

    def _process_batch(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():
            logger.info("Batch %d is empty — skipping.", batch_id)
            return

        envelope_df = parse_kafka_envelope(batch_df)
        if envelope_df.isEmpty():
            logger.info("Batch %d has no valid events — skipping.", batch_id)
            return

        records = expand_payload_to_bronze_records(envelope_df)
        if not records:
            logger.info("Batch %d produced no Bronze rows — skipping.", batch_id)
            return

        import pandas as pd

        pdf = pd.DataFrame(records)
        partition_cols = (
            [partition_by]
            if partition_by and partition_by in pdf.columns
            else None
        )

        write_deltalake(
            table_or_uri=bronze_path,
            data=pdf,
            mode="append",
            partition_by=partition_cols,
            storage_options=storage_options,
            schema_mode="merge",
        )
        logger.info(
            "Batch %d written to Bronze: %d records → %s",
            batch_id,
            len(pdf),
            bronze_path,
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


def run_kafka_to_bronze(
    spark: SparkSession,
    *,
    bootstrap_servers: str,
    topic: str,
    bronze_path: str,
    checkpoint_path: str,
    storage_options: dict[str, str],
    starting_offsets: str = "earliest",
) -> None:
    """End-to-end: Kafka Structured Streaming → parse → Bronze Delta."""
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
    )
    query.awaitTermination()
    logger.info("Streaming ingestion to '%s' completed.", DEFAULT_STREAM_SOURCE)
