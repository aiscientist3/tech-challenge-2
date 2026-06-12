"""
Bronze layer writer for Kafka streaming ingestion (Spark Structured Streaming).
"""

from __future__ import annotations

import json
import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json, lit
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


def expand_payload_to_bronze(envelope_df: DataFrame, spark: SparkSession) -> DataFrame:
    """Expand JSON payload strings into columns and attach streaming metadata."""
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
        records.append(record)

    if not records:
        return envelope_df.limit(0)

    return (
        spark.createDataFrame(records)
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_ingestion_mode", lit("stream"))
        .withColumn("_stream_sink", lit("kafka"))
        .withColumn("_source_table", lit(ALUNOS_BQ_TABLE))
    )


def write_stream_to_bronze(
    spark: SparkSession,
    kafka_df: DataFrame,
    *,
    bronze_path: str,
    checkpoint_path: str,
    partition_by: str = "ano",
) -> StreamingQuery:
    """Run a micro-batch streaming job (Trigger.AvailableNow) to Bronze Delta."""

    def _process_batch(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():
            logger.info("Batch %d is empty — skipping.", batch_id)
            return

        envelope_df = parse_kafka_envelope(batch_df)
        if envelope_df.isEmpty():
            logger.info("Batch %d has no valid events — skipping.", batch_id)
            return

        bronze_df = expand_payload_to_bronze(envelope_df, spark)
        if bronze_df.isEmpty():
            logger.info("Batch %d produced no Bronze rows — skipping.", batch_id)
            return

        writer = (
            bronze_df.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
        )
        if partition_by and partition_by in bronze_df.columns:
            writer = writer.partitionBy(partition_by)

        writer.save(bronze_path)
        logger.info(
            "Batch %d written to Bronze: %d records → %s",
            batch_id,
            bronze_df.count(),
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
    starting_offsets: str = "earliest",
) -> None:
    """End-to-end helper: Kafka → parse → Bronze Delta (single micro-batch)."""
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
    )
    query.awaitTermination()
    logger.info("Streaming ingestion to '%s' completed.", DEFAULT_STREAM_SOURCE)
