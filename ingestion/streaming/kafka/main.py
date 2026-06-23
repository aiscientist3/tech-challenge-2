"""
Kafka streaming consumer entry point — Bronze + Silver (alunos).

Medallion flow:
  Kafka → Bronze Delta (MERGE) → Silver Delta (MERGE)

Only this job consumes Kafka. Silver is derived from each Bronze micro-batch.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

from pyspark.sql import SparkSession

from ingestion.batch.config import BRONZE_PREFIX
from ingestion.streaming.batch_runtime import (
    get_or_create_spark_session,
    resolve_aws_storage_options,
    resolve_kafka_config,
    resolve_s3_bucket,
)
from ingestion.streaming.bronze_stream_writer import run_kafka_to_bronze
from ingestion.streaming.config import (
    ALUNOS_BQ_TABLE,
    ALUNOS_BRONZE_PARTITION_BY,
    DEFAULT_STREAM_SOURCE,
    alunos_silver_path,
    bronze_table_path,
    checkpoint_path_for_runtime,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)


def _normalize_argv() -> None:
    if len(sys.argv) == 2 and sys.argv[1].startswith("--"):
        import shlex

        sys.argv = [sys.argv[0]] + shlex.split(sys.argv[1])


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Streaming ingestion — Kafka → Bronze Delta → Silver Delta (alunos, availableNow)."
        )
    )
    parser.add_argument(
        "--starting-offsets",
        default=os.getenv("KAFKA_STARTING_OFFSETS", "earliest"),
        choices=["earliest", "latest"],
    )
    parser.add_argument(
        "--stream-source",
        default=os.getenv("STREAM_SOURCE", DEFAULT_STREAM_SOURCE),
    )
    return parser


def _get_spark_session() -> SparkSession:
    """Reuse Databricks Serverless session (Kafka read only — no S3 via Spark)."""
    active = SparkSession.getActiveSession()
    if active is not None:
        logger.info("Reusing active Databricks SparkSession.")
        return active
    return get_or_create_spark_session(app_name="tech-challenge-streaming-bronze")


def main() -> None:
    _normalize_argv()
    args = _build_arg_parser().parse_args()

    bootstrap_servers, topic = resolve_kafka_config()
    bucket = resolve_s3_bucket()
    storage_options = resolve_aws_storage_options()

    bronze_path = bronze_table_path(bucket, args.stream_source, BRONZE_PREFIX)
    silver_path = alunos_silver_path(bucket)
    ckpt_path = checkpoint_path_for_runtime(bucket, args.stream_source)

    logger.info("=== STREAMING INGESTION STARTED (BRONZE → SILVER) ===")
    logger.info("Kafka bootstrap : %s", bootstrap_servers)
    logger.info("Kafka topic     : %s", topic)
    logger.info("Bronze path     : %s", bronze_path)
    logger.info("Silver path     : %s", silver_path)
    logger.info("Checkpoint path : %s", ckpt_path)

    spark = _get_spark_session()

    run_kafka_to_bronze(
        spark,
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        bronze_path=bronze_path,
        checkpoint_path=ckpt_path,
        storage_options=storage_options,
        silver_path=silver_path,
        starting_offsets=args.starting_offsets,
        source_table=ALUNOS_BQ_TABLE,
        partition_by=ALUNOS_BRONZE_PARTITION_BY,
    )

    logger.info("=== STREAMING INGESTION COMPLETED (BRONZE → SILVER) ===")


if __name__ == "__main__":
    main()
