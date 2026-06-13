"""
Kafka streaming consumer entry point — Bronze layer.

Reads events from Kafka via Spark Structured Streaming and appends them to
the Bronze Delta table using Trigger.AvailableNow() (Databricks Serverless).

CLI usage:
  python -m ingestion.streaming.kafka.main
  python -m ingestion.streaming.kafka.main --starting-offsets latest
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

from ingestion.streaming.bronze_stream_writer import run_kafka_to_bronze
from ingestion.streaming.config import (
    BRONZE_PREFIX,
    CHECKPOINT_PREFIX,
    DEFAULT_STREAM_SOURCE,
    bronze_table_path,
    checkpoint_path,
)
from ingestion.streaming.connections.aws_credentials import (
    resolve_kafka_config,
    resolve_s3_bucket,
)
from ingestion.streaming.connections.spark_s3 import (
    get_or_create_spark_session,
    s3_path_for_spark,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)


def _normalize_argv() -> None:
    """Split Databricks job parameters passed as a single argv string."""
    if len(sys.argv) == 2 and sys.argv[1].startswith("--"):
        import shlex

        extra = shlex.split(sys.argv[1])
        sys.argv = [sys.argv[0]] + extra


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Streaming ingestion — Kafka → S3 Bronze Delta (availableNow)."
    )
    parser.add_argument(
        "--starting-offsets",
        default=os.getenv("KAFKA_STARTING_OFFSETS", "earliest"),
        choices=["earliest", "latest"],
        help="Kafka starting offset strategy.",
    )
    parser.add_argument(
        "--stream-source",
        default=os.getenv("STREAM_SOURCE", DEFAULT_STREAM_SOURCE),
        help="Bronze sub-path and checkpoint name for the stream.",
    )
    return parser


def main() -> None:
    _normalize_argv()
    args = _build_arg_parser().parse_args()

    bootstrap_servers, topic = resolve_kafka_config()
    bucket = resolve_s3_bucket()
    bronze_path = s3_path_for_spark(
        bronze_table_path(bucket, args.stream_source, BRONZE_PREFIX)
    )
    ckpt_path = s3_path_for_spark(
        checkpoint_path(bucket, args.stream_source, CHECKPOINT_PREFIX)
    )

    logger.info("=== STREAMING INGESTION STARTED (BRONZE) ===")
    logger.info("Kafka bootstrap : %s", bootstrap_servers)
    logger.info("Kafka topic     : %s", topic)
    logger.info("Bronze path     : %s", bronze_path)
    logger.info("Checkpoint path : %s", ckpt_path)
    logger.info("Starting offsets: %s", args.starting_offsets)

    spark = get_or_create_spark_session(app_name="tech-challenge-streaming-bronze")

    run_kafka_to_bronze(
        spark,
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        bronze_path=bronze_path,
        checkpoint_path=ckpt_path,
        starting_offsets=args.starting_offsets,
    )

    logger.info("=== STREAMING INGESTION COMPLETED ===")


if __name__ == "__main__":
    main()
