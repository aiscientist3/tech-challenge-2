"""
Kafka producer simulator — streams alunos performance events from BigQuery.

Reads a sample of student microdata from BigQuery and publishes each row as a
JSON event envelope to the Kafka topic, simulating near-real-time ingestion.

CLI usage:
  python -m ingestion.streaming.producer.alunos_simulator --years 2024 --row-limit 100
  python -m ingestion.streaming.producer.alunos_simulator --sleep-seconds 0.1
"""

from __future__ import annotations

import argparse
import logging
import os
import time
from typing import Any

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

from ingestion.batch.config import DEFAULT_KAFKA_TOPIC, DEFAULT_YEARS
from ingestion.batch.connections.aws_credentials import resolve_kafka_config
from ingestion.batch.connections.bigquery_client import create_bigquery_client
from ingestion.streaming.config import ALUNOS_BQ_TABLE, PRODUCER_MAX_RETRIES
from ingestion.streaming.event_schema import build_event_envelope, serialize_event

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)


def _build_query(years: list[int], row_limit: int | None) -> str:
    years_sql = ", ".join(str(y) for y in years)
    limit_sql = f"LIMIT {row_limit}" if row_limit else ""
    return (
        f"SELECT *\n"
        f"FROM `{ALUNOS_BQ_TABLE}`\n"
        f"WHERE ano IN ({years_sql})\n"
        f"{limit_sql}"
    ).strip()


def _row_to_dict(row: Any) -> dict[str, Any]:
    """Convert a BigQuery row or pandas Series to a plain dict."""
    if hasattr(row, "to_dict"):
        return row.to_dict()
    return dict(row)


def _create_producer(bootstrap_servers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers.split(","),
        acks="all",
        retries=0,
        linger_ms=10,
    )


def _send_event_with_retries(
    producer: KafkaProducer,
    topic: str,
    event: dict[str, Any],
    *,
    max_retries: int = PRODUCER_MAX_RETRIES,
) -> bool:
    """
    Publish one event with up to max_retries attempts.

    Returns:
        True when the event was acknowledged by Kafka, False after all retries fail.
    """
    payload = serialize_event(event)
    event_id = event.get("event_id", "unknown")

    for attempt in range(1, max_retries + 1):
        try:
            future = producer.send(topic, value=payload)
            future.get(timeout=30)
            logger.info("Event %s published (attempt %d).", event_id, attempt)
            return True
        except Exception as exc:
            logger.warning(
                "Attempt %d/%d failed for event %s: %s",
                attempt,
                max_retries,
                event_id,
                exc,
            )
            if attempt < max_retries:
                time.sleep(1)

    logger.error(
        "Event %s dropped after %d failed attempts — stopping producer.",
        event_id,
        max_retries,
    )
    return False


def publish_alunos_events(
    *,
    years: list[int],
    row_limit: int | None,
    sleep_seconds: float,
    bootstrap_servers: str | None = None,
    topic: str | None = None,
) -> int:
    """
    Extract alunos rows from BigQuery and publish them to Kafka.

    Returns:
        Number of events published.
    """
    if bootstrap_servers is None or topic is None:
        bootstrap_servers, topic = resolve_kafka_config()

    logger.info("Bootstrap servers: %s", bootstrap_servers)
    logger.info("Topic: %s", topic)
    logger.info("Years: %s | Row limit: %s", years, row_limit or "none")

    bq_client = create_bigquery_client()
    query = _build_query(years, row_limit)
    logger.info("BigQuery query:\n%s", query)

    df = bq_client.query(query).to_dataframe(create_bqstorage_client=False)
    if df.empty:
        logger.warning("No rows returned from BigQuery — nothing to publish.")
        return 0

    try:
        producer = _create_producer(bootstrap_servers)
    except NoBrokersAvailable as exc:
        raise RuntimeError(
            f"Cannot connect to Kafka at {bootstrap_servers}. "
            "Ensure the EC2 broker is running and port 9092 is reachable."
        ) from exc

    published = 0
    for _, row in df.iterrows():
        event = build_event_envelope(_row_to_dict(row))
        if not _send_event_with_retries(producer, topic, event):
            break

        published += 1

        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

    producer.flush()
    producer.close()
    logger.info("Finished. Published %d events to topic '%s'.", published, topic)
    return published


def _parse_years(raw: str) -> list[int]:
    years = [int(y.strip()) for y in raw.split(",") if y.strip()]
    if not years:
        raise ValueError("At least one valid year is required.")
    return years


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Simulate streaming alunos events: BigQuery → Kafka."
    )
    parser.add_argument(
        "--years",
        default=",".join(str(y) for y in DEFAULT_YEARS),
        help="Comma-separated years to sample from BigQuery.",
    )
    parser.add_argument(
        "--row-limit",
        type=int,
        default=None,
        help="Max events to publish (default: no limit).",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=float(os.getenv("PRODUCER_SLEEP_SECONDS", "0")),
        help="Delay between events to simulate real-time arrival.",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
        help="Kafka bootstrap servers (overrides Databricks secret / env).",
    )
    parser.add_argument(
        "--topic",
        default=os.getenv("KAFKA_TOPIC", DEFAULT_KAFKA_TOPIC),
        help="Kafka topic name.",
    )
    return parser


def main() -> None:
    parser = _build_arg_parser()
    args = parser.parse_args()

    bootstrap = args.bootstrap_servers
    topic = args.topic
    if not bootstrap:
        bootstrap, topic = resolve_kafka_config()

    publish_alunos_events(
        years=_parse_years(args.years),
        row_limit=args.row_limit,
        sleep_seconds=args.sleep_seconds,
        bootstrap_servers=bootstrap,
        topic=topic,
    )


if __name__ == "__main__":
    main()
