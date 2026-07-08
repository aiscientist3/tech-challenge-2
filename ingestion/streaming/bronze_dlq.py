"""
Bronze streaming Kafka parsing — separates valid records from malformed events.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from ingestion.streaming.config import ALUNOS_NATURAL_KEYS

logger = logging.getLogger(__name__)


@dataclass
class BronzeExpansionResult:
    """Valid Bronze records and rejected rows from one Kafka micro-batch."""

    records: list[dict[str, Any]] = field(default_factory=list)
    dlq_records: list[dict[str, Any]] = field(default_factory=list)


def _rejected_record(
    *,
    rule_id: str,
    reason: str,
    ingestion_ts: str,
    raw_payload: str | None = None,
    event_id: str | None = None,
    event_type: str | None = None,
    event_timestamp: str | None = None,
    ano: int | None = None,
    kafka_topic: str | None = None,
    kafka_partition: int | None = None,
    kafka_offset: int | None = None,
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "_dlq_rule_id": rule_id,
        "_dlq_reason": reason,
        "_raw_payload": raw_payload,
        "_event_id": event_id,
        "_event_type": event_type,
        "_event_timestamp": event_timestamp,
        "_kafka_topic": kafka_topic,
        "_kafka_partition": kafka_partition,
        "_kafka_offset": kafka_offset,
        "_ingestion_timestamp": ingestion_ts,
    }
    if ano is not None:
        row["ano"] = ano
    return row


def record_has_natural_keys(record: dict[str, Any], natural_keys: tuple[str, ...]) -> bool:
    """Return True when all natural key columns are present and non-null."""
    return all(record.get(key) is not None for key in natural_keys)


def expand_kafka_microbatch(
    kafka_rows: list[Any],
    *,
    source_table: str,
    natural_keys: tuple[str, ...] = ALUNOS_NATURAL_KEYS,
    ingestion_ts: str | None = None,
) -> BronzeExpansionResult:
    """
    Parse Kafka rows into valid Bronze records or rejected events.

    Malformed envelopes, invalid JSON payloads and missing natural keys are
    counted in ``dlq_records`` (log only — not persisted to S3).
    """
    resolved_ts = ingestion_ts or datetime.now(timezone.utc).isoformat()
    result = BronzeExpansionResult()

    for row in kafka_rows:
        topic = getattr(row, "topic", None)
        partition = getattr(row, "partition", None)
        offset = getattr(row, "offset", None)
        raw_value = getattr(row, "value", None)

        if raw_value is None:
            result.dlq_records.append(
                _rejected_record(
                    rule_id="bronze_kafka_empty_value",
                    reason="Kafka message value is null",
                    ingestion_ts=resolved_ts,
                    kafka_topic=topic,
                    kafka_partition=partition,
                    kafka_offset=offset,
                )
            )
            continue

        try:
            value_str = (
                raw_value.decode("utf-8")
                if isinstance(raw_value, (bytes, bytearray))
                else str(raw_value)
            )
        except Exception as exc:
            result.dlq_records.append(
                _rejected_record(
                    rule_id="bronze_kafka_decode_error",
                    reason=f"Cannot decode Kafka value: {exc}",
                    ingestion_ts=resolved_ts,
                    kafka_topic=topic,
                    kafka_partition=partition,
                    kafka_offset=offset,
                )
            )
            continue

        try:
            envelope = json.loads(value_str)
        except json.JSONDecodeError as exc:
            result.dlq_records.append(
                _rejected_record(
                    rule_id="bronze_invalid_envelope_json",
                    reason=f"Invalid envelope JSON: {exc}",
                    ingestion_ts=resolved_ts,
                    raw_payload=value_str,
                    kafka_topic=topic,
                    kafka_partition=partition,
                    kafka_offset=offset,
                )
            )
            continue

        if not isinstance(envelope, dict):
            result.dlq_records.append(
                _rejected_record(
                    rule_id="bronze_invalid_envelope_shape",
                    reason="Envelope JSON must be an object",
                    ingestion_ts=resolved_ts,
                    raw_payload=value_str,
                    kafka_topic=topic,
                    kafka_partition=partition,
                    kafka_offset=offset,
                )
            )
            continue

        event_id = envelope.get("event_id")
        event_type = envelope.get("event_type")
        event_timestamp = envelope.get("event_timestamp")
        envelope_ano = envelope.get("ano")
        payload_raw = envelope.get("payload")

        if not event_id or payload_raw is None:
            result.dlq_records.append(
                _rejected_record(
                    rule_id="bronze_missing_event_fields",
                    reason="Envelope missing event_id or payload",
                    ingestion_ts=resolved_ts,
                    raw_payload=value_str,
                    event_id=str(event_id) if event_id else None,
                    event_type=event_type,
                    event_timestamp=event_timestamp,
                    ano=int(envelope_ano) if envelope_ano is not None else None,
                    kafka_topic=topic,
                    kafka_partition=partition,
                    kafka_offset=offset,
                )
            )
            continue

        try:
            record = json.loads(payload_raw)
        except json.JSONDecodeError as exc:
            result.dlq_records.append(
                _rejected_record(
                    rule_id="bronze_invalid_payload_json",
                    reason=f"Invalid payload JSON: {exc}",
                    ingestion_ts=resolved_ts,
                    raw_payload=str(payload_raw),
                    event_id=str(event_id),
                    event_type=event_type,
                    event_timestamp=event_timestamp,
                    ano=int(envelope_ano) if envelope_ano is not None else None,
                    kafka_topic=topic,
                    kafka_partition=partition,
                    kafka_offset=offset,
                )
            )
            continue

        if not isinstance(record, dict):
            result.dlq_records.append(
                _rejected_record(
                    rule_id="bronze_invalid_payload_shape",
                    reason="Payload JSON must be an object",
                    ingestion_ts=resolved_ts,
                    raw_payload=str(payload_raw),
                    event_id=str(event_id),
                    event_type=event_type,
                    event_timestamp=event_timestamp,
                    ano=int(envelope_ano) if envelope_ano is not None else None,
                    kafka_topic=topic,
                    kafka_partition=partition,
                    kafka_offset=offset,
                )
            )
            continue

        if record.get("ano") is None and envelope_ano is not None:
            record["ano"] = int(envelope_ano)

        if not record_has_natural_keys(record, natural_keys):
            result.dlq_records.append(
                _rejected_record(
                    rule_id="bronze_missing_natural_keys",
                    reason=f"Missing natural keys {natural_keys}",
                    ingestion_ts=resolved_ts,
                    raw_payload=str(payload_raw),
                    event_id=str(event_id),
                    event_type=event_type,
                    event_timestamp=event_timestamp,
                    ano=record.get("ano"),
                    kafka_topic=topic,
                    kafka_partition=partition,
                    kafka_offset=offset,
                )
            )
            continue

        record["_event_id"] = event_id
        record["_event_type"] = event_type
        record["_event_timestamp"] = event_timestamp
        record["_kafka_topic"] = topic
        record["_kafka_partition"] = partition
        record["_kafka_offset"] = offset
        record["_ingestion_timestamp"] = resolved_ts
        record["_ingestion_mode"] = "stream"
        record["_stream_sink"] = "kafka"
        record["_source_table"] = source_table
        result.records.append(record)

    if result.dlq_records:
        logger.warning(
            "Kafka micro-batch: %d malformed events rejected.",
            len(result.dlq_records),
        )
    return result
