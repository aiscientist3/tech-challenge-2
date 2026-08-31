"""
Event envelope schema for streaming ingestion (Kafka → Bronze).

Contract (no duplicated business fields at the top level):
  {
    "event_id": "...",
    "event_type": "performance_measurement",
    "event_timestamp": "...",
    "payload": "{ ... business row JSON, including ano ... }"
  }
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from pyspark.sql.types import StringType, StructField, StructType

from ingestion.streaming.config import EVENT_TYPE_PERFORMANCE

KAFKA_EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("event_timestamp", StringType(), False),
        StructField("payload", StringType(), True),
    ]
)


def build_event_envelope(
    row: dict[str, Any],
    *,
    event_type: str = EVENT_TYPE_PERFORMANCE,
    ano: int | None = None,
) -> dict[str, Any]:
    """Wrap a raw data row in a streaming event envelope.

    ``ano`` may be passed to fill ``row['ano']`` when missing; it is never
    duplicated as a top-level envelope field (lives only inside ``payload``).
    """
    payload_row = dict(row)
    if payload_row.get("ano") is None and ano is not None:
        payload_row["ano"] = int(ano)

    return {
        "event_id": str(uuid4()),
        "event_type": event_type,
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "payload": json.dumps(payload_row, default=str, ensure_ascii=False),
    }


def serialize_event(event: dict[str, Any]) -> bytes:
    """Serialize an event envelope to UTF-8 JSON bytes for Kafka."""
    return json.dumps(event, ensure_ascii=False, default=str).encode("utf-8")
