"""
Event envelope schema for streaming ingestion (Kafka → Bronze).
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from ingestion.streaming.config import EVENT_TYPE_PERFORMANCE

KAFKA_EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("event_timestamp", StringType(), False),
        StructField("ano", IntegerType(), True),
        StructField("payload", StringType(), True),
    ]
)


def build_event_envelope(
    row: dict[str, Any],
    *,
    event_type: str = EVENT_TYPE_PERFORMANCE,
    ano: int | None = None,
) -> dict[str, Any]:
    """Wrap a raw data row in a streaming event envelope."""
    resolved_ano = ano if ano is not None else row.get("ano")
    if resolved_ano is not None:
        resolved_ano = int(resolved_ano)

    return {
        "event_id": str(uuid4()),
        "event_type": event_type,
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "ano": resolved_ano,
        "payload": json.dumps(row, default=str, ensure_ascii=False),
    }


def serialize_event(event: dict[str, Any]) -> bytes:
    """Serialize an event envelope to UTF-8 JSON bytes for Kafka."""
    return json.dumps(event, ensure_ascii=False, default=str).encode("utf-8")
