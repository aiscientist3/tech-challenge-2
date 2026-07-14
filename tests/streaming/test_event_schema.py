"""Unit tests for streaming event envelope schema."""

from __future__ import annotations

import json

from ingestion.streaming.event_schema import (
    build_event_envelope,
    serialize_event,
)


class TestEventSchema:
    def test_build_event_envelope_has_required_fields(self):
        event = build_event_envelope({"ano": 2024, "sigla_uf": "SP"})
        assert "event_id" in event
        assert event["event_type"] == "performance_measurement"
        assert "event_timestamp" in event
        assert "ano" not in event  # business fields only inside payload
        assert json.loads(event["payload"])["ano"] == 2024
        assert json.loads(event["payload"])["sigla_uf"] == "SP"

    def test_build_event_envelope_fills_ano_into_payload_when_passed(self):
        event = build_event_envelope({"id_municipio": "3550308"}, ano=2023)
        assert "ano" not in event
        assert json.loads(event["payload"])["ano"] == 2023

    def test_serialize_event_returns_valid_json_bytes(self):
        event = build_event_envelope({"ano": 2024})
        raw = serialize_event(event)
        parsed = json.loads(raw.decode("utf-8"))
        assert parsed["event_id"] == event["event_id"]
        assert parsed["event_type"] == "performance_measurement"
        assert "ano" not in parsed
