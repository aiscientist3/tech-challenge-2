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
        assert event["ano"] == 2024
        assert json.loads(event["payload"])["sigla_uf"] == "SP"

    def test_build_event_envelope_infers_ano_from_row(self):
        event = build_event_envelope({"ano": 2023, "id_municipio": "3550308"})
        assert event["ano"] == 2023

    def test_serialize_event_returns_valid_json_bytes(self):
        event = build_event_envelope({"ano": 2024})
        raw = serialize_event(event)
        parsed = json.loads(raw.decode("utf-8"))
        assert parsed["event_id"] == event["event_id"]
        assert parsed["event_type"] == "performance_measurement"
