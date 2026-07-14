"""Unit tests for Bronze streaming DLQ helpers."""

from __future__ import annotations

import json

from ingestion.streaming.bronze_dlq import expand_kafka_microbatch
from ingestion.streaming.event_schema import build_event_envelope


class _KafkaRow:
    def __init__(self, value: bytes | None, topic: str = "t", partition: int = 0, offset: int = 1):
        self.value = value
        self.topic = topic
        self.partition = partition
        self.offset = offset


class TestExpandKafkaMicrobatch:
    def test_valid_event_goes_to_bronze_records(self) -> None:
        payload = {"ano": 2024, "id_aluno": "A1", "id_municipio": "3550308"}
        envelope = build_event_envelope(payload)
        row = _KafkaRow(json.dumps(envelope).encode("utf-8"))

        result = expand_kafka_microbatch([row], source_table="test.alunos")

        assert len(result.records) == 1
        assert result.records[0]["id_aluno"] == "A1"
        assert result.records[0]["_event_id"] == envelope["event_id"]
        assert result.records[0]["_kafka_partition"] == 0
        assert result.records[0]["_kafka_offset"] == 1
        assert "_ingestion_timestamp" in result.records[0]
        assert "_ingestion_mode" not in result.records[0]
        assert "_kafka_topic" not in result.records[0]
        assert result.dlq_records == []

    def test_legacy_top_level_ano_still_accepted(self) -> None:
        """Older producers may still send ano on the envelope; prefer payload."""
        envelope = {
            "event_id": "e1",
            "event_type": "performance_measurement",
            "event_timestamp": "2026-01-01T00:00:00+00:00",
            "ano": 2024,
            "payload": json.dumps({"id_aluno": "A1", "id_municipio": "3550308"}),
        }
        row = _KafkaRow(json.dumps(envelope).encode("utf-8"))

        result = expand_kafka_microbatch([row], source_table="test.alunos")

        assert len(result.records) == 1
        assert result.records[0]["ano"] == 2024

    def test_invalid_payload_json_goes_to_dlq(self) -> None:
        envelope = {
            "event_id": "e1",
            "event_type": "performance_measurement",
            "event_timestamp": "2026-01-01T00:00:00+00:00",
            "payload": "{not-json",
        }
        row = _KafkaRow(json.dumps(envelope).encode("utf-8"))

        result = expand_kafka_microbatch([row], source_table="test.alunos")

        assert result.records == []
        assert len(result.dlq_records) == 1
        assert result.dlq_records[0]["_dlq_rule_id"] == "bronze_invalid_payload_json"

    def test_missing_natural_keys_goes_to_dlq(self) -> None:
        payload = {"ano": 2024, "id_municipio": "3550308"}
        envelope = build_event_envelope(payload)
        row = _KafkaRow(json.dumps(envelope).encode("utf-8"))

        result = expand_kafka_microbatch([row], source_table="test.alunos")

        assert result.records == []
        assert result.dlq_records[0]["_dlq_rule_id"] == "bronze_missing_natural_keys"

    def test_invalid_envelope_json_goes_to_dlq(self) -> None:
        row = _KafkaRow(b"{broken")

        result = expand_kafka_microbatch([row], source_table="test.alunos")

        assert result.records == []
        assert result.dlq_records[0]["_dlq_rule_id"] == "bronze_invalid_envelope_json"
