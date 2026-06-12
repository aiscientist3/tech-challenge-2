"""Unit tests for the alunos Kafka producer retry logic."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from ingestion.streaming.producer.alunos_simulator import _send_event_with_retries


class TestSendEventWithRetries:
    def test_succeeds_on_first_attempt(self):
        producer = MagicMock()
        producer.send.return_value.get.return_value = None

        ok = _send_event_with_retries(
            producer, "test-topic", {"event_id": "evt-1"}, max_retries=3
        )

        assert ok is True
        assert producer.send.call_count == 1

    def test_succeeds_on_second_attempt(self):
        producer = MagicMock()
        producer.send.return_value.get.side_effect = [RuntimeError("timeout"), None]

        with patch("ingestion.streaming.producer.alunos_simulator.time.sleep"):
            ok = _send_event_with_retries(
                producer, "test-topic", {"event_id": "evt-2"}, max_retries=3
            )

        assert ok is True
        assert producer.send.call_count == 2

    def test_drops_and_returns_false_after_max_retries(self):
        producer = MagicMock()
        producer.send.return_value.get.side_effect = RuntimeError("kafka down")

        with patch("ingestion.streaming.producer.alunos_simulator.time.sleep"):
            ok = _send_event_with_retries(
                producer, "test-topic", {"event_id": "evt-3"}, max_retries=3
            )

        assert ok is False
        assert producer.send.call_count == 3
