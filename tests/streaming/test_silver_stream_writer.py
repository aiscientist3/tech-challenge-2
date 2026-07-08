"""Unit tests for Silver streaming upsert helpers."""

from __future__ import annotations

import pandas as pd

from ingestion.streaming.silver_stream_writer import prepare_alunos_silver_batch


class TestPrepareAlunosSilverBatch:
    def test_standardizes_deduplicates_and_adds_silver_metadata(self) -> None:
        pdf = pd.DataFrame(
            {
                "ano": [2024, 2024],
                "id_aluno": ["A1", "A1"],
                "id_municipio": ["3550308", "3550308"],
                "rede": [" Municipal ", " Municipal "],
                "proficiencia": [700.0, 800.0],
                "_ingestion_timestamp": [
                    "2026-06-16T11:00:00+00:00",
                    "2026-06-16T12:00:00+00:00",
                ],
            }
        )

        result = prepare_alunos_silver_batch(
            pdf,
            stream_batch_id="batch-123",
            ingestion_ts="2026-06-16T12:00:00+00:00",
        )

        assert len(result) == 1
        assert result.iloc[0]["proficiencia"] == 800.0
        assert result.iloc[0]["id_municipio"] == "3550308"
        assert result.iloc[0]["rede"] == "municipal"
        assert result.iloc[0]["_silver_ingestion_mode"] == "stream"
        assert result.iloc[0]["_silver_stream_sink"] == "bronze"
        assert result.iloc[0]["_silver_stream_batch_id"] == "batch-123"
        assert result.iloc[0]["_silver_processed_at"] == "2026-06-16T12:00:00+00:00"

    def test_empty_dataframe_returns_empty(self) -> None:
        pdf = pd.DataFrame()
        result = prepare_alunos_silver_batch(
            pdf,
            stream_batch_id="batch-123",
            ingestion_ts="2026-06-16T12:00:00+00:00",
        )
        assert result.empty
