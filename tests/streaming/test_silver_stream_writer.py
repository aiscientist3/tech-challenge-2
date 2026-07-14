"""Unit tests for Silver streaming upsert helpers."""

from __future__ import annotations

import pandas as pd

from ingestion.streaming.silver_stream_writer import prepare_alunos_silver_batch


class TestPrepareAlunosSilverBatch:
    def test_standardizes_deduplicates_projects_and_adds_silver_metadata(self) -> None:
        pdf = pd.DataFrame(
            {
                "ano": [2024, 2024],
                "id_aluno": ["A1", "A1"],
                "id_municipio": ["3550308", "3550308"],
                "rede": [" Municipal ", " Municipal "],
                "alfabetizado": ["Sim", "Sim"],
                "proficiencia": [700.0, 800.0],
                "peso_aluno": [1.0, 1.0],
                "caderno": ["X", "X"],
                "id_escola": ["E1", "E1"],
                "_kafka_partition": [0, 0],
                "_kafka_offset": [1, 2],
                "_event_id": ["e1", "e2"],
                "_ingestion_timestamp": [
                    "2026-06-16T11:00:00+00:00",
                    "2026-06-16T12:00:00+00:00",
                ],
            }
        )

        result = prepare_alunos_silver_batch(
            pdf,
            stream_batch_id="stream-123",
            ingestion_ts="2026-06-16T12:00:00+00:00",
        )

        assert len(result) == 1
        assert result.iloc[0]["proficiencia"] == 800.0
        assert result.iloc[0]["id_municipio"] == "3550308"
        assert result.iloc[0]["rede"] == "municipal"
        assert result.iloc[0]["_silver_stream_batch_id"] == "stream-123"
        assert result.iloc[0]["_silver_processed_at"] == "2026-06-16T12:00:00+00:00"
        assert "_ingestion_timestamp" in result.columns
        assert "_silver_ingestion_mode" not in result.columns

        for col in (
            "_kafka_partition",
            "_kafka_offset",
            "_event_id",
            "caderno",
            "id_escola",
        ):
            assert col not in result.columns

    def test_empty_dataframe_returns_empty(self) -> None:
        pdf = pd.DataFrame()
        result = prepare_alunos_silver_batch(
            pdf,
            stream_batch_id="stream-123",
            ingestion_ts="2026-06-16T12:00:00+00:00",
        )
        assert result.empty
