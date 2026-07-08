"""Unit tests for Bronze streaming upsert helpers."""

from __future__ import annotations

import pandas as pd
import pytest

from ingestion.streaming.bronze_stream_writer import (
    build_matched_update_set,
    build_merge_predicate,
    dedupe_microbatch,
    record_has_natural_keys,
)


class TestBuildMergePredicate:
    def test_composite_key(self):
        assert build_merge_predicate(("ano", "id_aluno")) == (
            "target.`ano` = source.`ano` AND target.`id_aluno` = source.`id_aluno`"
        )


class TestBuildMatchedUpdateSet:
    def test_excludes_merge_keys_and_batch_id(self):
        updates = build_matched_update_set(
            ["ano", "id_aluno", "proficiencia", "_batch_id", "_event_id"],
            merge_keys=("ano", "id_aluno"),
            preserve_columns=("_batch_id",),
        )

        assert set(updates) == {"proficiencia", "_event_id"}


class TestRecordHasNaturalKeys:
    def test_valid_record(self):
        assert record_has_natural_keys(
            {"ano": 2024, "id_aluno": "A1"}, ("ano", "id_aluno")
        )

    def test_missing_key(self):
        assert not record_has_natural_keys({"id_aluno": "A1"}, ("ano", "id_aluno"))


class TestDedupeMicrobatch:
    def test_keeps_last_row_per_key(self):
        pdf = pd.DataFrame(
            [
                {"ano": 2024, "id_aluno": "A1", "proficiencia": 100},
                {"ano": 2024, "id_aluno": "A1", "proficiencia": 200},
                {"ano": 2024, "id_aluno": "A2", "proficiencia": 150},
            ]
        )

        result = dedupe_microbatch(pdf, ("ano", "id_aluno"))

        assert len(result) == 2
        assert result.loc[result["id_aluno"] == "A1", "proficiencia"].iloc[0] == 200

    def test_raises_when_key_missing(self):
        pdf = pd.DataFrame([{"id_aluno": "A1"}])

        with pytest.raises(ValueError, match="missing key columns"):
            dedupe_microbatch(pdf, ("ano", "id_aluno"))
