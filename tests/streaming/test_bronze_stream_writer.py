"""Unit tests for Bronze streaming upsert helpers."""

from __future__ import annotations

import pandas as pd
import pytest

from ingestion.streaming.bronze_stream_writer import (
    build_bronze_merge_predicate,
    build_matched_update_set,
    dedupe_merge_batch,
    record_has_merge_keys,
)


class TestBuildBronzeMergePredicate:
    def test_single_key(self):
        assert (
            build_bronze_merge_predicate(("id_aluno",))
            == "target.`id_aluno` = source.`id_aluno`"
        )

    def test_composite_key(self):
        assert build_bronze_merge_predicate(("ano", "id_aluno")) == (
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
        assert updates["proficiencia"] == "source.`proficiencia`"


class TestRecordHasMergeKeys:
    def test_valid_record(self):
        assert record_has_merge_keys({"ano": 2024, "id_aluno": "A1"}, ("ano", "id_aluno"))

    def test_missing_key(self):
        assert not record_has_merge_keys({"id_aluno": "A1"}, ("ano", "id_aluno"))


class TestDedupeMergeBatch:
    def test_keeps_last_row_per_key(self):
        pdf = pd.DataFrame(
            [
                {"ano": 2024, "id_aluno": "A1", "proficiencia": 100},
                {"ano": 2024, "id_aluno": "A1", "proficiencia": 200},
                {"ano": 2024, "id_aluno": "A2", "proficiencia": 150},
            ]
        )

        result = dedupe_merge_batch(pdf, ("ano", "id_aluno"))

        assert len(result) == 2
        assert result.loc[result["id_aluno"] == "A1", "proficiencia"].iloc[0] == 200

    def test_raises_when_key_missing(self):
        pdf = pd.DataFrame([{"id_aluno": "A1"}])

        with pytest.raises(ValueError, match="missing key columns"):
            dedupe_merge_batch(pdf, ("ano", "id_aluno"))
