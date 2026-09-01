"""Tests for GoldWriter memory-safe partition writes."""

from __future__ import annotations

from unittest.mock import patch

import pandas as pd

from ingestion.gold.config import GoldDatasetConfig
from ingestion.gold.gold_writer import GoldWriter, _prepare_for_delta


def test_prepare_for_delta_inplace_avoids_extra_copy():
    df = pd.DataFrame({"ano": [2023], "id_municipio": ["123"], "valor": [1.5]})
    original_id = id(df)
    result = _prepare_for_delta(df, copy=False)
    assert id(result) == original_id
    assert str(result["id_municipio"].dtype) == "string"


@patch("ingestion.gold.gold_writer.write_deltalake")
def test_write_by_partitions_uses_overwrite_then_append(mock_write):
    writer = GoldWriter(storage_options={"AWS_REGION": "us-east-1"})
    config = GoldDatasetConfig(
        name="alunos_features",
        gold_path="s3://bucket/gold/alunos_features",
        partition_by="ano",
    )
    frames = [
        pd.DataFrame({"ano": [2023], "id_aluno": ["a"], "alfabetizado": [1]}),
        pd.DataFrame({"ano": [2024], "id_aluno": ["b"], "alfabetizado": [0]}),
    ]

    writer.write_by_partitions(frames, config, batch_id="batch-1", overwrite=True)

    assert mock_write.call_count == 2
    assert mock_write.call_args_list[0].kwargs["mode"] == "overwrite"
    assert mock_write.call_args_list[1].kwargs["mode"] == "append"
