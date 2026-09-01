"""Tests for shared Delta → pandas reader."""

from __future__ import annotations

import pandas as pd

from ingestion.delta_read import (
    _hive_partition_value,
    _inject_partition_column,
)


def test_hive_partition_value_extracts_ano() -> None:
    key = "silver/br_inep/populacao_municipio/ano=2019/part-00000.parquet"
    assert _hive_partition_value(key, "ano") == "2019"
    assert _hive_partition_value(key, "rede") is None


def test_inject_partition_column_adds_missing_ano() -> None:
    df = pd.DataFrame({"id_municipio": ["3550308"], "populacao": [12_000_000]})
    result = _inject_partition_column(
        df,
        partition_col="ano",
        partition_value="2019",
    )
    assert result["ano"].tolist() == [2019]
    assert pd.api.types.is_integer_dtype(result["ano"].dtype) or str(
        result["ano"].dtype
    ).startswith("Int")
