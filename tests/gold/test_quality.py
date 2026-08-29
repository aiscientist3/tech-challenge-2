"""Unit tests for Gold contract quality checks."""

from __future__ import annotations

import pandas as pd
import pytest

from ingestion.gold.quality import (
    GoldContractError,
    assert_gold_contract,
    validate_gold_contract,
)


def test_validate_accepts_canonical_rede_and_id() -> None:
    df = pd.DataFrame(
        {
            "id_municipio": ["3550308", "3304557"],
            "rede": ["municipal", "estadual"],
            "nome_municipio": ["São Paulo", "Rio de Janeiro"],
            "alfabetizado": [1.0, 0.0],
            "_join_match": [True, True],
        }
    )
    assert validate_gold_contract("alunos_features", df) == []


def test_validate_rejects_numeric_rede() -> None:
    df = pd.DataFrame(
        {
            "id_municipio": ["3550308"],
            "rede": ["3"],
            "alfabetizado": [1.0],
            "_join_match": [True],
        }
    )
    errors = validate_gold_contract("alunos_features", df)
    assert any("rede" in message for message in errors)


def test_validate_rejects_short_id_municipio() -> None:
    df = pd.DataFrame(
        {
            "id_municipio": ["123"],
            "rede": ["municipal"],
            "alfabetizado": [1.0],
            "_join_match": [True],
        }
    )
    errors = validate_gold_contract("alunos_features", df)
    assert any("id_municipio" in message for message in errors)


def test_assert_raises_on_low_join_coverage() -> None:
    df = pd.DataFrame(
        {
            "id_municipio": ["3550308"] * 10,
            "rede": ["municipal"] * 10,
            "alfabetizado": [1.0] * 10,
            "_join_match": [True] + [False] * 9,
        }
    )
    with pytest.raises(GoldContractError):
        assert_gold_contract("alunos_features", df)
