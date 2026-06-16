"""Shared fixtures for Silver layer tests."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture
def sample_uf() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "sigla": ["SP", "RJ"],
            "nome": ["São Paulo", "Rio de Janeiro"],
            "regiao": ["Sudeste", "Sudeste"],
            "id_uf": ["35", "33"],
            "_ingestion_timestamp": ["2026-01-01T00:00:00+00:00"] * 2,
        }
    )


@pytest.fixture
def sample_municipio() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "id_municipio": ["3550308", "3304557"],
            "nome": ["São Paulo", "Rio de Janeiro"],
            "sigla_uf": ["SP", "RJ"],
            "nome_uf": ["São Paulo", "Rio de Janeiro"],
            "nome_regiao": ["Sudeste", "Sudeste"],
            "capital_uf": [1, 1],
            "_ingestion_timestamp": ["2026-01-01T00:00:00+00:00"] * 2,
        }
    )
