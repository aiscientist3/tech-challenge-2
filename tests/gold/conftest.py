"""Shared fixtures for Gold layer tests."""

from __future__ import annotations

import pandas as pd
import pytest


@pytest.fixture
def sample_alunos() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "ano": [2024, 2024, 2024, 2024],
            "id_municipio": ["3550308", "3550308", "3304557", "3304557"],
            "id_aluno": ["A1", "A2", "A3", "A4"],
            "serie": ["2º ano", "2º ano", "2º ano", "2º ano"],
            "rede": ["municipal", "municipal", "estadual", "estadual"],
            "alfabetizado": ["Sim", "Não", "Sim", "Sim"],
            "peso_aluno": [1.0, 1.0, 2.0, 1.0],
            "proficiencia": [800.0, 600.0, 750.0, 720.0],
        }
    )


@pytest.fixture
def sample_meta_municipio() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "ano": [2024, 2024],
            "id_municipio": ["3550308", "3304557"],
            "rede": ["municipal", "estadual"],
            "nome_municipio": ["São Paulo", "Rio de Janeiro"],
            "sigla_uf": ["SP", "RJ"],
            "taxa_alfabetizacao": [55.0, 60.0],
            "meta_alfabetizacao_2024": [70.0, 75.0],
            "meta_alfabetizacao_2030": [100.0, 100.0],
        }
    )


@pytest.fixture
def sample_municipio() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "id_municipio": ["3550308", "3304557"],
            "sigla_uf": ["SP", "RJ"],
            "nome": ["São Paulo", "Rio de Janeiro"],
            "nome_uf": ["São Paulo", "Rio de Janeiro"],
            "nome_regiao": ["Sudeste", "Sudeste"],
            "nome_mesorregiao": ["São Paulo", "Metropolitana do Rio de Janeiro"],
            "nome_microrregiao": ["São Paulo", "Rio de Janeiro"],
            "capital_uf": [1, 1],
            "amazonia_legal": [0, 0],
        }
    )


@pytest.fixture
def sample_meta_uf() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "ano": [2024, 2024],
            "sigla_uf": ["SP", "RJ"],
            "rede": ["municipal", "estadual"],
            "nome_uf": ["São Paulo", "Rio de Janeiro"],
            "regiao_uf": ["Sudeste", "Sudeste"],
            "taxa_alfabetizacao": [58.0, 62.0],
            "meta_alfabetizacao_2024": [72.0, 78.0],
            "meta_alfabetizacao_2030": [100.0, 100.0],
        }
    )


@pytest.fixture
def sample_meta_brasil() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "ano": [2024, 2024],
            "rede": ["municipal", "estadual"],
            "taxa_alfabetizacao": [56.0, 61.0],
            "meta_alfabetizacao_2024": [68.0, 73.0],
            "meta_alfabetizacao_2030": [100.0, 100.0],
        }
    )
