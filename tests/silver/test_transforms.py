"""Unit tests for Silver transformations."""

from __future__ import annotations

import pandas as pd

from ingestion.silver.transforms import (
    deduplicate,
    enrich_meta_municipio,
    enrich_meta_uf,
    standardize_common,
)


def test_standardize_id_municipio_zero_pads_to_seven_digits() -> None:
    df = pd.DataFrame({"id_municipio": ["123", "45", 3550308.0]})
    result = standardize_common(df)
    assert result["id_municipio"].tolist() == ["0000123", "0000045", "3550308"]


def test_standardize_sigla_and_rede() -> None:
    df = pd.DataFrame(
        {
            "sigla_uf": [" sp ", "Rj"],
            "rede": [" Municipal ", "ESTADUAL"],
        }
    )
    result = standardize_common(df)
    assert result["sigla_uf"].tolist() == ["SP", "RJ"]
    assert result["rede"].tolist() == ["municipal", "estadual"]


def test_deduplicate_keeps_latest_ingestion() -> None:
    df = pd.DataFrame(
        {
            "ano": [2024, 2024],
            "id_municipio": ["3550308", "3550308"],
            "rede": ["municipal", "municipal"],
            "taxa_alfabetizacao": [0.5, 0.9],
            "_ingestion_timestamp": [
                "2026-01-01T00:00:00+00:00",
                "2026-02-01T00:00:00+00:00",
            ],
        }
    )
    result = deduplicate(df, ("ano", "id_municipio", "rede"), entity_name="meta_municipio")
    assert len(result) == 1
    assert result.iloc[0]["taxa_alfabetizacao"] == 0.9


def test_enrich_meta_uf_adds_territorial_columns(sample_uf: pd.DataFrame) -> None:
    meta_uf = pd.DataFrame(
        {
            "ano": [2024],
            "sigla_uf": ["SP"],
            "rede": ["municipal"],
            "taxa_alfabetizacao": [0.8],
        }
    )
    result = enrich_meta_uf(meta_uf, sample_uf)
    assert result.iloc[0]["nome_uf"] == "São Paulo"
    assert result.iloc[0]["regiao_uf"] == "Sudeste"
    assert bool(result.iloc[0]["_join_match"]) is True


def test_enrich_meta_municipio_preserves_unmatched_rows(
    sample_municipio: pd.DataFrame,
) -> None:
    meta_municipio = pd.DataFrame(
        {
            "ano": [2024, 2024],
            "id_municipio": ["3550308", "9999999"],
            "rede": ["municipal", "municipal"],
            "taxa_alfabetizacao": [0.7, 0.1],
        }
    )
    standardized = standardize_common(meta_municipio)
    result = enrich_meta_municipio(standardized, sample_municipio)

    assert len(result) == 2
    assert result.iloc[0]["nome_municipio"] == "São Paulo"
    assert pd.isna(result.iloc[1]["nome_municipio"])
    assert bool(result.iloc[1]["_join_match"]) is False
