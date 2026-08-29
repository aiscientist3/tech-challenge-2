"""Unit tests for Gold transformations."""

from __future__ import annotations

import pandas as pd

from ingestion.gold.transforms import (
    add_gap_analysis,
    build_alunos_analytic,
    build_alunos_features,
    build_contexto_territorio,
    build_indicador_municipio,
    build_indicador_uf,
    normalize_alfabetizado_flag,
)


def test_normalize_alfabetizado_flag() -> None:
    series = pd.Series(["Sim", "Não", "1", "0", None])
    result = normalize_alfabetizado_flag(series)
    assert result.tolist() == [1.0, 0.0, 1.0, 0.0, 0.0]


def test_build_indicador_municipio_weighted_rate(
    sample_alunos: pd.DataFrame,
    sample_meta_municipio: pd.DataFrame,
) -> None:
    result = build_indicador_municipio(sample_alunos, sample_meta_municipio)

    sp_municipal = result[
        (result["id_municipio"] == "3550308") & (result["rede"] == "municipal")
    ].iloc[0]
    assert sp_municipal["total_alunos"] == 2
    assert sp_municipal["taxa_crianca_alfabetizada"] == 50.0
    assert sp_municipal["gap_taxa_vs_inep"] == -5.0
    assert sp_municipal["gap_meta_2024"] == -20.0
    assert sp_municipal["nome_municipio"] == "São Paulo"


def test_build_indicador_municipio_maps_rede_codes(
    sample_alunos_coded: pd.DataFrame,
    sample_meta_municipio: pd.DataFrame,
) -> None:
    result = build_indicador_municipio(sample_alunos_coded, sample_meta_municipio)
    assert set(result["rede"].tolist()) == {"municipal", "estadual"}
    assert "3" not in set(result["rede"].astype(str))


def test_build_indicador_uf(
    sample_alunos: pd.DataFrame,
    sample_municipio: pd.DataFrame,
    sample_meta_uf: pd.DataFrame,
) -> None:
    result = build_indicador_uf(sample_alunos, sample_municipio, sample_meta_uf)

    sp_municipal = result[
        (result["sigla_uf"] == "SP") & (result["rede"] == "municipal")
    ].iloc[0]
    assert sp_municipal["taxa_crianca_alfabetizada"] == 50.0
    assert sp_municipal["gap_meta_2030"] == -50.0


def test_add_gap_analysis_creates_meta_gaps() -> None:
    indicator = pd.DataFrame(
        {
            "ano": [2024],
            "sigla_uf": ["SP"],
            "rede": ["municipal"],
            "taxa_crianca_alfabetizada": [80.0],
        }
    )
    meta = pd.DataFrame(
        {
            "ano": [2024],
            "sigla_uf": ["SP"],
            "rede": ["municipal"],
            "taxa_alfabetizacao": [75.0],
            "meta_alfabetizacao_2024": [85.0],
        }
    )

    result = add_gap_analysis(
        indicator,
        meta,
        join_keys=["ano", "sigla_uf", "rede"],
    )

    assert result.iloc[0]["gap_taxa_vs_inep"] == 5.0
    assert result.iloc[0]["gap_meta_2024"] == -5.0


def test_contexto_and_alunos_features_join_with_coded_rede(
    sample_alunos_coded: pd.DataFrame,
    sample_meta_municipio: pd.DataFrame,
    sample_municipio: pd.DataFrame,
    sample_populacao: pd.DataFrame,
    sample_pib: pd.DataFrame,
    sample_socioeconomico: pd.DataFrame,
) -> None:
    contexto = build_contexto_territorio(
        sample_meta_municipio,
        sample_municipio,
        alunos=sample_alunos_coded,
        populacao=sample_populacao,
        pib=sample_pib,
        socioeconomico=sample_socioeconomico,
    )
    assert set(contexto["rede"].tolist()) == {"municipal", "estadual"}
    assert contexto["populacao"].notna().all()
    assert contexto["ivs"].notna().all()
    assert "pib_per_capita" in contexto.columns

    features = build_alunos_features(sample_alunos_coded, contexto)
    assert len(features) == 4
    assert features["_join_match"].all()
    assert features["nome_municipio"].notna().all()
    assert set(features["rede"].tolist()) == {"municipal", "estadual"}
    assert set(features["alfabetizado"].tolist()) == {0.0, 1.0}

    analytic = build_alunos_analytic(features)
    assert "alfabetizado" in analytic.columns
    assert "_silver_batch_id" not in analytic.columns
    assert "proficiencia" not in analytic.columns
    assert "nivel_alfabetizacao" not in analytic.columns
