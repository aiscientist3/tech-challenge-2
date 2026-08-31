"""Unit tests for Gold transformations."""

from __future__ import annotations

import pandas as pd

from ingestion.gold.transforms import (
    add_gap_analysis,
    as_of_join,
    build_alunos_analytic,
    build_alunos_features,
    build_contexto_territorio,
    build_indicador_municipio,
    build_indicador_uf,
    normalize_alfabetizado_flag,
    snapshot_join,
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


def test_as_of_join_picks_latest_year_not_after_left() -> None:
    left = pd.DataFrame(
        {
            "ano": [2024, 2023],
            "id_municipio": ["3550308", "3550308"],
        }
    )
    right = pd.DataFrame(
        {
            "ano": [2021, 2022, 2023],
            "id_municipio": ["3550308", "3550308", "3550308"],
            "populacao": [10.0, 20.0, 30.0],
        }
    )
    result = as_of_join(
        left,
        right,
        by="id_municipio",
        time_col="ano",
        value_cols=["populacao"],
        right_time_alias="populacao_ano_ref",
    )
    by_year = result.set_index("ano")
    assert by_year.loc[2024, "populacao"] == 30.0
    assert by_year.loc[2024, "populacao_ano_ref"] == 2023
    assert by_year.loc[2023, "populacao"] == 30.0


def test_snapshot_join_broadcasts_latest_year() -> None:
    left = pd.DataFrame({"ano": [2024], "id_municipio": ["3550308"]})
    right = pd.DataFrame(
        {
            "ano": [2000, 2010],
            "id_municipio": ["3550308", "3550308"],
            "ivs": [0.5, 0.2],
        }
    )
    result = snapshot_join(
        left,
        right,
        by="id_municipio",
        time_col="ano",
        value_cols=["ivs"],
        year_alias="socio_ano_ref",
    )
    assert result.iloc[0]["ivs"] == 0.2
    assert result.iloc[0]["socio_ano_ref"] == 2010


def test_contexto_lags_inep_and_joins_national_meta(
    sample_meta_municipio: pd.DataFrame,
    sample_meta_uf: pd.DataFrame,
    sample_meta_brasil: pd.DataFrame,
    sample_municipio: pd.DataFrame,
) -> None:
    populacao = pd.DataFrame(
        {"ano": [2022], "id_municipio": ["3550308"], "populacao": [12_000_000]}
    )
    pib = pd.DataFrame(
        {"ano": [2021], "id_municipio": ["3550308"], "pib": [24_000_000.0]}
    )
    socio = pd.DataFrame(
        {"ano": [2010], "id_municipio": ["3550308"], "ivs": [0.25]}
    )
    mun_ind = pd.DataFrame(
        {
            "ano": [2023, 2024],
            "id_municipio": ["3550308", "3550308"],
            "serie": ["2º ano", "2º ano"],
            "rede": ["municipal", "municipal"],
            "taxa_alfabetizacao": [40.0, 99.0],
            "media_portugues": [700.0, 900.0],
        }
    )
    uf_ind = pd.DataFrame(
        {
            "ano": [2023],
            "sigla_uf": ["SP"],
            "serie": ["2º ano"],
            "rede": ["municipal"],
            "taxa_alfabetizacao": [42.0],
            "media_portugues": [710.0],
        }
    )

    contexto = build_contexto_territorio(
        meta_municipio=sample_meta_municipio,
        meta_uf=sample_meta_uf,
        meta_brasil=sample_meta_brasil,
        municipio=sample_municipio,
        populacao=populacao,
        pib=pib,
        socioeconomico=socio,
        municipio_indicadores=mun_ind,
        uf_indicadores=uf_ind,
    )
    sp = contexto[contexto["id_municipio"] == "3550308"].iloc[0]
    assert sp["nome_regiao"] == "Sudeste"
    assert sp["brasil_meta_alfabetizacao_2024"] == 68.0
    assert sp["populacao"] == 12_000_000
    assert sp["lag1_taxa_alfabetizacao"] == 40.0
    assert sp["lag1_media_portugues"] == 700.0
    assert sp["taxa_alfabetizacao"] == 55.0


def test_alunos_features_drops_proficiencia_and_same_year_rate(
    sample_alunos: pd.DataFrame,
    sample_meta_municipio: pd.DataFrame,
    sample_meta_uf: pd.DataFrame,
    sample_meta_brasil: pd.DataFrame,
    sample_municipio: pd.DataFrame,
) -> None:
    contexto = build_contexto_territorio(
        meta_municipio=sample_meta_municipio,
        meta_uf=sample_meta_uf,
        meta_brasil=sample_meta_brasil,
        municipio=sample_municipio,
        populacao=pd.DataFrame(),
        pib=pd.DataFrame(),
        socioeconomico=pd.DataFrame(),
        municipio_indicadores=pd.DataFrame(),
        uf_indicadores=pd.DataFrame(),
    )
    features = build_alunos_features(sample_alunos, contexto)

    assert "proficiencia" not in features.columns
    assert "taxa_alfabetizacao" not in features.columns
    assert "serie" in features.columns
    sp = features[features["id_aluno"] == "A1"].iloc[0]
    assert sp["alfabetizado"] == 1.0
    assert sp["meta_alfabetizacao_2024"] == 70.0
    assert sp["nome_regiao"] == "Sudeste"


def test_alunos_features_maps_rede_codes_for_join(
    sample_meta_municipio: pd.DataFrame,
    sample_meta_uf: pd.DataFrame,
    sample_meta_brasil: pd.DataFrame,
    sample_municipio: pd.DataFrame,
) -> None:
    alunos = pd.DataFrame(
        {
            "ano": [2024, 2024],
            "id_municipio": ["3550308", "3304557"],
            "id_aluno": ["A1", "A3"],
            "serie": ["2º ano", "2º ano"],
            "rede": ["3", "2"],
            "alfabetizado": ["Sim", "Sim"],
            "peso_aluno": [1.0, 1.0],
            "proficiencia": [800.0, 750.0],
        }
    )
    contexto = build_contexto_territorio(
        meta_municipio=sample_meta_municipio,
        meta_uf=sample_meta_uf,
        meta_brasil=sample_meta_brasil,
        municipio=sample_municipio,
        populacao=pd.DataFrame(),
        pib=pd.DataFrame(),
        socioeconomico=pd.DataFrame(),
        municipio_indicadores=pd.DataFrame(),
        uf_indicadores=pd.DataFrame(),
        alunos=alunos,
    )
    features = build_alunos_features(alunos, contexto)
    assert set(features["rede"].tolist()) == {"municipal", "estadual"}
    assert features["_join_match"].all()
    assert features["nome_municipio"].notna().all()

    analytic = build_alunos_analytic(features)
    assert "alfabetizado" in analytic.columns
    assert "proficiencia" not in analytic.columns
    assert "_join_match" not in analytic.columns
