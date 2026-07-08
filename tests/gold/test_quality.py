"""Unit tests for Gold layer cross-table quality checks (lean mode)."""

from __future__ import annotations

import pandas as pd

from ingestion.gold.quality import (
    load_gold_quality_rules,
    log_meta_coverage_warning,
    validate_indicador_municipio,
    validate_indicador_uf,
)


def _municipio_ref() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "id_municipio": ["3550308", "3304557"],
            "sigla_uf": ["SP", "RJ"],
        }
    )


class TestLoadGoldQualityRules:
    def test_loads_municipio_active_rules(self) -> None:
        rules = load_gold_quality_rules("indicador_crianca_alfabetizada_municipio")
        rule_ids = {rule.id for rule in rules}
        assert rule_ids == {"gold_taxa_faixa", "gold_referencia_territorial"}


class TestGoldQuality:
    def test_quarantines_rate_out_of_range(self) -> None:
        indicator = pd.DataFrame(
            {
                "ano": [2024],
                "id_municipio": ["3550308"],
                "rede": ["municipal"],
                "taxa_crianca_alfabetizada": [120.0],
                "taxa_alfabetizacao": [80.0],
            }
        )
        result = validate_indicador_municipio(
            indicator,
            meta_municipio=pd.DataFrame(),
            municipio=_municipio_ref(),
        )
        assert result.quarantine_count == 1
        assert result.summary["gold_taxa_faixa"] == 1

    def test_quarantines_unknown_municipio(self) -> None:
        indicator = pd.DataFrame(
            {
                "ano": [2024],
                "id_municipio": ["9999999"],
                "rede": ["municipal"],
                "taxa_crianca_alfabetizada": [75.0],
                "taxa_alfabetizacao": [70.0],
            }
        )
        result = validate_indicador_municipio(
            indicator,
            meta_municipio=pd.DataFrame(),
            municipio=_municipio_ref(),
        )
        assert result.quarantine_count == 1
        assert result.summary["gold_referencia_territorial"] == 1

    def test_missing_meta_rate_logs_only_not_quarantine(self, caplog) -> None:
        import logging

        indicator = pd.DataFrame(
            {
                "ano": [2024],
                "id_municipio": ["3550308"],
                "rede": ["municipal"],
                "taxa_crianca_alfabetizada": [75.0],
                "taxa_alfabetizacao": [None],
            }
        )
        with caplog.at_level(logging.WARNING):
            log_meta_coverage_warning(
                indicator,
                dataset_name="indicador_crianca_alfabetizada_municipio",
            )
            result = validate_indicador_municipio(
                indicator,
                meta_municipio=pd.DataFrame(),
                municipio=_municipio_ref(),
            )
        assert result.quarantine_count == 0
        assert "without official INEP rate" in caplog.text

    def test_valid_uf_indicator_passes(self) -> None:
        indicator = pd.DataFrame(
            {
                "ano": [2024],
                "sigla_uf": ["SP"],
                "rede": ["municipal"],
                "taxa_crianca_alfabetizada": [75.0],
                "taxa_alfabetizacao": [70.0],
            }
        )
        result = validate_indicador_uf(
            indicator,
            meta_uf=pd.DataFrame(),
            municipio=_municipio_ref(),
        )
        assert result.quarantine_count == 0
        assert len(result.valid_df) == 1
