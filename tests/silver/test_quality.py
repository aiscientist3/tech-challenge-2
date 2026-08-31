"""Unit tests for Silver layer data quality validators (lean mode)."""

from __future__ import annotations

import pandas as pd
import pytest

from ingestion.silver.quality import load_quality_rules, validate_entity


@pytest.fixture
def municipio_ref() -> pd.DataFrame:
    return pd.DataFrame({"id_municipio": ["3550308", "3304557"]})


class TestLoadQualityRules:
    def test_loads_alunos_rules_from_catalog(self) -> None:
        rules = load_quality_rules("alunos")
        rule_ids = {rule.id for rule in rules}
        assert "alunos_ano_obrigatorio" in rule_ids
        assert "alunos_id_aluno_obrigatorio" in rule_ids
        assert "alunos_municipio_fk" in rule_ids
        assert "alunos_chave_unica" not in rule_ids

    def test_loads_meta_municipio_rules_from_catalog(self) -> None:
        rules = load_quality_rules("meta_municipio")
        rule_ids = {rule.id for rule in rules}
        assert "meta_municipio_fk" in rule_ids
        assert "meta_municipio_taxas_faixa" not in rule_ids


class TestValidateEntity:
    def test_completude_quarantines_null_ano(
        self, municipio_ref: pd.DataFrame
    ) -> None:
        df = pd.DataFrame(
            {
                "ano": [2024, None],
                "id_aluno": ["A1", "A2"],
                "id_municipio": ["3550308", "3550308"],
            }
        )
        result = validate_entity(df, "alunos", {"municipio": municipio_ref})
        assert len(result.valid_df) == 1
        assert result.quarantine_count == 1
        assert result.summary["alunos_ano_obrigatorio"] == 1

    def test_dominio_quarantines_invalid_municipio_format(self) -> None:
        df = pd.DataFrame(
            {
                "ano": [2024],
                "id_aluno": ["A1"],
                "id_municipio": ["invalid"],
            }
        )
        result = validate_entity(df, "alunos", {})
        assert result.quarantine_count == 1
        assert result.summary["alunos_id_municipio_formato"] == 1

    def test_referencial_quarantines_unknown_municipio(
        self, municipio_ref: pd.DataFrame
    ) -> None:
        df = pd.DataFrame(
            {
                "ano": [2024, 2024],
                "id_aluno": ["A1", "A2"],
                "id_municipio": ["3550308", "9999999"],
            }
        )
        result = validate_entity(df, "alunos", {"municipio": municipio_ref})
        assert len(result.valid_df) == 1
        assert result.quarantine_count == 1
        assert result.summary["alunos_municipio_fk"] == 1

    def test_referencial_skipped_when_reference_unavailable(self) -> None:
        df = pd.DataFrame(
            {
                "ano": [2024],
                "id_aluno": ["A1"],
                "id_municipio": ["3550308"],
            }
        )
        result = validate_entity(df, "alunos", {})
        assert result.quarantine_count == 0

    def test_valid_rows_pass_all_rules(
        self, municipio_ref: pd.DataFrame
    ) -> None:
        df = pd.DataFrame(
            {
                "ano": [2024],
                "id_aluno": ["A1"],
                "id_municipio": ["3550308"],
            }
        )
        result = validate_entity(df, "alunos", {"municipio": municipio_ref})
        assert result.quarantine_count == 0
        assert len(result.valid_df) == 1
