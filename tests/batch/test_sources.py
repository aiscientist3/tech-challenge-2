"""Unit tests for the batch ingestion pipeline (Bronze layer)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from ingestion.batch.bronze_writer import BronzeWriter
from ingestion.batch.config import SOURCE_CONFIGS, bronze_s3_path
from ingestion.batch.main import _parse_sources, _parse_years, build_run_config
from ingestion.batch.sources import SOURCE_REGISTRY


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

class TestConfig:
    def test_all_sources_registered(self):
        assert set(SOURCE_CONFIGS.keys()) == set(SOURCE_REGISTRY.keys())

    def test_bronze_s3_path_format(self):
        path = bronze_s3_path("meta_uf")
        assert path.startswith("s3a://")
        assert path.endswith("/meta_uf")

    def test_reference_sources_skip_year_filter(self):
        assert SOURCE_CONFIGS["uf"].filter_by_year is False
        assert SOURCE_CONFIGS["municipio"].filter_by_year is False

    def test_indicator_sources_apply_year_filter(self):
        assert SOURCE_CONFIGS["meta_brasil"].filter_by_year is True
        assert SOURCE_CONFIGS["meta_uf"].filter_by_year is True
        assert SOURCE_CONFIGS["alunos"].filter_by_year is True

    def test_reference_sources_have_no_year_partition(self):
        assert SOURCE_CONFIGS["uf"].partition_by is None
        assert SOURCE_CONFIGS["municipio"].partition_by is None

    def test_indicator_sources_are_partitioned_by_year(self):
        for name in ("meta_brasil", "meta_uf", "meta_municipio", "alunos"):
            assert SOURCE_CONFIGS[name].partition_by == "ano"


# ---------------------------------------------------------------------------
# Query building
# ---------------------------------------------------------------------------

class TestQueryBuilding:
    @pytest.mark.parametrize("source_name", list(SOURCE_REGISTRY.keys()))
    def test_query_references_correct_table(self, source_instances, source_name):
        query = source_instances[source_name].build_query()
        assert source_instances[source_name].source_config.bq_table in query

    @pytest.mark.parametrize("source_name", list(SOURCE_REGISTRY.keys()))
    def test_query_starts_with_select(self, source_instances, source_name):
        query = source_instances[source_name].build_query()
        assert query.strip().upper().startswith("SELECT")

    def test_year_filter_applied_to_meta_uf(self, source_instances):
        query = source_instances["meta_uf"].build_query()
        assert "ano IN (2023, 2024)" in query

    def test_year_filter_not_applied_to_uf(self, source_instances):
        query = source_instances["uf"].build_query()
        assert "ano IN" not in query

    def test_row_limit_applied(self, source_instances):
        query = source_instances["meta_municipio"].build_query()
        assert "LIMIT 1000" in query

    def test_uf_query_includes_key_columns(self, source_instances):
        query = source_instances["uf"].build_query()
        assert "sigla_uf" in query
        assert "nome" in query

    def test_municipio_query_includes_territorial_columns(self, source_instances):
        query = source_instances["municipio"].build_query()
        assert "id_municipio" in query
        assert "sigla_uf" in query
        assert "nome_regiao" in query


# ---------------------------------------------------------------------------
# Argument parsing (main.py)
# ---------------------------------------------------------------------------

class TestArgumentParsing:
    def test_parse_sources_all(self):
        assert _parse_sources("all") == list(SOURCE_CONFIGS.keys())

    def test_parse_sources_subset(self):
        assert _parse_sources("uf,meta_brasil") == ["uf", "meta_brasil"]

    def test_parse_sources_strips_whitespace(self):
        assert _parse_sources(" uf , meta_brasil ") == ["uf", "meta_brasil"]

    def test_parse_sources_invalid_raises(self):
        with pytest.raises(ValueError, match="Unknown source"):
            _parse_sources("nonexistent_source")

    def test_parse_years_single(self):
        assert _parse_years("2024") == [2024]

    def test_parse_years_multiple(self):
        assert _parse_years("2023,2024") == [2023, 2024]

    def test_build_run_config_from_args(self):
        args = MagicMock(
            sources="uf,meta_brasil",
            years="2023",
            batch_id="abc-123",
            row_limit=500,
            append=False,
        )
        config = build_run_config(args)
        assert config.sources == ["uf", "meta_brasil"]
        assert config.years == [2023]
        assert config.batch_id == "abc-123"
        assert config.row_limit == 500
        assert config.overwrite is True

    def test_build_run_config_append_mode(self):
        args = MagicMock(
            sources="alunos",
            years="2024",
            batch_id=None,
            row_limit=None,
            append=True,
        )
        config = build_run_config(args)
        assert config.overwrite is False


# ---------------------------------------------------------------------------
# BronzeWriter
# ---------------------------------------------------------------------------

class TestBronzeWriter:
    def test_validate_raises_on_missing_required_columns(self):
        writer = BronzeWriter(spark=MagicMock())
        df = pd.DataFrame({"wrong_column": [1]})
        with pytest.raises(ValueError, match="missing required columns"):
            writer.validate(df, SOURCE_CONFIGS["uf"])

    def test_validate_passes_with_correct_columns(self):
        writer = BronzeWriter(spark=MagicMock())
        df = pd.DataFrame({"sigla_uf": ["SP"], "nome": ["São Paulo"]})
        writer.validate(df, SOURCE_CONFIGS["uf"])

    def test_validate_empty_dataframe_does_not_raise(self):
        writer = BronzeWriter(spark=MagicMock())
        writer.validate(pd.DataFrame(), SOURCE_CONFIGS["uf"])

    def test_write_returns_none_for_empty_dataframe(self):
        writer = BronzeWriter(spark=MagicMock())
        result = writer.write(pd.DataFrame(), SOURCE_CONFIGS["uf"], batch_id="x")
        assert result is None

    def test_write_attaches_metadata_columns(self):
        mock_spark = MagicMock()
        mock_sdf = MagicMock()
        mock_spark.createDataFrame.return_value = mock_sdf
        mock_sdf.withColumn.return_value = mock_sdf
        mock_sdf.columns = ["ano", "sigla_uf"]
        mock_sdf.count.return_value = 2

        mock_write_chain = MagicMock()
        mock_sdf.write.format.return_value = mock_write_chain
        mock_write_chain.mode.return_value = mock_write_chain
        mock_write_chain.option.return_value = mock_write_chain
        mock_write_chain.partitionBy.return_value = mock_write_chain

        writer = BronzeWriter(spark=mock_spark)
        df = pd.DataFrame({"ano": [2023], "sigla_uf": ["SP"]})
        destination = writer.write(df, SOURCE_CONFIGS["meta_uf"], batch_id="batch-xyz")

        assert destination == SOURCE_CONFIGS["meta_uf"].bronze_path
        assert mock_sdf.withColumn.call_count == 3  # timestamp, table, batch_id
        mock_write_chain.partitionBy.assert_called_once_with("ano")
        mock_write_chain.save.assert_called_once_with(SOURCE_CONFIGS["meta_uf"].bronze_path)

    def test_write_skips_partition_for_reference_sources(self):
        mock_spark = MagicMock()
        mock_sdf = MagicMock()
        mock_spark.createDataFrame.return_value = mock_sdf
        mock_sdf.withColumn.return_value = mock_sdf
        mock_sdf.columns = ["sigla_uf", "nome"]
        mock_sdf.count.return_value = 27

        mock_write_chain = MagicMock()
        mock_sdf.write.format.return_value = mock_write_chain
        mock_write_chain.mode.return_value = mock_write_chain
        mock_write_chain.option.return_value = mock_write_chain

        writer = BronzeWriter(spark=mock_spark)
        df = pd.DataFrame({"sigla_uf": ["SP"], "nome": ["São Paulo"]})
        writer.write(df, SOURCE_CONFIGS["uf"], batch_id="b1")

        mock_write_chain.partitionBy.assert_not_called()


# ---------------------------------------------------------------------------
# BaseSource — extract with retry
# ---------------------------------------------------------------------------

class TestExtractRetry:
    def test_retries_once_on_transient_error(self, source_instances, mock_bq_client):
        source = source_instances["uf"]
        success_df = pd.DataFrame({"sigla_uf": ["SP"], "nome": ["São Paulo"]})
        mock_bq_client.query.side_effect = [
            RuntimeError("timeout"),
            MagicMock(to_dataframe=lambda: success_df),
        ]

        with patch("ingestion.batch.sources.base_source.time.sleep"):
            df = source.extract()

        assert len(df) == 1
        assert mock_bq_client.query.call_count == 2

    def test_raises_after_max_retries(self, source_instances, mock_bq_client):
        source = source_instances["uf"]
        mock_bq_client.query.side_effect = RuntimeError("persistent failure")

        with patch("ingestion.batch.sources.base_source.time.sleep"):
            with pytest.raises(RuntimeError, match="Failed to extract source"):
                source.extract()

        assert mock_bq_client.query.call_count == 3

    def test_no_retry_on_success(self, source_instances, mock_bq_client):
        source = source_instances["meta_brasil"]
        mock_bq_client.query.return_value = MagicMock(
            to_dataframe=lambda: pd.DataFrame({"ano": [2023]})
        )

        with patch("ingestion.batch.sources.base_source.time.sleep") as mock_sleep:
            source.extract()

        mock_sleep.assert_not_called()
        assert mock_bq_client.query.call_count == 1
