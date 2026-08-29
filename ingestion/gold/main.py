"""
Gold layer entry point — Silver → analytical / ML datasets → Gold.

CLI usage:
  python -m ingestion.gold.main --datasets all --years 2023,2024
  python -m ingestion.gold.main --datasets alunos_features,alunos_analytic --years 2024
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import uuid
from typing import Any

import pandas as pd

from ingestion.batch.connections.aws_credentials import (
    resolve_aws_storage_options,
    resolve_s3_bucket,
)
from ingestion.gold.config import (
    ALL_DATASET_NAMES,
    DEFAULT_YEARS,
    GoldRunConfig,
    build_gold_configs,
    silver_table_path,
)
from ingestion.gold.gold_writer import GoldWriter
from ingestion.gold.quality import assert_gold_contract
from ingestion.gold.silver_reader import read_silver
from ingestion.gold.transforms import (
    build_alunos_analytic,
    build_alunos_features,
    build_contexto_territorio,
    build_indicador_municipio,
    build_indicador_uf,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)


def _get_dbutils() -> Any | None:
    try:
        from pyspark.dbutils import DBUtils  # type: ignore[import-untyped]
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        if spark is not None:
            return DBUtils(spark)
    except Exception:
        pass

    try:
        import IPython  # type: ignore[import-untyped]

        return IPython.get_ipython().user_ns.get("dbutils")
    except Exception:
        return None


def _cli_args_provided() -> bool:
    argv = sys.argv[1:]
    if len(argv) == 1 and argv[0].startswith("--"):
        import shlex

        argv = shlex.split(argv[0])
    return any(arg.startswith("--") for arg in argv)


def _config_from_widgets() -> GoldRunConfig | None:
    dbutils = _get_dbutils()
    if dbutils is None:
        return None

    try:
        datasets_raw = dbutils.widgets.get("datasets")
        years_raw = dbutils.widgets.get("years")
        batch_id = dbutils.widgets.get("batch_id") or None
        overwrite_raw = (dbutils.widgets.get("overwrite") or "true").strip().lower()

        return GoldRunConfig(
            years=_parse_years(years_raw),
            datasets=_parse_datasets(datasets_raw),
            batch_id=batch_id,
            overwrite=overwrite_raw != "false",
        )
    except Exception as exc:
        if "InputWidgetNotDefined" in type(exc).__name__ or "InputWidgetNotDefined" in str(exc):
            logger.debug("Databricks widgets not defined — using CLI/default config.")
        else:
            logger.warning("Could not read Databricks widgets: %s", exc)
        return None


def _parse_datasets(raw: str) -> list[str]:
    if raw.strip().lower() == "all":
        return list(ALL_DATASET_NAMES)

    datasets = [item.strip() for item in raw.split(",") if item.strip()]
    invalid = [name for name in datasets if name not in ALL_DATASET_NAMES]
    if invalid:
        raise ValueError(
            f"Unknown dataset(s): {invalid}. Valid options: {list(ALL_DATASET_NAMES)}"
        )
    return datasets


def _parse_years(raw: str) -> list[int]:
    years = [int(year.strip()) for year in raw.split(",") if year.strip()]
    if not years:
        raise ValueError("At least one valid year is required.")
    return years


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Gold layer — Silver Delta → analytical / ML datasets on S3."
    )
    parser.add_argument(
        "--datasets",
        default="all",
        help="Comma-separated dataset names or 'all'. Default: all.",
    )
    parser.add_argument(
        "--years",
        default=",".join(str(year) for year in DEFAULT_YEARS),
        help="Comma-separated years to process. Default: %(default)s.",
    )
    parser.add_argument(
        "--batch-id",
        default=None,
        help="Gold batch identifier (auto-generated UUID if omitted).",
    )
    parser.add_argument(
        "--append",
        action="store_true",
        help="Use append mode instead of overwrite.",
    )
    parser.add_argument(
        "--skip-quality",
        action="store_true",
        help="Skip post-build Gold contract quality checks.",
    )
    return parser


def build_run_config(args: argparse.Namespace | None = None) -> GoldRunConfig:
    if args is not None or _cli_args_provided():
        resolved = args if args is not None else _build_arg_parser().parse_args()
        return GoldRunConfig(
            years=_parse_years(resolved.years),
            datasets=_parse_datasets(resolved.datasets),
            batch_id=resolved.batch_id,
            overwrite=not resolved.append,
        )

    widget_config = _config_from_widgets()
    if widget_config is not None:
        return widget_config

    resolved = _build_arg_parser().parse_args()
    return GoldRunConfig(
        years=_parse_years(resolved.years),
        datasets=_parse_datasets(resolved.datasets),
        batch_id=resolved.batch_id,
        overwrite=not resolved.append,
    )


def _resolve_bucket() -> str:
    try:
        return resolve_s3_bucket()
    except RuntimeError:
        bucket = os.getenv("S3_BUCKET")
        if bucket:
            logger.info("S3 bucket loaded from S3_BUCKET environment variable.")
            return bucket
        raise


def _read_silver_optional(
    path: str,
    storage_options: dict[str, str],
    *,
    years: list[int] | None = None,
    partition_col: str | None = "ano",
) -> pd.DataFrame:
    """Read a Silver table; return empty frame when unavailable."""
    try:
        return read_silver(
            path,
            storage_options,
            years=years,
            partition_col=partition_col,
        )
    except Exception as exc:
        logger.warning("Optional Silver table unavailable at %s: %s", path, exc)
        return pd.DataFrame()


def run_gold(
    run_config: GoldRunConfig,
    *,
    skip_quality: bool = False,
) -> dict[str, str | None]:
    batch_id = run_config.batch_id or str(uuid.uuid4())
    logger.info("=== GOLD PROCESSING STARTED ===")
    logger.info("Batch ID : %s", batch_id)
    logger.info("Datasets : %s", run_config.datasets)
    logger.info("Years    : %s", run_config.years)

    storage_options = resolve_aws_storage_options()
    bucket = _resolve_bucket()
    gold_configs = build_gold_configs(bucket)
    writer = GoldWriter(storage_options)

    logger.info("S3 bucket: %s", bucket)

    needs_ml = any(
        name in run_config.datasets
        for name in ("contexto_territorio", "alunos_features", "alunos_analytic")
    )
    needs_indicadores = any(
        name in run_config.datasets
        for name in (
            "indicador_crianca_alfabetizada_municipio",
            "indicador_crianca_alfabetizada_uf",
        )
    )

    logger.info("Loading Silver reference tables...")
    alunos = read_silver(
        silver_table_path(bucket, "alunos"),
        storage_options,
        years=run_config.years,
    )
    meta_municipio = read_silver(
        silver_table_path(bucket, "meta_municipio"),
        storage_options,
        years=run_config.years,
    )
    meta_uf = read_silver(
        silver_table_path(bucket, "meta_uf"),
        storage_options,
        years=run_config.years,
    )
    municipio = read_silver(
        silver_table_path(bucket, "municipio"),
        storage_options,
        years=None,
        partition_col=None,
    )

    populacao = pd.DataFrame()
    pib = pd.DataFrame()
    socioeconomico = pd.DataFrame()
    if needs_ml:
        populacao = _read_silver_optional(
            silver_table_path(bucket, "populacao_municipio"),
            storage_options,
            years=None,
        )
        pib = _read_silver_optional(
            silver_table_path(bucket, "pib_municipio"),
            storage_options,
            years=None,
        )
        socioeconomico = _read_silver_optional(
            silver_table_path(bucket, "socioeconomico_municipio"),
            storage_options,
            years=None,
        )

    results: dict[str, str | None] = {}
    contexto = pd.DataFrame()
    alunos_features = pd.DataFrame()

    if needs_ml or "contexto_territorio" in run_config.datasets:
        logger.info("--- Building contexto_territorio ---")
        contexto = build_contexto_territorio(
            meta_municipio,
            municipio,
            alunos=alunos,
            populacao=populacao,
            pib=pib,
            socioeconomico=socioeconomico,
        )
        if "contexto_territorio" in run_config.datasets:
            if not skip_quality:
                assert_gold_contract("contexto_territorio", contexto)
            results["contexto_territorio"] = writer.write(
                contexto,
                gold_configs["contexto_territorio"],
                batch_id=batch_id,
                overwrite=run_config.overwrite,
            )

    if "alunos_features" in run_config.datasets or "alunos_analytic" in run_config.datasets:
        logger.info("--- Building alunos_features ---")
        if contexto.empty:
            contexto = build_contexto_territorio(
                meta_municipio,
                municipio,
                alunos=alunos,
                populacao=populacao,
                pib=pib,
                socioeconomico=socioeconomico,
            )
        alunos_features = build_alunos_features(alunos, contexto)
        if "alunos_features" in run_config.datasets:
            if not skip_quality:
                assert_gold_contract("alunos_features", alunos_features)
            results["alunos_features"] = writer.write(
                alunos_features,
                gold_configs["alunos_features"],
                batch_id=batch_id,
                overwrite=run_config.overwrite,
            )

    if "alunos_analytic" in run_config.datasets:
        logger.info("--- Building alunos_analytic ---")
        if alunos_features.empty:
            if contexto.empty:
                contexto = build_contexto_territorio(
                    meta_municipio,
                    municipio,
                    alunos=alunos,
                    populacao=populacao,
                    pib=pib,
                    socioeconomico=socioeconomico,
                )
            alunos_features = build_alunos_features(alunos, contexto)
        analytic = build_alunos_analytic(alunos_features)
        if not skip_quality:
            assert_gold_contract("alunos_analytic", analytic)
        results["alunos_analytic"] = writer.write(
            analytic,
            gold_configs["alunos_analytic"],
            batch_id=batch_id,
            overwrite=run_config.overwrite,
        )

    if "indicador_crianca_alfabetizada_municipio" in run_config.datasets:
        logger.info("--- Building indicador_crianca_alfabetizada_municipio ---")
        indicador_mun = build_indicador_municipio(alunos, meta_municipio)
        if not skip_quality:
            assert_gold_contract(
                "indicador_crianca_alfabetizada_municipio", indicador_mun
            )
        results["indicador_crianca_alfabetizada_municipio"] = writer.write(
            indicador_mun,
            gold_configs["indicador_crianca_alfabetizada_municipio"],
            batch_id=batch_id,
            overwrite=run_config.overwrite,
        )

    if "indicador_crianca_alfabetizada_uf" in run_config.datasets:
        logger.info("--- Building indicador_crianca_alfabetizada_uf ---")
        indicador_uf = build_indicador_uf(alunos, municipio, meta_uf)
        if not skip_quality:
            assert_gold_contract("indicador_crianca_alfabetizada_uf", indicador_uf)
        results["indicador_crianca_alfabetizada_uf"] = writer.write(
            indicador_uf,
            gold_configs["indicador_crianca_alfabetizada_uf"],
            batch_id=batch_id,
            overwrite=run_config.overwrite,
        )

    if not needs_indicadores and not needs_ml:
        logger.warning("No datasets selected.")

    logger.info("=== GOLD PROCESSING COMPLETED ===")
    return results


def _normalize_argv() -> None:
    if len(sys.argv) == 2 and sys.argv[1].startswith("--"):
        import shlex

        extra = shlex.split(sys.argv[1])
        sys.argv = [sys.argv[0]] + extra


def main() -> None:
    _normalize_argv()
    parser = _build_arg_parser()
    args = parser.parse_args()
    run_config = build_run_config(args)

    if not run_config.batch_id:
        run_config.batch_id = str(uuid.uuid4())

    run_gold(run_config, skip_quality=args.skip_quality)


if __name__ == "__main__":
    main()
