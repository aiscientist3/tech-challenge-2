"""
Gold layer entry point — Silver → analytical indicators → Gold.

CLI usage:
  python -m ingestion.gold.main --datasets all --years 2023,2024
  python -m ingestion.gold.main --datasets indicador_crianca_alfabetizada_municipio --years 2024
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import uuid
from typing import Any

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
from ingestion.gold.silver_reader import read_silver
from ingestion.gold.transforms import build_indicador_municipio, build_indicador_uf

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
        description="Gold layer — Silver Delta → analytical indicators on S3."
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


def run_gold(run_config: GoldRunConfig) -> dict[str, str | None]:
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

    results: dict[str, str | None] = {}

    if "indicador_crianca_alfabetizada_municipio" in run_config.datasets:
        logger.info("--- Building indicador_crianca_alfabetizada_municipio ---")
        indicador_mun = build_indicador_municipio(alunos, meta_municipio)
        results["indicador_crianca_alfabetizada_municipio"] = writer.write(
            indicador_mun,
            gold_configs["indicador_crianca_alfabetizada_municipio"],
            batch_id=batch_id,
            overwrite=run_config.overwrite,
        )

    if "indicador_crianca_alfabetizada_uf" in run_config.datasets:
        logger.info("--- Building indicador_crianca_alfabetizada_uf ---")
        indicador_uf = build_indicador_uf(alunos, municipio, meta_uf)
        results["indicador_crianca_alfabetizada_uf"] = writer.write(
            indicador_uf,
            gold_configs["indicador_crianca_alfabetizada_uf"],
            batch_id=batch_id,
            overwrite=run_config.overwrite,
        )

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

    run_gold(run_config)


if __name__ == "__main__":
    main()
