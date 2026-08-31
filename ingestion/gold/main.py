"""
Gold layer entry point — Silver → analytical indicators → Gold.

CLI usage:
  python -m ingestion.gold.main --datasets all --years 2023,2024
  python -m ingestion.gold.main --datasets indicador_crianca_alfabetizada_municipio --years 2024
  python -m ingestion.gold.main --datasets contexto_territorio,alunos_features --years 2024
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import uuid

import pandas as pd

from ingestion.batch.metrics import publish_quality_metrics
from ingestion.common.dbutils import get_dbutils
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
from ingestion.gold.quality import (
    GoldQualityResult,
    log_meta_coverage_warning,
    validate_alunos_features,
    validate_contexto_territorio,
    validate_indicador_municipio,
    validate_indicador_uf,
)
from ingestion.gold.silver_reader import read_silver
from ingestion.gold.transforms import (
    attach_prefixed_meta,
    attach_territorio_municipio,
    build_alunos_analytic,
    build_alunos_features,
    build_contexto_territorio,
    build_indicador_municipio,
    build_indicador_uf,
)
from ingestion.silver.quarantine_writer import QuarantineWriter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)


def _cli_args_provided() -> bool:
    argv = sys.argv[1:]
    if len(argv) == 1 and argv[0].startswith("--"):
        import shlex

        argv = shlex.split(argv[0])
    return any(arg.startswith("--") for arg in argv)


def _config_from_widgets() -> GoldRunConfig | None:
    dbutils = get_dbutils()
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


def _read_optional_silver(
    bucket: str,
    entity: str,
    storage_options: dict[str, str],
    *,
    years: list[int] | None,
    partition_col: str | None,
) -> pd.DataFrame:
    """Read a Silver table; missing/unreadable tables become empty frames."""
    path = silver_table_path(bucket, entity)
    try:
        return read_silver(
            path,
            storage_options,
            years=years,
            partition_col=partition_col,
        )
    except RuntimeError as exc:
        logger.warning("Silver table '%s' unavailable (%s). Using empty frame.", entity, exc)
        return pd.DataFrame()


def _publish(
    *,
    name: str,
    quality: GoldQualityResult,
    writer: GoldWriter,
    quarantine_writer: QuarantineWriter,
    gold_configs: dict,
    batch_id: str,
    overwrite: bool,
    results: dict[str, str | None],
    quarantine_counts: dict[str, int],
    record_counts: dict[str, int],
) -> None:
    if not quality.quarantine_df.empty:
        quarantine_writer.write(
            quality.quarantine_df,
            entity_name=name,
            batch_id=batch_id,
            partition_by="ano",
            layer="gold",
        )
    results[name] = writer.write(
        quality.valid_df,
        gold_configs[name],
        batch_id=batch_id,
        overwrite=overwrite,
    )
    quarantine_counts[name] = quality.quarantine_count
    record_counts[name] = len(quality.valid_df)


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
    quarantine_writer = QuarantineWriter(storage_options, bucket)

    logger.info("S3 bucket: %s", bucket)

    logger.info("Loading Silver tables...")
    alunos = _read_optional_silver(
        bucket, "alunos", storage_options, years=run_config.years, partition_col="ano"
    )
    meta_municipio = _read_optional_silver(
        bucket, "meta_municipio", storage_options, years=run_config.years, partition_col="ano"
    )
    meta_uf = _read_optional_silver(
        bucket, "meta_uf", storage_options, years=run_config.years, partition_col="ano"
    )
    meta_brasil = _read_optional_silver(
        bucket, "meta_brasil", storage_options, years=run_config.years, partition_col="ano"
    )
    municipio = _read_optional_silver(
        bucket, "municipio", storage_options, years=None, partition_col=None
    )
    populacao = _read_optional_silver(
        bucket, "populacao_municipio", storage_options, years=None, partition_col="ano"
    )
    pib = _read_optional_silver(
        bucket, "pib_municipio", storage_options, years=None, partition_col="ano"
    )
    socioeconomico = _read_optional_silver(
        bucket, "socioeconomico_municipio", storage_options, years=None, partition_col="ano"
    )
    municipio_indicadores = _read_optional_silver(
        bucket, "municipio_indicadores", storage_options, years=None, partition_col="ano"
    )
    uf_indicadores = _read_optional_silver(
        bucket, "uf_indicadores", storage_options, years=None, partition_col="ano"
    )

    results: dict[str, str | None] = {}
    quarantine_counts: dict[str, int] = {}
    record_counts: dict[str, int] = {}

    if "indicador_crianca_alfabetizada_municipio" in run_config.datasets:
        logger.info("--- Building indicador_crianca_alfabetizada_municipio ---")
        indicador_mun = build_indicador_municipio(alunos, meta_municipio)
        indicador_mun = attach_territorio_municipio(indicador_mun, municipio)
        indicador_mun = attach_prefixed_meta(
            indicador_mun, meta_brasil, ["ano", "rede"], prefix="brasil_"
        )
        log_meta_coverage_warning(
            indicador_mun,
            dataset_name="indicador_crianca_alfabetizada_municipio",
        )
        mun_quality = validate_indicador_municipio(
            indicador_mun, meta_municipio, municipio
        )
        _publish(
            name="indicador_crianca_alfabetizada_municipio",
            quality=mun_quality,
            writer=writer,
            quarantine_writer=quarantine_writer,
            gold_configs=gold_configs,
            batch_id=batch_id,
            overwrite=run_config.overwrite,
            results=results,
            quarantine_counts=quarantine_counts,
            record_counts=record_counts,
        )

    if "indicador_crianca_alfabetizada_uf" in run_config.datasets:
        logger.info("--- Building indicador_crianca_alfabetizada_uf ---")
        indicador_uf = build_indicador_uf(alunos, municipio, meta_uf)
        indicador_uf = attach_prefixed_meta(
            indicador_uf, meta_brasil, ["ano", "rede"], prefix="brasil_"
        )
        log_meta_coverage_warning(
            indicador_uf,
            dataset_name="indicador_crianca_alfabetizada_uf",
        )
        uf_quality = validate_indicador_uf(indicador_uf, meta_uf, municipio)
        _publish(
            name="indicador_crianca_alfabetizada_uf",
            quality=uf_quality,
            writer=writer,
            quarantine_writer=quarantine_writer,
            gold_configs=gold_configs,
            batch_id=batch_id,
            overwrite=run_config.overwrite,
            results=results,
            quarantine_counts=quarantine_counts,
            record_counts=record_counts,
        )

    contexto = pd.DataFrame()
    need_contexto = any(
        name in run_config.datasets
        for name in ("contexto_territorio", "alunos_features", "alunos_analytic")
    )
    if need_contexto:
        logger.info("--- Building contexto_territorio ---")
        contexto = build_contexto_territorio(
            meta_municipio=meta_municipio,
            meta_uf=meta_uf,
            meta_brasil=meta_brasil,
            municipio=municipio,
            populacao=populacao,
            pib=pib,
            socioeconomico=socioeconomico,
            municipio_indicadores=municipio_indicadores,
            uf_indicadores=uf_indicadores,
            alunos=alunos,
        )

    if "contexto_territorio" in run_config.datasets:
        ctx_quality = validate_contexto_territorio(contexto, municipio)
        _publish(
            name="contexto_territorio",
            quality=ctx_quality,
            writer=writer,
            quarantine_writer=quarantine_writer,
            gold_configs=gold_configs,
            batch_id=batch_id,
            overwrite=run_config.overwrite,
            results=results,
            quarantine_counts=quarantine_counts,
            record_counts=record_counts,
        )
        contexto = ctx_quality.valid_df

    features = pd.DataFrame()
    if "alunos_features" in run_config.datasets or "alunos_analytic" in run_config.datasets:
        logger.info("--- Building alunos_features ---")
        features = build_alunos_features(alunos, contexto)
        if "alunos_features" in run_config.datasets:
            feat_quality = validate_alunos_features(features, municipio)
            _publish(
                name="alunos_features",
                quality=feat_quality,
                writer=writer,
                quarantine_writer=quarantine_writer,
                gold_configs=gold_configs,
                batch_id=batch_id,
                overwrite=run_config.overwrite,
                results=results,
                quarantine_counts=quarantine_counts,
                record_counts=record_counts,
            )
            features = feat_quality.valid_df

    if "alunos_analytic" in run_config.datasets:
        logger.info("--- Building alunos_analytic ---")
        analytic = build_alunos_analytic(features)
        # Analytic inherits territorial FK checks from the parent features table.
        analytic_quality = validate_alunos_features(analytic, municipio)
        _publish(
            name="alunos_analytic",
            quality=analytic_quality,
            writer=writer,
            quarantine_writer=quarantine_writer,
            gold_configs=gold_configs,
            batch_id=batch_id,
            overwrite=run_config.overwrite,
            results=results,
            quarantine_counts=quarantine_counts,
            record_counts=record_counts,
        )

    environment = os.getenv("ENVIRONMENT", "dev")
    publish_quality_metrics(
        batch_id=batch_id,
        environment=environment,
        layer="gold",
        quarantine_counts=quarantine_counts,
        record_counts=record_counts,
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
