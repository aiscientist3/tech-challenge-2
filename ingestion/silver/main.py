"""
Silver layer entry point — Bronze → standardise → deduplicate → join → Silver.

CLI usage:
  python -m ingestion.silver.main --entities all --years 2023,2024
  python -m ingestion.silver.main --entities meta_uf,meta_municipio --years 2024
  python -m ingestion.silver.main --entities uf --append
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
from ingestion.silver.bronze_reader import read_bronze
from ingestion.silver.config import (
    ALL_ENTITY_NAMES,
    DEFAULT_YEARS,
    SilverRunConfig,
    build_entity_configs,
)
from ingestion.silver.silver_writer import SilverWriter
from ingestion.silver.transforms import (
    apply_enrichment,
    deduplicate,
    standardize_common,
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


def _config_from_widgets() -> SilverRunConfig | None:
    dbutils = _get_dbutils()
    if dbutils is None:
        return None

    try:
        entities_raw = dbutils.widgets.get("entities")
        years_raw = dbutils.widgets.get("years")
        batch_id = dbutils.widgets.get("batch_id") or None
        overwrite_raw = (dbutils.widgets.get("overwrite") or "true").strip().lower()

        return SilverRunConfig(
            years=_parse_years(years_raw),
            entities=_parse_entities(entities_raw),
            batch_id=batch_id,
            overwrite=overwrite_raw != "false",
        )
    except Exception as exc:
        if "InputWidgetNotDefined" in type(exc).__name__ or "InputWidgetNotDefined" in str(exc):
            logger.debug("Databricks widgets not defined — using CLI/default config.")
        else:
            logger.warning("Could not read Databricks widgets: %s", exc)
        return None


def _parse_entities(raw: str) -> list[str]:
    if raw.strip().lower() == "all":
        return list(ALL_ENTITY_NAMES)

    entities = [item.strip() for item in raw.split(",") if item.strip()]
    invalid = [name for name in entities if name not in ALL_ENTITY_NAMES]
    if invalid:
        raise ValueError(
            f"Unknown entity(ies): {invalid}. Valid options: {list(ALL_ENTITY_NAMES)}"
        )
    return entities


def _parse_years(raw: str) -> list[int]:
    years = [int(year.strip()) for year in raw.split(",") if year.strip()]
    if not years:
        raise ValueError("At least one valid year is required.")
    return years


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Silver layer processing — Bronze Delta → treated Silver Delta on S3."
    )
    parser.add_argument(
        "--entities",
        default="all",
        help="Comma-separated entity names or 'all'. Default: all.",
    )
    parser.add_argument(
        "--years",
        default=",".join(str(year) for year in DEFAULT_YEARS),
        help="Comma-separated years to process. Default: %(default)s.",
    )
    parser.add_argument(
        "--batch-id",
        default=None,
        help="Silver batch identifier (auto-generated UUID if omitted).",
    )
    parser.add_argument(
        "--append",
        action="store_true",
        help="Use append mode instead of overwrite.",
    )
    return parser


def build_run_config(args: argparse.Namespace | None = None) -> SilverRunConfig:
    if args is not None or _cli_args_provided():
        resolved = args if args is not None else _build_arg_parser().parse_args()
        return SilverRunConfig(
            years=_parse_years(resolved.years),
            entities=_parse_entities(resolved.entities),
            batch_id=resolved.batch_id,
            overwrite=not resolved.append,
        )

    widget_config = _config_from_widgets()
    if widget_config is not None:
        return widget_config

    resolved = _build_arg_parser().parse_args()
    return SilverRunConfig(
        years=_parse_years(resolved.years),
        entities=_parse_entities(resolved.entities),
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


def _expand_entities(requested: list[str]) -> list[str]:
    """
    Ensure reference entities are processed before entities that depend on them.
    """
    requested_set = set(requested)
    entity_configs = build_entity_configs("placeholder")

    for entity_name in requested:
        enrichment_ref = entity_configs[entity_name].enrichment_ref
        if enrichment_ref:
            requested_set.add(enrichment_ref)

    return [name for name in ALL_ENTITY_NAMES if name in requested_set]


def _transform_entity(
    entity_name: str,
    df: pd.DataFrame,
    references: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    entity_config = build_entity_configs("placeholder")[entity_name]

    treated = standardize_common(df)
    treated = deduplicate(treated, entity_config.natural_key, entity_name=entity_name)
    treated = apply_enrichment(entity_name, treated, references)
    return treated


def run_silver(run_config: SilverRunConfig) -> tuple[dict[str, str | None], dict[str, int]]:
    batch_id = run_config.batch_id or str(uuid.uuid4())
    logger.info("=== SILVER PROCESSING STARTED ===")
    logger.info("Batch ID : %s", batch_id)
    logger.info("Entities : %s", run_config.entities)
    logger.info("Years    : %s", run_config.years)

    storage_options = resolve_aws_storage_options()
    bucket = _resolve_bucket()
    entity_configs = build_entity_configs(bucket)
    writer = SilverWriter(storage_options)

    logger.info("S3 bucket: %s", bucket)

    processing_order = _expand_entities(run_config.entities)
    references: dict[str, pd.DataFrame] = {}
    results: dict[str, str | None] = {}
    record_counts: dict[str, int] = {}

    for entity_name in processing_order:
        entity_config = entity_configs[entity_name]
        logger.info("--- Processing entity: %s ---", entity_name)

        years = run_config.years if entity_config.filter_by_year else None
        partition_col = entity_config.partition_by if entity_config.filter_by_year else None

        bronze_df = read_bronze(
            path=entity_config.bronze_path,
            storage_options=storage_options,
            years=years,
            partition_col=partition_col,
        )

        silver_df = _transform_entity(entity_name, bronze_df, references)

        if entity_name in ("uf", "municipio"):
            references[entity_name] = silver_df

        destination = writer.write(
            df=silver_df,
            entity_config=entity_config,
            batch_id=batch_id,
            overwrite=run_config.overwrite,
        )

        results[entity_name] = destination
        record_counts[entity_name] = len(silver_df)
        logger.info(
            "Entity '%s' done. Records: %d. Destination: %s",
            entity_name,
            len(silver_df),
            destination or "N/A (empty)",
        )

    logger.info("=== SILVER PROCESSING COMPLETED ===")
    return results, record_counts


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

    run_silver(run_config)


if __name__ == "__main__":
    main()
