"""
Batch ingestion entry point — Bronze layer.

Supports two invocation styles:
  1. Databricks Widgets (when running inside a Databricks Job)
  2. CLI arguments (local development or CI)

CLI usage:
  python -m ingestion.batch.main --sources all --years 2023,2024
  python -m ingestion.batch.main --sources uf,meta_brasil --years 2024 --row-limit 5000
  python -m ingestion.batch.main --sources alunos --years 2024 --append
"""

from __future__ import annotations

import argparse
import logging
import sys
import uuid
from typing import Any

from ingestion.batch.bronze_writer import BronzeWriter
from ingestion.batch.config import (
    ALL_SOURCE_NAMES,
    DEFAULT_YEARS,
    SOURCE_CONFIGS,
    IngestionRunConfig,
)
from ingestion.batch.connections.bigquery_client import create_bigquery_client
from ingestion.batch.connections.spark_s3 import get_or_create_spark_session
from ingestion.batch.sources import SOURCE_REGISTRY

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Databricks helpers
# ---------------------------------------------------------------------------

def _get_dbutils() -> Any | None:
    """Return Databricks dbutils when running inside a cluster."""
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


def _config_from_widgets() -> IngestionRunConfig | None:
    """Build IngestionRunConfig from Databricks widgets when available."""
    dbutils = _get_dbutils()
    if dbutils is None:
        return None

    try:
        sources_raw = dbutils.widgets.get("sources")
        years_raw = dbutils.widgets.get("years")
        batch_id = dbutils.widgets.get("batch_id") or None
        row_limit_raw = (dbutils.widgets.get("row_limit") or "").strip()
        overwrite_raw = (dbutils.widgets.get("overwrite") or "true").strip().lower()

        return IngestionRunConfig(
            years=_parse_years(years_raw),
            sources=_parse_sources(sources_raw),
            batch_id=batch_id,
            row_limit=int(row_limit_raw) if row_limit_raw else None,
            overwrite=overwrite_raw != "false",
        )
    except Exception as exc:
        logger.warning("Could not read Databricks widgets: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def _parse_sources(raw: str) -> list[str]:
    if raw.strip().lower() == "all":
        return list(ALL_SOURCE_NAMES)

    sources = [s.strip() for s in raw.split(",") if s.strip()]
    invalid = [s for s in sources if s not in SOURCE_CONFIGS]
    if invalid:
        raise ValueError(
            f"Unknown source(s): {invalid}. Valid options: {list(ALL_SOURCE_NAMES)}"
        )
    return sources


def _parse_years(raw: str) -> list[int]:
    years = [int(y.strip()) for y in raw.split(",") if y.strip()]
    if not years:
        raise ValueError("At least one valid year is required.")
    return years


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Batch ingestion — Bronze layer (BigQuery → S3 Delta Lake)."
    )
    parser.add_argument(
        "--sources",
        default="all",
        help="Comma-separated source names or 'all'. Default: all.",
    )
    parser.add_argument(
        "--years",
        default=",".join(str(y) for y in DEFAULT_YEARS),
        help="Comma-separated years to ingest. Default: %(default)s.",
    )
    parser.add_argument(
        "--batch-id",
        default=None,
        help="Ingestion batch identifier (auto-generated UUID if omitted).",
    )
    parser.add_argument(
        "--row-limit",
        type=int,
        default=None,
        help="Max rows per source — reduces BigQuery cost in development.",
    )
    parser.add_argument(
        "--append",
        action="store_true",
        help="Use append mode instead of overwrite.",
    )
    return parser


def build_run_config(args: argparse.Namespace | None = None) -> IngestionRunConfig:
    """Resolve run configuration from widgets (Databricks) or CLI args."""
    widget_config = _config_from_widgets()
    if widget_config is not None:
        return widget_config

    if args is None:
        args = _build_arg_parser().parse_args()

    return IngestionRunConfig(
        years=_parse_years(args.years),
        sources=_parse_sources(args.sources),
        batch_id=args.batch_id,
        row_limit=args.row_limit,
        overwrite=not args.append,
    )


# ---------------------------------------------------------------------------
# Ingestion orchestration
# ---------------------------------------------------------------------------

def run_ingestion(run_config: IngestionRunConfig) -> dict[str, str | None]:
    """
    Run batch ingestion for all configured sources.

    Args:
        run_config: Runtime parameters (sources, years, batch_id, etc.).

    Returns:
        Mapping of {source_name: s3_destination_path}.
    """
    batch_id = run_config.batch_id or str(uuid.uuid4())
    logger.info("=== BATCH INGESTION STARTED (BRONZE) ===")
    logger.info("Batch ID : %s", batch_id)
    logger.info("Sources  : %s", run_config.sources)
    logger.info("Years    : %s", run_config.years)
    logger.info("Row limit: %s", run_config.row_limit or "none")

    bq_client = create_bigquery_client()
    spark = get_or_create_spark_session()
    writer = BronzeWriter(spark)

    results: dict[str, str | None] = {}

    for source_name in run_config.sources:
        source_config = SOURCE_CONFIGS[source_name]
        source = SOURCE_REGISTRY[source_name](
            client=bq_client,
            run_config=run_config,
            source_config=source_config,
        )

        logger.info("--- Processing source: %s ---", source_name)
        df = source.extract()
        destination = writer.write(
            df=df,
            source_config=source_config,
            batch_id=batch_id,
            overwrite=run_config.overwrite,
        )
        results[source_name] = destination
        logger.info(
            "Source '%s' done. Records: %d. Destination: %s",
            source_name,
            len(df),
            destination or "N/A (empty)",
        )

    logger.info("=== BATCH INGESTION COMPLETED ===")
    return results


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> int:
    parser = _build_arg_parser()
    args = parser.parse_args()

    try:
        run_config = build_run_config(args)
        run_ingestion(run_config)
        return 0
    except Exception as exc:
        logger.exception("Batch ingestion failed: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
