"""
Batch ingestion entry point — Bronze layer.

Supports two invocation styles:
  1. CLI arguments (Databricks Jobs, local development, CI) — preferred
  2. Databricks Widgets (interactive notebooks only)

CLI usage:
  python -m ingestion.batch.main --sources all --years 2023,2024
  python -m ingestion.batch.main --sources uf,meta_brasil --years 2024 --row-limit 5000
  python -m ingestion.batch.main --sources meta_municipio --years 2024 --append
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
import uuid

from ingestion.common.dbutils import get_dbutils
from ingestion.batch.bronze_writer import BronzeWriter
from ingestion.batch.config import (
    ALL_SOURCE_NAMES,
    DEFAULT_YEARS,
    IngestionRunConfig,
    build_source_configs,
)
from ingestion.batch.connections.aws_credentials import (
    resolve_aws_storage_options,
    resolve_s3_bucket,
)
from ingestion.batch.connections.bigquery_client import create_bigquery_client
from ingestion.batch.metrics import publish_ingestion_metrics
from ingestion.batch.sources import SOURCE_REGISTRY

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Databricks helpers
# ---------------------------------------------------------------------------

def _cli_args_provided() -> bool:
    """Return True when explicit CLI flags were passed (Job or local invocation)."""
    argv = sys.argv[1:]
    if len(argv) == 1 and argv[0].startswith("--"):
        import shlex

        argv = shlex.split(argv[0])
    return any(arg.startswith("--") for arg in argv)


def _config_from_widgets() -> IngestionRunConfig | None:
    """Build IngestionRunConfig from Databricks notebook widgets when defined."""
    dbutils = get_dbutils()
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
        if "InputWidgetNotDefined" in type(exc).__name__ or "InputWidgetNotDefined" in str(exc):
            logger.debug("Databricks widgets not defined — using CLI/default config.")
        else:
            logger.warning("Could not read Databricks widgets: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def _parse_sources(raw: str) -> list[str]:
    if raw.strip().lower() == "all":
        return list(ALL_SOURCE_NAMES)

    sources = [s.strip() for s in raw.split(",") if s.strip()]
    invalid = [s for s in sources if s not in ALL_SOURCE_NAMES]
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
    """Resolve run configuration from CLI args (Job/local) or notebook widgets."""
    if args is not None or _cli_args_provided():
        resolved = args if args is not None else _build_arg_parser().parse_args()
        return IngestionRunConfig(
            years=_parse_years(resolved.years),
            sources=_parse_sources(resolved.sources),
            batch_id=resolved.batch_id,
            row_limit=resolved.row_limit,
            overwrite=not resolved.append,
        )

    widget_config = _config_from_widgets()
    if widget_config is not None:
        return widget_config

    resolved = _build_arg_parser().parse_args()
    return IngestionRunConfig(
        years=_parse_years(resolved.years),
        sources=_parse_sources(resolved.sources),
        batch_id=resolved.batch_id,
        row_limit=resolved.row_limit,
        overwrite=not resolved.append,
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
    storage_options = resolve_aws_storage_options()
    bucket = resolve_s3_bucket()
    source_configs = build_source_configs(bucket)
    writer = BronzeWriter(storage_options)

    logger.info("S3 bucket  : %s", bucket)

    results: dict[str, str | None] = {}
    record_counts: dict[str, int] = {}

    for source_name in run_config.sources:
        source_config = source_configs[source_name]
        source = SOURCE_REGISTRY[source_name](
            client=bq_client,
            run_config=run_config,
            source_config=source_configs[source_name],
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
        record_counts[source_name] = len(df)
        logger.info(
            "Source '%s' done. Records: %d. Destination: %s",
            source_name,
            len(df),
            destination or "N/A (empty)",
        )

    logger.info("=== BATCH INGESTION COMPLETED ===")
    return results, record_counts


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def _normalize_argv() -> None:
    """
    Databricks Serverless passes job parameters as a single string element
    in sys.argv, e.g. ["main.py", "--sources all --years 2023,2024"].
    Split it into proper tokens so argparse can handle them.
    """
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

    environment = os.getenv("ENVIRONMENT", "prod")
    started_at = time.monotonic()
    record_counts: dict[str, int] = {}

    try:
        _results, record_counts = run_ingestion(run_config)
    except Exception:
        publish_ingestion_metrics(
            batch_id=run_config.batch_id,
            environment=environment,
            record_counts=record_counts,
            duration_seconds=time.monotonic() - started_at,
            success=False,
        )
        raise

    publish_ingestion_metrics(
        batch_id=run_config.batch_id,
        environment=environment,
        record_counts=record_counts,
        duration_seconds=time.monotonic() - started_at,
        success=True,
    )


if __name__ == "__main__":
    main()
