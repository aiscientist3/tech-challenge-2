"""
Load batch AWS/Spark helpers without importing batch.connections.__init__.

Databricks streaming jobs do not install google-cloud-bigquery; importing
``ingestion.batch.connections`` eagerly loads bigquery_client and fails.
The producer runs locally with full deps and may import batch.connections directly.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType

_INGESTION_ROOT = Path(__file__).resolve().parent.parent


def _load_batch_connection_module(module_name: str) -> ModuleType:
    path = _INGESTION_ROOT / "batch" / "connections" / f"{module_name}.py"
    spec = importlib.util.spec_from_file_location(
        f"ingestion.batch.connections.{module_name}",
        path,
    )
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load batch connection module: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_aws = _load_batch_connection_module("aws_credentials")
resolve_s3_bucket = _aws.resolve_s3_bucket
resolve_aws_storage_options = _aws.resolve_aws_storage_options
resolve_kafka_config = _aws.resolve_kafka_config

_spark = _load_batch_connection_module("spark_s3")
get_or_create_spark_session = _spark.get_or_create_spark_session
