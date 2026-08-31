"""
Shared Delta → pandas reader for Gold/Silver batch jobs.

Databricks Serverless notes:
- Spark cannot inject s3a credentials (Spark Connect blocks spark.hadoop.fs.s3a.*).
- deltalake ``to_pandas`` can fail on hive-partitioned tables (pyarrow ``field()`` bug).
- Fallback: list active files via Delta Rust API (``file_uris``) and read parquet with boto3
  using the same ``storage_options`` credentials that already work for writes.
"""

from __future__ import annotations

import io
import logging
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import pandas as pd
from deltalake import DeltaTable

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


def _parse_s3_uri(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc:
        raise ValueError(f"Invalid S3 URI: {uri}")
    key = parsed.path.lstrip("/")
    return parsed.netloc, key


def _build_s3_client(storage_options: dict[str, str]):
    import boto3

    return boto3.client(
        "s3",
        aws_access_key_id=storage_options.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=storage_options.get("AWS_SECRET_ACCESS_KEY"),
        region_name=storage_options.get("AWS_REGION", "us-east-1"),
    )


def _read_parquet_objects(
    s3_client,
    bucket: str,
    keys: list[str],
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for key in keys:
        if not key.endswith(".parquet") or "_delta_log" in key:
            continue
        body = s3_client.get_object(Bucket=bucket, Key=key)["Body"].read()
        frames.append(pd.read_parquet(io.BytesIO(body)))
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _uris_to_keys(uris: list[str]) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    for uri in uris:
        bucket, key = _parse_s3_uri(uri)
        pairs.append((bucket, key))
    return pairs


def _read_via_file_uris(
    table: DeltaTable,
    storage_options: dict[str, str],
    *,
    years: list[int] | None,
    partition_col: str | None,
) -> pd.DataFrame:
    """Read active Delta files directly — bypasses pyarrow dataset partition parsing."""
    s3_client = _build_s3_client(storage_options)
    frames: list[pd.DataFrame] = []

    if years and partition_col:
        for year in years:
            uris = table.file_uris(
                partition_filters=[(partition_col, "=", str(year))]
            )
            if not uris:
                continue
            bucket, _ = _parse_s3_uri(uris[0])
            keys = [key for _, key in _uris_to_keys(uris)]
            part = _read_parquet_objects(s3_client, bucket, keys)
            if partition_col not in part.columns:
                part = part.copy()
                part[partition_col] = year
            if not part.empty:
                frames.append(part)
    else:
        uris = table.file_uris()
        if not uris:
            return pd.DataFrame()
        bucket, _ = _parse_s3_uri(uris[0])
        keys = [key for _, key in _uris_to_keys(uris)]
        return _read_parquet_objects(s3_client, bucket, keys)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _read_via_deltalake(
    table: DeltaTable,
    *,
    years: list[int] | None,
    partition_col: str | None,
) -> pd.DataFrame:
    if years and partition_col:
        return table.to_pandas(filters=[(partition_col, "in", years)])
    return table.to_pandas()


def read_delta_to_pandas(
    path: str,
    storage_options: dict[str, str],
    *,
    years: list[int] | None = None,
    partition_col: str | None = "ano",
) -> pd.DataFrame:
    """
    Read a Delta table from S3 into pandas.

    Raises:
        RuntimeError: When the table cannot be read by any backend.
    """
    table = DeltaTable(path, storage_options=storage_options)
    errors: list[str] = []

    try:
        return _read_via_deltalake(table, years=years, partition_col=partition_col)
    except Exception as exc:
        errors.append(f"deltalake: {exc}")
        logger.warning(
            "deltalake read failed for %s (%s); trying file_uris + boto3.",
            path,
            exc,
        )

    try:
        return _read_via_file_uris(
            table,
            storage_options,
            years=years,
            partition_col=partition_col,
        )
    except Exception as exc:
        errors.append(f"file_uris+boto3: {exc}")

    raise RuntimeError(
        f"Could not read Delta table at {path}. Attempts: {'; '.join(errors)}"
    )
