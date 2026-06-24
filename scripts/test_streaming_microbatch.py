"""
Simula um micro-batch de streaming (Bronze MERGE → Silver MERGE) sem Kafka/Spark.

Uso:
  export S3_BUCKET="tech-challenge-2-datalake-prod"
  export AWS_DEFAULT_REGION="us-east-1"
  python scripts/test_streaming_microbatch.py
"""

from __future__ import annotations

import os
import sys
import uuid
from datetime import datetime, timezone

import pandas as pd
from deltalake import DeltaTable

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from ingestion.batch.config import BRONZE_PREFIX
from ingestion.batch.connections.aws_credentials import (
    resolve_aws_storage_options,
    resolve_s3_bucket,
)
from ingestion.streaming.bronze_stream_writer import merge_upsert_to_bronze
from ingestion.streaming.config import (
    ALUNOS_BRONZE_MERGE_KEYS,
    ALUNOS_BRONZE_PARTITION_BY,
    alunos_silver_path,
    bronze_table_path,
)
from ingestion.streaming.silver_stream_writer import process_bronze_to_silver_microbatch

TEST_ID_ALUNO = f"STREAM_TEST_{uuid.uuid4().hex[:8]}"


def _sample_microbatch() -> pd.DataFrame:
    ts = datetime.now(timezone.utc).isoformat()
    return pd.DataFrame(
        [
            {
                "ano": 2024,
                "id_aluno": TEST_ID_ALUNO,
                "id_municipio": "3550308",
                "rede": "municipal",
                "alfabetizado": "Sim",
                "proficiencia": 850.0,
                "peso_aluno": 1.0,
                "_event_id": str(uuid.uuid4()),
                "_event_type": "performance_measurement",
                "_event_timestamp": ts,
                "_ingestion_timestamp": ts,
                "_ingestion_mode": "stream",
                "_stream_sink": "kafka",
            }
        ]
    )


def main() -> None:
    bucket = resolve_s3_bucket()
    storage_options = resolve_aws_storage_options()
    bronze_path = bronze_table_path(bucket, "alunos", BRONZE_PREFIX)
    silver_path = alunos_silver_path(bucket)
    pdf = _sample_microbatch()

    print(f"=== Streaming micro-batch test ===")
    print(f"Bucket      : {bucket}")
    print(f"Bronze path : {bronze_path}")
    print(f"Silver path : {silver_path}")
    print(f"Test aluno  : {TEST_ID_ALUNO}")

    bronze_rows = merge_upsert_to_bronze(
        pdf,
        bronze_path=bronze_path,
        storage_options=storage_options,
        merge_keys=ALUNOS_BRONZE_MERGE_KEYS,
        partition_by=ALUNOS_BRONZE_PARTITION_BY,
    )
    print(f"Bronze MERGE: {bronze_rows} row(s)")

    silver_rows = process_bronze_to_silver_microbatch(
        pdf,
        silver_path=silver_path,
        storage_options=storage_options,
    )
    print(f"Silver MERGE: {silver_rows} row(s)")

    silver_df = DeltaTable(silver_path, storage_options=storage_options).to_pandas()
    match = silver_df[silver_df["id_aluno"] == TEST_ID_ALUNO]
    if match.empty:
        raise SystemExit("FAIL: test aluno not found in Silver table.")

    row = match.iloc[0]
    assert row["_silver_ingestion_mode"] == "stream"
    assert row["_silver_stream_sink"] == "bronze"
    assert row["rede"] == "municipal"

    print("OK: Silver row found with streaming metadata:")
    print(f"  id_aluno={row['id_aluno']}, rede={row['rede']}, mode={row['_silver_ingestion_mode']}")
    print("=== TEST PASSED ===")


if __name__ == "__main__":
    main()
