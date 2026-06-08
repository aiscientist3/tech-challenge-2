"""
Bronze layer writer — persists raw DataFrames to S3 in Delta Lake format.

Responsibilities:
- Validate required columns before writing
- Attach ingestion metadata (_ingestion_timestamp, _source_table, _batch_id)
- Partition by year when applicable
- Write idempotently via overwrite mode
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ingestion.batch.config import SourceConfig

logger = logging.getLogger(__name__)

METADATA_COLUMNS = (
    "_ingestion_timestamp",
    "_source_table",
    "_batch_id",
)


class BronzeWriter:
    """Writes raw data to the Bronze layer in Delta Lake format on S3."""

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def validate(self, df: pd.DataFrame, source_config: SourceConfig) -> None:
        """
        Assert that all required columns are present.

        Skips validation for empty DataFrames (logged as a warning).

        Raises:
            ValueError: When required columns are missing.
        """
        if df.empty:
            logger.warning(
                "Empty DataFrame for source '%s'. Write will be skipped.",
                source_config.name,
            )
            return

        missing = [c for c in source_config.required_columns if c not in df.columns]
        if missing:
            raise ValueError(
                f"Source '{source_config.name}': missing required columns {missing}."
            )

    def write(
        self,
        df: pd.DataFrame,
        source_config: SourceConfig,
        batch_id: str,
        overwrite: bool = True,
    ) -> Optional[str]:
        """
        Write a pandas DataFrame to the Bronze layer.

        Args:
            df:            Raw data extracted from BigQuery.
            source_config: Metadata for the source (path, partition column, etc.).
            batch_id:      Unique identifier for this ingestion run.
            overwrite:     Use overwrite mode when True, append otherwise.

        Returns:
            S3 destination path, or None if the DataFrame was empty.
        """
        self.validate(df, source_config)
        if df.empty:
            return None

        destination = source_config.bronze_path
        ingestion_ts = datetime.now(timezone.utc).isoformat()

        spark_df: DataFrame = self.spark.createDataFrame(df)
        spark_df = (
            spark_df
            .withColumn("_ingestion_timestamp", F.lit(ingestion_ts))
            .withColumn("_source_table", F.lit(source_config.bq_table))
            .withColumn("_batch_id", F.lit(batch_id))
        )

        write_mode = "overwrite" if overwrite else "append"
        writer = (
            spark_df.write
            .format("delta")
            .mode(write_mode)
            .option("overwriteSchema", "true")
        )

        partition_col = source_config.partition_by
        if partition_col and partition_col in spark_df.columns:
            writer = writer.partitionBy(partition_col)
            logger.info(
                "Writing '%s' → %s  (partitionBy=%s, mode=%s)",
                source_config.name,
                destination,
                partition_col,
                write_mode,
            )
        else:
            logger.info(
                "Writing '%s' → %s  (no partition, mode=%s)",
                source_config.name,
                destination,
                write_mode,
            )

        writer.save(destination)

        record_count = spark_df.count()
        logger.info(
            "Bronze write complete: '%s' — %d records at %s.",
            source_config.name,
            record_count,
            destination,
        )
        return destination
