"""
Abstract base class for all Bronze ingestion sources.

Defines the extraction contract, standardised logging, and retry logic.
Every concrete source must implement `build_query()`.
"""

from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod
from typing import Optional

import pandas as pd
from google.cloud import bigquery

from ingestion.batch.config import (
    DEFAULT_RETRY_ATTEMPTS,
    DEFAULT_RETRY_DELAY_SECONDS,
    IngestionRunConfig,
    SourceConfig,
)

logger = logging.getLogger(__name__)


class BaseSource(ABC):
    """Contract for extracting data from BigQuery into a pandas DataFrame."""

    def __init__(
        self,
        client: bigquery.Client,
        run_config: IngestionRunConfig,
        source_config: SourceConfig,
    ) -> None:
        self.client = client
        self.run_config = run_config
        self.source_config = source_config
        self.source_name = source_config.name
        self.logger = logging.getLogger(f"{__name__}.{self.source_name}")

    @abstractmethod
    def build_query(self) -> str:
        """Build the SQL extraction query for this source."""

    def extract(self) -> pd.DataFrame:
        """
        Execute the query with simple exponential retry.

        Returns:
            Raw pandas DataFrame from BigQuery.

        Raises:
            RuntimeError: After exhausting all retry attempts.
        """
        query = self.build_query()
        self.logger.info("Starting extraction for '%s'.", self.source_name)
        self.logger.debug("Query:\n%s", query)

        last_error: Exception | None = None
        for attempt in range(1, DEFAULT_RETRY_ATTEMPTS + 1):
            try:
                query_job = self.client.query(query)
                # create_bqstorage_client=False disables the BigQuery Storage
                # Read API, avoiding the need for bigquery.readsessions.create
                # permission. Sufficient for row-limited development queries.
                df = query_job.to_dataframe(create_bqstorage_client=False)
                self.logger.info(
                    "Extraction complete: '%s' — %d records.",
                    self.source_name,
                    len(df),
                )
                return df
            except Exception as exc:
                last_error = exc
                self.logger.warning(
                    "Attempt %d/%d failed for '%s': %s",
                    attempt,
                    DEFAULT_RETRY_ATTEMPTS,
                    self.source_name,
                    exc,
                )
                if attempt < DEFAULT_RETRY_ATTEMPTS:
                    delay = DEFAULT_RETRY_DELAY_SECONDS * attempt
                    self.logger.info("Retrying in %.1fs...", delay)
                    time.sleep(delay)

        raise RuntimeError(
            f"Failed to extract source '{self.source_name}' after "
            f"{DEFAULT_RETRY_ATTEMPTS} attempts."
        ) from last_error

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------

    def _year_filter_clause(self) -> str:
        """Build a WHERE clause to filter by the configured years."""
        years = ", ".join(str(y) for y in self.run_config.years)
        return f"ano IN ({years})"

    def _limit_clause(self) -> str:
        """Build an optional LIMIT clause (dev cost guard)."""
        limit = self.run_config.row_limit
        if limit is not None and limit > 0:
            return f"LIMIT {limit}"
        return ""

    def _compose_query(
        self,
        select_clause: str = "*",
        extra_where: Optional[str] = None,
    ) -> str:
        """
        Assemble a standard SELECT query for this source's BigQuery table.

        Args:
            select_clause: Column list or expressions for SELECT.
            extra_where:   Additional filter condition (without WHERE keyword).
        """
        table = self.source_config.bq_table
        conditions: list[str] = []

        if self.source_config.filter_by_year:
            conditions.append(self._year_filter_clause())

        if extra_where:
            conditions.append(f"({extra_where})")

        where_sql = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        limit_sql = self._limit_clause()

        return (
            f"SELECT\n    {select_clause}\nFROM `{table}`"
            + (f"\n{where_sql}" if where_sql else "")
            + (f"\n{limit_sql}" if limit_sql else "")
        )
