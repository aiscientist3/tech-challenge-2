"""Source: state (UF) reference directory."""

from __future__ import annotations

from ingestion.batch.sources.base_source import BaseSource


class UfSource(BaseSource):
    """Extract state reference data from the Base dos Dados directory."""

    def build_query(self) -> str:
        return self._compose_query(select_clause="*")
