"""Source: municipality territorial reference directory."""

from __future__ import annotations

from ingestion.batch.sources.base_source import BaseSource


class MunicipioSource(BaseSource):
    """Extract municipality territorial data from the Base dos Dados directory."""

    def build_query(self) -> str:
        return self._compose_query(select_clause="*")
