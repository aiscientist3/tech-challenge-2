"""Source: municipality-level literacy target and indicator."""

from __future__ import annotations

from ingestion.batch.sources.base_source import BaseSource


class MetaMunicipioSource(BaseSource):
    """Extract per-municipality literacy targets and indicators."""

    def build_query(self) -> str:
        return self._compose_query(select_clause="*")
