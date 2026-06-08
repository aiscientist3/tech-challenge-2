"""Source: state-level literacy target and indicator."""

from __future__ import annotations

from ingestion.batch.sources.base_source import BaseSource


class MetaUfSource(BaseSource):
    """Extract per-state literacy targets and indicators."""

    def build_query(self) -> str:
        return self._compose_query(select_clause="*")
