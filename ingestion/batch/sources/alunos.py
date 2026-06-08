"""Source: student microdata — Criança Alfabetizada indicator."""

from __future__ import annotations

from ingestion.batch.sources.base_source import BaseSource


class AlunosSource(BaseSource):
    """
    Extract student-level microdata from the literacy assessment.

    For large production loads, consider splitting by sigla_uf to reduce
    BigQuery on-demand query costs.
    """

    def build_query(self) -> str:
        return self._compose_query(select_clause="*")
