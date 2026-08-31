"""Source: IBGE municipal GDP."""

from __future__ import annotations

from ingestion.batch.sources.base_source import BaseSource


class PibMunicipioSource(BaseSource):
    """Extract municipal GDP from Base dos Dados (IBGE)."""

    def build_query(self) -> str:
        return self._compose_query(
            select_clause="ano, id_municipio, pib",
        )
