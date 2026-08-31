"""Source: IBGE municipal population estimates."""

from __future__ import annotations

from ingestion.batch.sources.base_source import BaseSource


class PopulacaoMunicipioSource(BaseSource):
    """Extract municipal population from Base dos Dados (IBGE)."""

    def build_query(self) -> str:
        return self._compose_query(
            select_clause="ano, id_municipio, populacao",
        )
