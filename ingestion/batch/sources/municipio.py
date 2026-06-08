"""Source: municipality territorial reference directory."""

from __future__ import annotations

from ingestion.batch.sources.base_source import BaseSource


class MunicipioSource(BaseSource):
    """Extract municipality territorial data from the Base dos Dados directory."""

    def build_query(self) -> str:
        return self._compose_query(
            select_clause=(
                "id_municipio,\n"
                "    id_municipio_6,\n"
                "    nome,\n"
                "    sigla_uf,\n"
                "    id_uf,\n"
                "    nome_regiao,\n"
                "    id_mesorregiao,\n"
                "    id_microrregiao,\n"
                "    id_regiao_metropolitana"
            )
        )
