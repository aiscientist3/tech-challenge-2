"""Source: IPEA Atlas da Vulnerabilidade Social (municipal)."""

from __future__ import annotations

from ingestion.batch.sources.base_source import BaseSource


class SocioeconomicoMunicipioSource(BaseSource):
    """Extract municipal social vulnerability indicators (IPEA AVS)."""

    def build_query(self) -> str:
        return self._compose_query(
            select_clause=(
                "ano, id_municipio, ivs, "
                "ivs_infraestrutura_urbana, ivs_capital_humano, ivs_renda_e_trabalho"
            ),
        )
