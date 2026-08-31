"""Source: INEP literacy assessment indicators at UF grain."""

from __future__ import annotations

from ingestion.batch.sources.base_source import BaseSource


class UfIndicadoresSource(BaseSource):
    """Extract complementary INEP literacy indicators per UF."""

    def build_query(self) -> str:
        return self._compose_query(
            select_clause=(
                "ano, sigla_uf, serie, rede, taxa_alfabetizacao, media_portugues, "
                "proporcao_aluno_nivel_0, proporcao_aluno_nivel_1, proporcao_aluno_nivel_2, "
                "proporcao_aluno_nivel_3, proporcao_aluno_nivel_4, proporcao_aluno_nivel_5, "
                "proporcao_aluno_nivel_6, proporcao_aluno_nivel_7, proporcao_aluno_nivel_8"
            ),
        )
