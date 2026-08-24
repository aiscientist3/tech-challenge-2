"""Data sources for batch ingestion."""

from ingestion.batch.sources.base_source import BaseSource
from ingestion.batch.sources.meta_brasil import MetaBrasilSource
from ingestion.batch.sources.meta_municipio import MetaMunicipioSource
from ingestion.batch.sources.meta_uf import MetaUfSource
from ingestion.batch.sources.municipio import MunicipioSource
from ingestion.batch.sources.municipio_indicadores import MunicipioIndicadoresSource
from ingestion.batch.sources.pib_municipio import PibMunicipioSource
from ingestion.batch.sources.populacao_municipio import PopulacaoMunicipioSource
from ingestion.batch.sources.socioeconomico_municipio import SocioeconomicoMunicipioSource
from ingestion.batch.sources.uf import UfSource
from ingestion.batch.sources.uf_indicadores import UfIndicadoresSource

SOURCE_REGISTRY: dict[str, type[BaseSource]] = {
    "uf": UfSource,
    "municipio": MunicipioSource,
    "meta_brasil": MetaBrasilSource,
    "meta_uf": MetaUfSource,
    "meta_municipio": MetaMunicipioSource,
    "populacao_municipio": PopulacaoMunicipioSource,
    "pib_municipio": PibMunicipioSource,
    "socioeconomico_municipio": SocioeconomicoMunicipioSource,
    "municipio_indicadores": MunicipioIndicadoresSource,
    "uf_indicadores": UfIndicadoresSource,
}

__all__ = [
    "BaseSource",
    "SOURCE_REGISTRY",
    "UfSource",
    "MunicipioSource",
    "MetaBrasilSource",
    "MetaUfSource",
    "MetaMunicipioSource",
    "PopulacaoMunicipioSource",
    "PibMunicipioSource",
    "SocioeconomicoMunicipioSource",
    "MunicipioIndicadoresSource",
    "UfIndicadoresSource",
]
