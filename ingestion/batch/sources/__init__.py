"""Data sources for batch ingestion."""

from ingestion.batch.sources.alunos import AlunosSource
from ingestion.batch.sources.base_source import BaseSource
from ingestion.batch.sources.meta_brasil import MetaBrasilSource
from ingestion.batch.sources.meta_municipio import MetaMunicipioSource
from ingestion.batch.sources.meta_uf import MetaUfSource
from ingestion.batch.sources.municipio import MunicipioSource
from ingestion.batch.sources.uf import UfSource

SOURCE_REGISTRY: dict[str, type[BaseSource]] = {
    "uf": UfSource,
    "municipio": MunicipioSource,
    "meta_brasil": MetaBrasilSource,
    "meta_uf": MetaUfSource,
    "meta_municipio": MetaMunicipioSource,
    "alunos": AlunosSource,
}

__all__ = [
    "BaseSource",
    "SOURCE_REGISTRY",
    "UfSource",
    "MunicipioSource",
    "MetaBrasilSource",
    "MetaUfSource",
    "MetaMunicipioSource",
    "AlunosSource",
]
