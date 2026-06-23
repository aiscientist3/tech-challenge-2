"""
Gold layer transformations — Indicador Criança Alfabetizada and gap analysis.

Medallion: reads Silver tables only (alunos, meta_municipio, meta_uf, municipio).
"""

from __future__ import annotations

import logging

import pandas as pd

from ingestion.gold.config import META_GOAL_YEARS

logger = logging.getLogger(__name__)

META_GOAL_COLUMNS: tuple[str, ...] = tuple(
    f"meta_alfabetizacao_{year}" for year in META_GOAL_YEARS
)


def normalize_alfabetizado_flag(series: pd.Series) -> pd.Series:
    """Map alfabetizado values to 0/1 floats."""
    normalized = series.astype("string").str.strip().str.lower()
    return normalized.isin(["sim", "1", "true", "s"]).astype(float)


def _prepare_alunos_for_indicator(alunos: pd.DataFrame) -> pd.DataFrame:
    """Select valid student rows and compute helper columns for aggregation."""
    if alunos.empty:
        return alunos.copy()

    prepared = alunos.copy()
    prepared["_alfabetizado_flag"] = normalize_alfabetizado_flag(
        prepared.get("alfabetizado", pd.Series(dtype="string"))
    )
    prepared["_peso"] = pd.to_numeric(
        prepared.get("peso_aluno", pd.Series(dtype="float")),
        errors="coerce",
    ).fillna(1.0)
    prepared["_proficiencia"] = pd.to_numeric(
        prepared.get("proficiencia", pd.Series(dtype="float")),
        errors="coerce",
    )

    valid = prepared["_alfabetizado_flag"].notna()
    return prepared.loc[valid].copy()


def _aggregate_indicator(
    df: pd.DataFrame,
    group_cols: list[str],
) -> pd.DataFrame:
    """Compute weighted literacy indicator metrics for the given grouping."""
    if df.empty:
        return pd.DataFrame()

    grouped = (
        df.groupby(group_cols, dropna=False)
        .apply(
            lambda group: pd.Series(
                {
                    "total_alunos": len(group),
                    "total_peso": group["_peso"].sum(),
                    "total_alfabetizados_ponderado": (
                        group["_alfabetizado_flag"] * group["_peso"]
                    ).sum(),
                    "proficiencia_media_ponderada": (
                        (group["_proficiencia"] * group["_peso"]).sum()
                        / group["_peso"].sum()
                        if group["_peso"].sum() > 0
                        else float("nan")
                    ),
                }
            ),
            include_groups=False,
        )
        .reset_index()
    )

    grouped["taxa_crianca_alfabetizada"] = (
        grouped["total_alfabetizados_ponderado"] / grouped["total_peso"] * 100.0
    )
    return grouped


def add_gap_analysis(
    indicator: pd.DataFrame,
    meta: pd.DataFrame,
    join_keys: list[str],
    *,
    official_rate_col: str = "taxa_alfabetizacao",
) -> pd.DataFrame:
    """
    Join official INEP rates and goals, then compute gap columns.

    Adds:
    - ``gap_taxa_vs_inep`` = taxa calculada − taxa INEP
    - ``gap_meta_{year}`` for each meta column present
    """
    if indicator.empty:
        return indicator.copy()

    meta_cols = [
        col
        for col in [official_rate_col, *META_GOAL_COLUMNS, "nome_municipio", "nome_uf"]
        if col in meta.columns
    ]
    meta_subset = meta[join_keys + meta_cols].drop_duplicates(subset=join_keys)

    enriched = indicator.merge(meta_subset, on=join_keys, how="left")

    if official_rate_col in enriched.columns:
        enriched["gap_taxa_vs_inep"] = (
            enriched["taxa_crianca_alfabetizada"] - enriched[official_rate_col]
        )

    for year in META_GOAL_YEARS:
        meta_col = f"meta_alfabetizacao_{year}"
        gap_col = f"gap_meta_{year}"
        if meta_col in enriched.columns:
            enriched[gap_col] = (
                enriched["taxa_crianca_alfabetizada"] - enriched[meta_col]
            )

    return enriched


def build_indicador_municipio(
    alunos: pd.DataFrame,
    meta_municipio: pd.DataFrame,
) -> pd.DataFrame:
    """Build the municipal Criança Alfabetizada indicator with gap analysis."""
    prepared = _prepare_alunos_for_indicator(alunos)
    if prepared.empty:
        logger.warning("No valid alunos rows for municipal indicator.")
        return pd.DataFrame()

    indicator = _aggregate_indicator(
        prepared,
        group_cols=["ano", "id_municipio", "rede"],
    )

    indicator = add_gap_analysis(
        indicator,
        meta_municipio,
        join_keys=["ano", "id_municipio", "rede"],
    )

    if "nome_municipio" in indicator.columns and "sigla_uf" not in indicator.columns:
        if "sigla_uf" in meta_municipio.columns:
            lookup = meta_municipio[
                ["id_municipio", "sigla_uf"]
            ].drop_duplicates(subset=["id_municipio"])
            indicator = indicator.merge(lookup, on="id_municipio", how="left")

    logger.info(
        "Municipal indicator built: %d rows across %d years.",
        len(indicator),
        indicator["ano"].nunique() if "ano" in indicator.columns else 0,
    )
    return indicator


def build_indicador_uf(
    alunos: pd.DataFrame,
    municipio: pd.DataFrame,
    meta_uf: pd.DataFrame,
) -> pd.DataFrame:
    """Build the UF-level Criança Alfabetizada indicator with gap analysis."""
    prepared = _prepare_alunos_for_indicator(alunos)
    if prepared.empty:
        logger.warning("No valid alunos rows for UF indicator.")
        return pd.DataFrame()

    if "sigla_uf" not in prepared.columns:
        if municipio.empty or "sigla_uf" not in municipio.columns:
            raise ValueError(
                "Cannot build UF indicator: missing sigla_uf on alunos and municipio."
            )
        lookup = municipio[["id_municipio", "sigla_uf"]].drop_duplicates(
            subset=["id_municipio"]
        )
        prepared = prepared.merge(lookup, on="id_municipio", how="left")

    prepared = prepared[prepared["sigla_uf"].notna()].copy()
    if prepared.empty:
        logger.warning("No alunos rows with sigla_uf for UF indicator.")
        return pd.DataFrame()

    indicator = _aggregate_indicator(
        prepared,
        group_cols=["ano", "sigla_uf", "rede"],
    )

    indicator = add_gap_analysis(
        indicator,
        meta_uf,
        join_keys=["ano", "sigla_uf", "rede"],
    )

    logger.info(
        "UF indicator built: %d rows across %d years.",
        len(indicator),
        indicator["ano"].nunique() if "ano" in indicator.columns else 0,
    )
    return indicator
