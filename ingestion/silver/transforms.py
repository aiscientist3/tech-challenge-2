"""
Silver layer transformations — standardisation, deduplication and territorial joins.
"""

from __future__ import annotations

import logging

import pandas as pd

logger = logging.getLogger(__name__)

_MISSING_STRINGS = {"", "nan", "none", "null", "<na>"}


def _normalise_string_series(series: pd.Series) -> pd.Series:
    """Trim whitespace and normalise common null-like string values."""
    as_str = series.astype("string")
    stripped = as_str.str.strip()
    lowered = stripped.str.lower()
    return stripped.mask(lowered.isin(_MISSING_STRINGS))


def standardize_common(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply cross-entity standardisation rules.

    - ``ano`` → nullable integer
    - ``id_municipio`` → 7-digit zero-padded string
    - ``sigla`` / ``sigla_uf`` → uppercase trimmed string
    - ``rede`` → lowercase trimmed string
    """
    if df.empty:
        return df.copy()

    result = df.copy()

    if "ano" in result.columns:
        result["ano"] = pd.to_numeric(result["ano"], errors="coerce").astype("Int64")

    if "id_municipio" in result.columns:
        raw = result["id_municipio"].astype("string")
        cleaned = raw.str.replace(r"\.0$", "", regex=True)
        digits = cleaned.str.replace(r"\D", "", regex=True)
        result["id_municipio"] = digits.str.zfill(7)
        result.loc[digits.isna() | (digits == ""), "id_municipio"] = pd.NA

    for column in ("sigla", "sigla_uf"):
        if column in result.columns:
            normalised = _normalise_string_series(result[column])
            result[column] = normalised.str.upper()

    if "rede" in result.columns:
        normalised = _normalise_string_series(result["rede"])
        result["rede"] = normalised.str.lower()

    return result


def deduplicate(
    df: pd.DataFrame,
    keys: tuple[str, ...],
    entity_name: str = "entity",
) -> pd.DataFrame:
    """
    Remove duplicate rows keeping the most recent Bronze ingestion record.

    Args:
        df:          Input DataFrame.
        keys:        Natural business key columns.
        entity_name: Used for logging only.
    """
    if df.empty:
        return df.copy()

    missing_keys = [key for key in keys if key not in df.columns]
    if missing_keys:
        raise ValueError(
            f"Entity '{entity_name}': cannot deduplicate — missing key columns {missing_keys}."
        )

    result = df.copy()
    sort_col = "_ingestion_timestamp" if "_ingestion_timestamp" in result.columns else None

    if sort_col:
        result = result.sort_values(sort_col, ascending=False, na_position="last")

    before = len(result)
    result = result.drop_duplicates(subset=list(keys), keep="first")
    removed = before - len(result)

    if removed > 0:
        logger.info(
            "Deduplicated '%s' on %s — removed %d of %d rows.",
            entity_name,
            keys,
            removed,
            before,
        )

    return result.reset_index(drop=True)


def enrich_meta_uf(meta_uf: pd.DataFrame, uf: pd.DataFrame) -> pd.DataFrame:
    """Left-join meta_uf with the UF reference table on sigla."""
    if meta_uf.empty:
        return meta_uf.copy()

    required_uf_cols = ("sigla", "nome", "regiao", "id_uf")
    missing = [col for col in required_uf_cols if col not in uf.columns]
    if missing:
        raise ValueError(f"UF reference table missing columns: {missing}")

    uf_lookup = (
        uf[list(required_uf_cols)]
        .rename(
            columns={
                "nome": "nome_uf",
                "regiao": "regiao_uf",
            }
        )
        .drop_duplicates(subset=["sigla"], keep="first")
    )

    enriched = meta_uf.merge(
        uf_lookup,
        left_on="sigla_uf",
        right_on="sigla",
        how="left",
    )

    if "sigla" in enriched.columns and "sigla_uf" in enriched.columns:
        enriched = enriched.drop(columns=["sigla"])

    enriched["_join_match"] = enriched["nome_uf"].notna()
    unmatched = (~enriched["_join_match"]).sum()
    if unmatched:
        logger.warning(
            "meta_uf enrichment: %d rows without UF match (of %d).",
            unmatched,
            len(enriched),
        )

    return enriched


def enrich_meta_municipio(
    meta_municipio: pd.DataFrame,
    municipio: pd.DataFrame,
) -> pd.DataFrame:
    """Left-join meta_municipio with the municipality reference table."""
    if meta_municipio.empty:
        return meta_municipio.copy()

    municipio_cols = (
        "id_municipio",
        "nome",
        "sigla_uf",
        "nome_uf",
        "nome_regiao",
        "capital_uf",
    )
    available_cols = [col for col in municipio_cols if col in municipio.columns]
    if "id_municipio" not in available_cols:
        raise ValueError("Municipality reference table missing column: id_municipio")

    municipio_lookup = (
        municipio[available_cols]
        .rename(columns={"nome": "nome_municipio", "nome_regiao": "regiao_municipio"})
        .drop_duplicates(subset=["id_municipio"], keep="first")
    )

    enriched = meta_municipio.merge(
        municipio_lookup,
        on="id_municipio",
        how="left",
    )

    enriched["_join_match"] = enriched["nome_municipio"].notna()
    unmatched = (~enriched["_join_match"]).sum()
    if unmatched:
        logger.warning(
            "meta_municipio enrichment: %d rows without municipality match (of %d).",
            unmatched,
            len(enriched),
        )

    return enriched


def apply_enrichment(
    entity_name: str,
    df: pd.DataFrame,
    references: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """Apply territorial enrichment when the entity requires a reference join."""
    if entity_name == "meta_uf":
        return enrich_meta_uf(df, references["uf"])
    if entity_name == "meta_municipio":
        return enrich_meta_municipio(df, references["municipio"])
    return df
