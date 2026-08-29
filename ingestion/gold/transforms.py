"""
Gold layer transformations — indicators, territorial context and ML feature tables.

Medallion: reads Silver tables only.
"""

from __future__ import annotations

import logging

import pandas as pd

from ingestion.gold.config import META_GOAL_YEARS
from ingestion.silver.transforms import standardize_common

logger = logging.getLogger(__name__)

META_GOAL_COLUMNS: tuple[str, ...] = tuple(
    f"meta_alfabetizacao_{year}" for year in META_GOAL_YEARS
)

JOIN_KEYS_MUNICIPIO: list[str] = ["ano", "id_municipio", "rede"]

_LEAKAGE_COLS: frozenset[str] = frozenset(
    {
        "nivel_alfabetizacao",
        "proficiencia",
        "caderno",
        "presenca",
        "preenchimento_caderno",
    }
)

_MUNICIPIO_CONTEXT_COLS: tuple[str, ...] = (
    "id_municipio",
    "nome",
    "sigla_uf",
    "nome_uf",
    "nome_regiao",
    "regiao_municipio",
    "capital_uf",
    "nome_mesorregiao",
    "nome_microrregiao",
    "amazonia_legal",
)


def _standardize_keys(df: pd.DataFrame) -> pd.DataFrame:
    """Apply shared key standardisation (id_municipio, rede, ano, siglas)."""
    if df.empty:
        return df.copy()
    return standardize_common(df)


def normalize_alfabetizado_flag(series: pd.Series) -> pd.Series:
    """Map alfabetizado values to 0/1 floats."""
    numeric = pd.to_numeric(series, errors="coerce")
    normalized = series.astype("string").str.strip().str.lower()
    from_label = normalized.isin(["sim", "1", "1.0", "true", "s"])
    return ((numeric == 1) | from_label).astype(float)


def _prepare_alunos_for_indicator(alunos: pd.DataFrame) -> pd.DataFrame:
    """Select valid student rows and compute helper columns for aggregation."""
    if alunos.empty:
        return alunos.copy()

    prepared = _standardize_keys(alunos)
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

    meta_std = _standardize_keys(meta)
    meta_cols = [
        col
        for col in [official_rate_col, *META_GOAL_COLUMNS, "nome_municipio", "nome_uf"]
        if col in meta_std.columns
    ]
    meta_subset = meta_std[join_keys + meta_cols].drop_duplicates(subset=join_keys)

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
        meta_std = _standardize_keys(meta_municipio)
        if "sigla_uf" in meta_std.columns:
            lookup = meta_std[
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
        municipio_std = _standardize_keys(municipio)
        if municipio_std.empty or "sigla_uf" not in municipio_std.columns:
            raise ValueError(
                "Cannot build UF indicator: missing sigla_uf on alunos and municipio."
            )
        lookup = municipio_std[["id_municipio", "sigla_uf"]].drop_duplicates(
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


def _municipio_lookup(municipio: pd.DataFrame) -> pd.DataFrame:
    """Territorial attributes keyed by id_municipio."""
    if municipio.empty:
        return pd.DataFrame(columns=["id_municipio"])

    mun = _standardize_keys(municipio)
    rename = {"nome": "nome_municipio"}
    if "nome_regiao" in mun.columns and "regiao_municipio" not in mun.columns:
        # keep nome_regiao; also expose as regiao_municipio when absent
        pass
    cols = [c for c in _MUNICIPIO_CONTEXT_COLS if c in mun.columns]
    lookup = mun[cols].rename(columns=rename).drop_duplicates(
        subset=["id_municipio"], keep="first"
    )
    if "nome_regiao" in lookup.columns and "regiao_municipio" not in lookup.columns:
        lookup["regiao_municipio"] = lookup["nome_regiao"]
    return lookup


def _latest_metric_by_municipio(
    df: pd.DataFrame,
    value_cols: list[str],
    *,
    ref_col: str,
) -> pd.DataFrame:
    """Keep the latest ``ano`` row per municipality for socio/economic metrics."""
    if df.empty:
        return pd.DataFrame(columns=["id_municipio", *value_cols, ref_col])

    prepared = _standardize_keys(df)
    if "ano" not in prepared.columns:
        prepared["ano"] = pd.NA
    prepared = prepared.sort_values("ano", ascending=False, na_position="last")
    keep = ["id_municipio", "ano", *[c for c in value_cols if c in prepared.columns]]
    latest = prepared[keep].drop_duplicates(subset=["id_municipio"], keep="first")
    latest = latest.rename(columns={"ano": ref_col})
    return latest


def build_contexto_territorio(
    meta_municipio: pd.DataFrame,
    municipio: pd.DataFrame,
    *,
    alunos: pd.DataFrame | None = None,
    populacao: pd.DataFrame | None = None,
    pib: pd.DataFrame | None = None,
    socioeconomico: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    Build município×rede×ano context with standardised ``rede`` and socio metrics.

    Grain includes every ``(ano, id_municipio, rede)`` present in ``meta_municipio``
    and (optionally) in ``alunos``, so redes without meta still get territory/socio.
    """
    meta = _standardize_keys(meta_municipio)
    frames: list[pd.DataFrame] = []

    if not meta.empty:
        frames.append(meta[JOIN_KEYS_MUNICIPIO].drop_duplicates())

    if alunos is not None and not alunos.empty:
        alunos_std = _standardize_keys(alunos)
        if set(JOIN_KEYS_MUNICIPIO).issubset(alunos_std.columns):
            frames.append(alunos_std[JOIN_KEYS_MUNICIPIO].drop_duplicates())

    if not frames:
        logger.warning("No keys available to build contexto_territorio.")
        return pd.DataFrame()

    keys = pd.concat(frames, ignore_index=True).drop_duplicates()
    keys = keys.dropna(subset=["id_municipio", "rede"])

    meta_cols = [
        c
        for c in [
            *JOIN_KEYS_MUNICIPIO,
            "taxa_alfabetizacao",
            *META_GOAL_COLUMNS,
            "nivel_alfabetizacao",
            "percentual_participacao",
            "nome_municipio",
            "sigla_uf",
            "nome_uf",
            "regiao_municipio",
            "capital_uf",
            "_ingestion_timestamp",
            "_source_table",
            "_batch_id",
            "_silver_processed_at",
            "_silver_batch_id",
            "_join_match",
        ]
        if c in meta.columns
    ]
    contexto = keys.merge(
        meta[meta_cols].drop_duplicates(subset=JOIN_KEYS_MUNICIPIO),
        on=JOIN_KEYS_MUNICIPIO,
        how="left",
    )

    mun_lookup = _municipio_lookup(municipio)
    if not mun_lookup.empty:
        # Prefer meta names when present; fill from município directory.
        contexto = contexto.merge(mun_lookup, on="id_municipio", how="left", suffixes=("", "_mun"))
        for base in (
            "nome_municipio",
            "sigla_uf",
            "nome_uf",
            "regiao_municipio",
            "capital_uf",
            "nome_regiao",
            "nome_mesorregiao",
            "nome_microrregiao",
            "amazonia_legal",
        ):
            mun_col = f"{base}_mun"
            if base in contexto.columns and mun_col in contexto.columns:
                contexto[base] = contexto[base].combine_first(contexto[mun_col])
                contexto = contexto.drop(columns=[mun_col])
            elif mun_col in contexto.columns:
                contexto = contexto.rename(columns={mun_col: base})

    if populacao is not None and not populacao.empty:
        pop = _latest_metric_by_municipio(
            populacao, ["populacao"], ref_col="populacao_ano_ref"
        )
        contexto = contexto.merge(pop, on="id_municipio", how="left")

    if pib is not None and not pib.empty:
        pib_df = _latest_metric_by_municipio(pib, ["pib"], ref_col="pib_ano_ref")
        contexto = contexto.merge(pib_df, on="id_municipio", how="left")
        if "pib" in contexto.columns and "populacao" in contexto.columns:
            contexto["pib_per_capita"] = (
                pd.to_numeric(contexto["pib"], errors="coerce")
                / pd.to_numeric(contexto["populacao"], errors="coerce").replace(0, pd.NA)
            )

    if socioeconomico is not None and not socioeconomico.empty:
        socio = _latest_metric_by_municipio(
            socioeconomico,
            [
                "ivs",
                "ivs_infraestrutura_urbana",
                "ivs_capital_humano",
                "ivs_renda_trabalho",
            ],
            ref_col="socio_ano_ref",
        )
        contexto = contexto.merge(socio, on="id_municipio", how="left")

    if "_join_match" not in contexto.columns:
        contexto["_join_match"] = contexto["taxa_alfabetizacao"].notna() | contexto[
            "nome_municipio"
        ].notna()

    logger.info(
        "contexto_territorio built: %d rows | redes=%s",
        len(contexto),
        sorted(contexto["rede"].dropna().astype(str).unique().tolist()),
    )
    return contexto.reset_index(drop=True)


def build_alunos_features(
    alunos: pd.DataFrame,
    contexto: pd.DataFrame,
) -> pd.DataFrame:
    """
    Materialise aluno → contexto join with standardised keys.

    Join keys: ``(ano, id_municipio, rede)``.
    """
    if alunos.empty:
        return pd.DataFrame()

    left = _standardize_keys(alunos)
    left["alfabetizado"] = normalize_alfabetizado_flag(
        left.get("alfabetizado", pd.Series(dtype="string"))
    )

    if contexto.empty:
        logger.warning("Empty contexto — alunos_features will have null enrichment.")
        out = left.copy()
        out["_join_match"] = False
        return out

    right = _standardize_keys(contexto)
    core = [
        c
        for c in [
            "ano",
            "id_aluno",
            "id_municipio",
            "id_escola",
            "rede",
            "serie",
            "alfabetizado",
            "peso_aluno",
            "proficiencia",
            "_ingestion_timestamp",
            "_silver_processed_at",
            "_silver_batch_id",
        ]
        if c in left.columns
    ]
    left_core = left[core].copy()

    ctx_cols = [
        c
        for c in right.columns
        if c in JOIN_KEYS_MUNICIPIO or c == "_join_match" or not c.startswith("_")
    ]
    right_ctx = right[ctx_cols].drop_duplicates(subset=JOIN_KEYS_MUNICIPIO)

    merged = left_core.merge(right_ctx, on=JOIN_KEYS_MUNICIPIO, how="left")
    has_name = (
        merged["nome_municipio"].notna()
        if "nome_municipio" in merged.columns
        else pd.Series(False, index=merged.index)
    )
    if "_join_match" in merged.columns:
        merged["_join_match"] = merged["_join_match"].fillna(False) | has_name
    else:
        merged["_join_match"] = has_name

    matched = float(merged["_join_match"].mean()) if len(merged) else 0.0
    logger.info(
        "alunos_features built: %d rows | join coverage=%.1f%%",
        len(merged),
        matched * 100.0,
    )
    if matched < 0.95:
        logger.warning(
            "alunos→contexto join coverage below 95%% (%.1f%%). "
            "Check rede standardisation and meta_municipio coverage.",
            matched * 100.0,
        )
    return merged.reset_index(drop=True)


def build_alunos_analytic(alunos_features: pd.DataFrame) -> pd.DataFrame:
    """
    ML-ready view: target + features, without pipeline metadata / leakage cols.
    """
    if alunos_features.empty:
        return pd.DataFrame()

    drop_cols = [
        c
        for c in alunos_features.columns
        if c in _LEAKAGE_COLS
        or c.startswith("_")
        or c in {"id_escola", "serie"}
    ]
    # Keep id_aluno for audit/grain; model pipeline should drop it later.
    analytic = alunos_features.drop(columns=drop_cols, errors="ignore").copy()
    if "alfabetizado" not in analytic.columns:
        raise ValueError("alunos_analytic requires target column 'alfabetizado'.")

    logger.info("alunos_analytic built: %d rows × %d cols", *analytic.shape)
    return analytic.reset_index(drop=True)
