"""
Gold layer transformations — Indicador Criança Alfabetizada, territorial
context, and student-level features for supervised modelling.

Medallion: reads Silver tables only.
"""

from __future__ import annotations

import logging

import pandas as pd

from ingestion.gold.config import LEAKAGE_FEATURE_COLUMNS, META_GOAL_YEARS
from ingestion.silver.transforms import standardize_common

logger = logging.getLogger(__name__)

META_GOAL_COLUMNS: tuple[str, ...] = tuple(
    f"meta_alfabetizacao_{year}" for year in META_GOAL_YEARS
)

_EXTRA_META_COLUMNS: tuple[str, ...] = (
    "taxa_alfabetizacao",
    *META_GOAL_COLUMNS,
    "nome_municipio",
    "nome_uf",
    "sigla_uf",
    "percentual_participacao",
    "regiao_municipio",
    "regiao_uf",
    "capital_uf",
    "nome_regiao",
    "nome_mesorregiao",
    "nome_microrregiao",
    "amazonia_legal",
)

_TERRITORIAL_MUNICIPIO_COLS: tuple[str, ...] = (
    "id_municipio",
    "nome",
    "sigla_uf",
    "nome_uf",
    "nome_regiao",
    "capital_uf",
    "nome_mesorregiao",
    "nome_microrregiao",
    "amazonia_legal",
)

_INEP_LAG_VALUE_COLS: tuple[str, ...] = (
    "taxa_alfabetizacao",
    "media_portugues",
    *(f"proporcao_aluno_nivel_{level}" for level in range(9)),
)

_SOCIO_VALUE_COLS: tuple[str, ...] = (
    "ivs",
    "ivs_infraestrutura_urbana",
    "ivs_capital_humano",
    "ivs_renda_e_trabalho",
    "ivs_renda_trabalho",
)

_JOIN_KEYS_MUNICIPIO: list[str] = ["ano", "id_municipio", "rede"]

_PIPELINE_OR_LEAKAGE_ANALYTIC: frozenset[str] = frozenset(
    {
        "nivel_alfabetizacao",
        "proficiencia",
        "id_escola",
        "serie",
        "caderno",
        "presenca",
        "preenchimento_caderno",
    }
)


def _standardize_keys(df: pd.DataFrame) -> pd.DataFrame:
    """Apply shared Silver key rules (rede codes → labels, id_municipio 7 digits)."""
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

    meta = _standardize_keys(meta)
    available_keys = [key for key in join_keys if key in meta.columns]
    meta_cols = [
        col
        for col in _EXTRA_META_COLUMNS
        if col in meta.columns and col not in available_keys
    ]
    if official_rate_col not in meta_cols and official_rate_col in meta.columns:
        meta_cols = [official_rate_col, *meta_cols]

    meta_subset = meta[available_keys + meta_cols].drop_duplicates(subset=available_keys)

    enriched = indicator.merge(meta_subset, on=available_keys, how="left")

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


def attach_prefixed_meta(
    left: pd.DataFrame,
    meta: pd.DataFrame,
    join_keys: list[str],
    prefix: str,
) -> pd.DataFrame:
    """Left-join meta columns with a prefix (national / UF goals)."""
    if left.empty or meta.empty:
        return left.copy()

    available_keys = [key for key in join_keys if key in meta.columns and key in left.columns]
    if not available_keys:
        return left.copy()

    value_cols = [col for col in _EXTRA_META_COLUMNS if col in meta.columns and col not in available_keys]
    if not value_cols:
        return left.copy()

    subset = meta[available_keys + value_cols].drop_duplicates(subset=available_keys)
    renamed = subset.rename(columns={col: f"{prefix}{col}" for col in value_cols})
    return left.merge(renamed, on=available_keys, how="left")


def attach_territorio_municipio(
    df: pd.DataFrame,
    municipio: pd.DataFrame,
) -> pd.DataFrame:
    """Join remaining territorial attributes from the municipality directory."""
    if df.empty or municipio.empty or "id_municipio" not in df.columns:
        return df.copy()

    cols = [col for col in _TERRITORIAL_MUNICIPIO_COLS if col in municipio.columns]
    lookup = (
        municipio[cols]
        .rename(
            columns={
                "nome": "nome_municipio",
                "nome_regiao": "nome_regiao",
            }
        )
        .drop_duplicates(subset=["id_municipio"])
    )
    extra = [col for col in lookup.columns if col == "id_municipio" or col not in df.columns]
    if extra == ["id_municipio"]:
        return df.copy()
    return df.merge(lookup[extra], on="id_municipio", how="left")


def as_of_join(
    left: pd.DataFrame,
    right: pd.DataFrame,
    *,
    by: str,
    time_col: str,
    value_cols: list[str],
    right_time_alias: str,
) -> pd.DataFrame:
    """Attach the latest ``right`` row per entity with ``time_col`` <= left time."""
    result = left.copy()
    present_values = [col for col in value_cols if col in right.columns]
    for col in present_values:
        if col not in result.columns:
            result[col] = pd.NA
    if right_time_alias not in result.columns:
        result[right_time_alias] = pd.NA

    if (
        result.empty
        or right.empty
        or by not in result.columns
        or by not in right.columns
        or time_col not in result.columns
        or time_col not in right.columns
        or not present_values
    ):
        return result

    take = [by, time_col, *present_values]
    right_sub = right[take].dropna(subset=[by, time_col]).drop_duplicates()
    if right_sub.empty:
        return result

    result["_row_id"] = range(len(result))
    left_ok = result.dropna(subset=[by, time_col]).copy()
    left_na = result.loc[~result.index.isin(left_ok.index)].copy()

    left_ok[time_col] = pd.to_numeric(left_ok[time_col], errors="coerce")
    right_sub[time_col] = pd.to_numeric(right_sub[time_col], errors="coerce")
    left_ok = left_ok.dropna(subset=[time_col])
    right_sub = right_sub.dropna(subset=[time_col])
    left_ok[time_col] = left_ok[time_col].astype("int64")
    right_sub[time_col] = right_sub[time_col].astype("int64")
    left_ok[by] = left_ok[by].astype("string")
    right_sub[by] = right_sub[by].astype("string")

    if left_ok.empty or right_sub.empty:
        return result.drop(columns=["_row_id"])

    right_sub = right_sub.rename(columns={time_col: right_time_alias})
    # Drop placeholder columns so merge_asof can attach real values.
    drop_placeholders = [
        col for col in [*present_values, right_time_alias] if col in left_ok.columns
    ]
    left_ok = left_ok.drop(columns=drop_placeholders)

    merged = pd.merge_asof(
        left_ok.sort_values([by, time_col]),
        right_sub.sort_values([by, right_time_alias]),
        left_on=time_col,
        right_on=right_time_alias,
        by=by,
        direction="backward",
    )
    combined = pd.concat([merged, left_na], ignore_index=True)
    combined = combined.sort_values("_row_id").drop(columns=["_row_id"])
    return combined.reset_index(drop=True)


def snapshot_join(
    left: pd.DataFrame,
    right: pd.DataFrame,
    *,
    by: str,
    time_col: str,
    value_cols: list[str],
    year_alias: str,
) -> pd.DataFrame:
    """Broadcast the latest snapshot per entity (e.g. AVS 2010) onto ``left``."""
    result = left.copy()
    present_values = [col for col in value_cols if col in right.columns]
    if result.empty or right.empty or by not in result.columns or not present_values:
        for col in present_values:
            if col not in result.columns:
                result[col] = pd.NA
        if year_alias not in result.columns:
            result[year_alias] = pd.NA
        return result

    usable = right.dropna(subset=[by, time_col]) if time_col in right.columns else right.dropna(subset=[by])
    if usable.empty:
        return result

    if time_col in usable.columns:
        idx = usable.groupby(by, dropna=False)[time_col].idxmax()
        snap = usable.loc[idx, [by, time_col, *present_values]].rename(
            columns={time_col: year_alias}
        )
    else:
        snap = usable[[by, *present_values]].drop_duplicates(subset=[by])
        snap[year_alias] = pd.NA

    extra = [col for col in snap.columns if col == by or col not in result.columns]
    return result.merge(snap[extra], on=by, how="left")


def _filter_segundo_ano_or_aggregate(
    indicators: pd.DataFrame,
    group_keys: list[str],
) -> pd.DataFrame:
    """Prefer 2º ano rows; otherwise mean numeric columns at ``group_keys``."""
    if indicators.empty:
        return indicators.copy()

    working = indicators.copy()
    if "serie" in working.columns:
        serie = working["serie"].astype("string").str.lower()
        segundo = working[serie.str.contains(r"2\s*[ºo°]?\s*ano", regex=True, na=False)]
        if not segundo.empty:
            working = segundo

    numeric_cols = [
        col
        for col in _INEP_LAG_VALUE_COLS
        if col in working.columns
    ]
    present_keys = [key for key in group_keys if key in working.columns]
    if not present_keys or not numeric_cols:
        return working

    aggregated = (
        working.groupby(present_keys, dropna=False)[numeric_cols]
        .mean()
        .reset_index()
    )
    return aggregated


def lag_indicators(
    indicators: pd.DataFrame,
    *,
    group_keys: list[str],
    prefix: str = "lag1_",
) -> pd.DataFrame:
    """Shift indicator year by +1 so they join onto the following cohort."""
    preferred = _filter_segundo_ano_or_aggregate(indicators, group_keys)
    if preferred.empty or "ano" not in preferred.columns:
        return pd.DataFrame()

    lagged = preferred.copy()
    lagged["ano"] = pd.to_numeric(lagged["ano"], errors="coerce") + 1
    rename = {
        col: f"{prefix}{col}"
        for col in lagged.columns
        if col not in group_keys and col != "ano"
    }
    return lagged.rename(columns=rename)


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


def build_contexto_territorio(
    meta_municipio: pd.DataFrame,
    meta_uf: pd.DataFrame,
    meta_brasil: pd.DataFrame,
    municipio: pd.DataFrame,
    populacao: pd.DataFrame,
    pib: pd.DataFrame,
    socioeconomico: pd.DataFrame,
    municipio_indicadores: pd.DataFrame,
    uf_indicadores: pd.DataFrame,
    alunos: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build the municipal feature store used by BI and ``alunos_features``."""
    meta = _standardize_keys(meta_municipio)
    if meta.empty and (alunos is None or alunos.empty):
        logger.warning("Cannot build contexto_territorio: empty meta_municipio.")
        return pd.DataFrame()

    key_frames: list[pd.DataFrame] = []
    if not meta.empty and set(_JOIN_KEYS_MUNICIPIO).issubset(meta.columns):
        key_frames.append(meta[_JOIN_KEYS_MUNICIPIO].drop_duplicates())
    if alunos is not None and not alunos.empty:
        alunos_std = _standardize_keys(alunos)
        if set(_JOIN_KEYS_MUNICIPIO).issubset(alunos_std.columns):
            key_frames.append(alunos_std[_JOIN_KEYS_MUNICIPIO].drop_duplicates())

    if not key_frames:
        logger.warning("Cannot build contexto_territorio: no join keys available.")
        return pd.DataFrame()

    keys = pd.concat(key_frames, ignore_index=True).drop_duplicates()
    keys = keys.dropna(subset=["id_municipio", "rede"])
    contexto = keys.merge(meta, on=_JOIN_KEYS_MUNICIPIO, how="left") if not meta.empty else keys

    contexto = attach_territorio_municipio(contexto, _standardize_keys(municipio))
    contexto = attach_prefixed_meta(
        contexto,
        _standardize_keys(meta_uf),
        ["ano", "sigla_uf", "rede"],
        prefix="uf_",
    )
    contexto = attach_prefixed_meta(
        contexto,
        _standardize_keys(meta_brasil),
        ["ano", "rede"],
        prefix="brasil_",
    )

    contexto = as_of_join(
        contexto,
        _standardize_keys(populacao),
        by="id_municipio",
        time_col="ano",
        value_cols=["populacao"],
        right_time_alias="populacao_ano_ref",
    )
    contexto = as_of_join(
        contexto,
        _standardize_keys(pib),
        by="id_municipio",
        time_col="ano",
        value_cols=["pib"],
        right_time_alias="pib_ano_ref",
    )

    if "pib" in contexto.columns and "populacao" in contexto.columns:
        pib_num = pd.to_numeric(contexto["pib"], errors="coerce")
        pop_num = pd.to_numeric(contexto["populacao"], errors="coerce")
        contexto["pib_per_capita"] = pib_num / pop_num.replace(0, pd.NA)

    socio_std = _standardize_keys(socioeconomico)
    socio_cols = [col for col in _SOCIO_VALUE_COLS if col in socio_std.columns]
    contexto = snapshot_join(
        contexto,
        socio_std,
        by="id_municipio",
        time_col="ano",
        value_cols=socio_cols,
        year_alias="socio_ano_ref",
    )

    lagged_mun = lag_indicators(
        _standardize_keys(municipio_indicadores),
        group_keys=["ano", "id_municipio", "rede"],
    )
    if not lagged_mun.empty:
        lag_keys = [
            key for key in ["ano", "id_municipio", "rede"] if key in lagged_mun.columns
        ]
        contexto = contexto.merge(lagged_mun, on=lag_keys, how="left")

    if "sigla_uf" in contexto.columns:
        lagged_uf = lag_indicators(
            _standardize_keys(uf_indicadores),
            group_keys=["ano", "sigla_uf", "rede"],
            prefix="lag1_uf_",
        )
        if not lagged_uf.empty:
            lag_keys = [
                key for key in ["ano", "sigla_uf", "rede"] if key in lagged_uf.columns
            ]
            contexto = contexto.merge(lagged_uf, on=lag_keys, how="left")

    logger.info(
        "contexto_territorio built: %d rows | redes=%s",
        len(contexto),
        sorted(contexto["rede"].dropna().astype(str).unique().tolist())
        if "rede" in contexto.columns
        else [],
    )
    return contexto


def _drop_leakage_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Remove same-year outcome columns that would leak the literacy label."""
    extra_lag_unprefixed = [
        col
        for col in df.columns
        if col.startswith("proporcao_aluno_nivel_") and not col.startswith("lag1_")
    ]
    drop = [
        col
        for col in (*LEAKAGE_FEATURE_COLUMNS, *extra_lag_unprefixed, "media_portugues")
        if col in df.columns and not col.startswith("lag1_")
    ]
    if not drop:
        return df
    return df.drop(columns=drop)


def build_alunos_features(
    alunos: pd.DataFrame,
    contexto: pd.DataFrame,
) -> pd.DataFrame:
    """Join student microdata to territorial context without label leakage."""
    if alunos.empty:
        logger.warning("No alunos rows for alunos_features.")
        return pd.DataFrame()

    features = _standardize_keys(alunos)
    features["alfabetizado"] = normalize_alfabetizado_flag(
        features.get("alfabetizado", pd.Series(dtype="string"))
    )

    drop_student = [col for col in ("proficiencia",) if col in features.columns]
    if drop_student:
        features = features.drop(columns=drop_student)

    if contexto.empty:
        logger.warning("contexto_territorio is empty — alunos_features without context.")
        features["_join_match"] = False
        return _drop_leakage_columns(features)

    context_clean = _drop_leakage_columns(_standardize_keys(contexto))
    join_keys = [
        key
        for key in _JOIN_KEYS_MUNICIPIO
        if key in features.columns and key in context_clean.columns
    ]
    overlap = [
        col
        for col in context_clean.columns
        if col in features.columns and col not in join_keys
    ]
    if overlap:
        context_clean = context_clean.drop(columns=overlap)

    features = features.merge(context_clean, on=join_keys, how="left")
    features["_join_match"] = (
        features["nome_municipio"].notna()
        if "nome_municipio" in features.columns
        else False
    )
    features = _drop_leakage_columns(features)

    coverage = float(features["_join_match"].mean()) if len(features) else 0.0
    logger.info(
        "alunos_features built: %d rows | join coverage=%.1f%%",
        len(features),
        coverage * 100.0,
    )
    if coverage < 0.80:
        logger.warning(
            "alunos→contexto join coverage below 80%% (%.1f%%). "
            "Check rede standardisation and meta coverage.",
            coverage * 100.0,
        )
    return features


def build_alunos_analytic(alunos_features: pd.DataFrame) -> pd.DataFrame:
    """ML-ready view: target + features without pipeline metadata / leakage cols."""
    if alunos_features.empty:
        return pd.DataFrame()

    drop_cols = [
        col
        for col in alunos_features.columns
        if col in _PIPELINE_OR_LEAKAGE_ANALYTIC
        or col.startswith("_")
    ]
    analytic = alunos_features.drop(columns=drop_cols, errors="ignore").copy()
    if "alfabetizado" not in analytic.columns:
        raise ValueError("alunos_analytic requires target column 'alfabetizado'.")

    logger.info("alunos_analytic built: %d rows × %d cols", *analytic.shape)
    return analytic.reset_index(drop=True)
