"""Adaptadores para migrar el dashboard legacy de indicadores a Django."""

from __future__ import annotations

from datetime import datetime
from io import BytesIO
from typing import Any

import pandas as pd

from accounts.db import get_connection


RATE_COLUMNS = {
    "Tasa Varones",
    "Tasa Mujeres",
    "Tasa Cobertura DRE/GRE",
    "Tasa Cobertura UGEL",
    "Tasa DRE/GRE Fortalecida",
    "Tasa UGEL Fortalecida",
    "Tasa Retencion",
    "Tasa Finalizacion",
    "Tasa Certificacion",
    "Tasa Progreso",
    "Tasa Satisfaccion",
}

COUNT_COLUMNS = {
    "Postulaciones",
    "Matriculaciones",
    "Participaciones",
    "Varones",
    "Mujeres",
    "Retiros",
    "Finalizaciones",
    "Certificaciones",
    "Evaluados",
    "Progreso",
    "Capacitacion",
    "DRE/GRE Coberturada",
    "DRE/GRE Fortalecida",
    "UGEL Coberturada",
    "UGEL Fortalecida",
    "Total DRE/GRE",
    "Total UGEL",
    "Capacitaciones",
}


def _fetch_dataframe(table_name: str, columns: list[str]) -> pd.DataFrame:
    """Lee un subconjunto de columnas desde MySQL y retorna DataFrame."""
    sql_columns = ", ".join(f"`{column}`" for column in columns)
    sql = f"SELECT {sql_columns} FROM `{table_name}`"
    try:
        with get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql)
                rows = cursor.fetchall()
    except Exception:
        return pd.DataFrame(columns=columns)

    frame = pd.DataFrame(list(rows or []))
    if frame.empty:
        return pd.DataFrame(columns=columns)
    return frame


def _load_base_tables() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Carga las tablas base necesarias para el dashboard."""
    oferta = _fetch_dataframe(
        "oferta_formativa_difoca",
        [
            "codigo",
            "anio",
            "condicion",
            "tipo_proceso_formativo",
            "denominacion_proceso_formativo",
            "especialista_cargo",
            "objetivo_capacitacion",
            "implementacion_final",
        ],
    )
    bbdd = _fetch_dataframe(
        "bbdd_difoca",
        [
            "ID", "obs", "numero", "codigo", "tipo_documento", "dni",
            "apellidos", "nombres", "genero", "fecha_nacimiento",
            "telefono_celular", "email", "actualizo_datos",
            "region", "tipo_iged", "codigo_iged", "nombre_iged",
            "ambitos", "nivel_puesto", "nombre_puesto", "regimen_laboral",
            "publico_objetivo", "ultimo_acceso_curso", "dias_ausencia",
            "ingreso_curso", "estado", "compromiso",
            "promedio_final_general", "promedio_final_condicion",
            "avance_curso_certificacion", "estado_participante_curso",
            "situacion_participante", "retiros", "aprobados_certificados",
            "desaprobado_permanente", "desaprobado_abandono",
            "cuestionario_entrada", "cuestionario_salida",
            "encuesta", "ev_progreso_aprendizaje", "mantuvo_o_progreso",
            "progreso", "nivel_c_entrada", "nivel_c_salida", "telefono_fijo",
        ],
    )
    satisfaccion = _fetch_dataframe(
        "satisfaccion",
        ["codigo", "anio", "satisfaccion", "aspecto"],
    )
    iged = _fetch_dataframe(
        "iged_s3",
        ["region", "NOMBRE IGED", "tipo IGED", "CODIGO_R"],
    )
    return oferta, bbdd, satisfaccion, iged


def _build_process_label(df: pd.DataFrame) -> pd.Series:
    tipo = df.get("tipo_proceso_formativo", pd.Series(index=df.index, dtype=str)).fillna("").astype(str).str.strip()
    nombre = df.get("denominacion_proceso_formativo", pd.Series(index=df.index, dtype=str)).fillna("").astype(str).str.strip()
    return (tipo + " " + nombre).str.strip()


def _to_flag(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    if numeric.notna().any():
        return (numeric.fillna(0) >= 1).astype(int)
    text = series.fillna("").astype(str).str.strip().str.upper()
    return text.isin({"1", "SI", "SÍ", "TRUE", "VERDADERO", "X", "YES", "Y"}).astype(int)


def _normalize_iged_type(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip().str.upper().str.replace(" ", "", regex=False)


def _normalize_gender(series: pd.Series) -> pd.Series:
    normalized = (
        series.fillna("")
        .astype(str)
        .str.strip()
        .str.lower()
        .str.normalize("NFKD")
        .str.encode("ascii", errors="ignore")
        .str.decode("ascii")
    )
    result = pd.Series("", index=series.index, dtype=str)
    result.loc[normalized.isin({"m", "masculino", "masculina", "hombre", "varon", "masc"})] = "M"
    result.loc[normalized.isin({"f", "femenino", "femenina", "mujer", "fem"})] = "F"
    return result


def _selected_values(values: list[str], options: list[str], default: list[str]) -> list[str]:
    filtered = [value for value in values if value in options]
    return filtered or default


def _selected_value(value: Any, options: list[str], default: str = "") -> str:
    selected = str(value or "").strip()
    if selected in options:
        return selected
    return default if default in options or default == "" else ""


def _selected_values(raw: Any, options: list[str], default: str = "") -> list[str]:
    """Devuelve lista de valores seleccionados validados contra opciones disponibles."""
    if isinstance(raw, list):
        items = [str(v).strip() for v in raw if str(v).strip()]
    elif raw:
        items = [str(raw).strip()]
    else:
        items = []
    valid = [v for v in items if v in options]
    if not valid and default and default in options:
        valid = [default]
    return valid


def _series_options(series: pd.Series, *, reverse: bool = False) -> list[str]:
    values = [str(value).strip() for value in series.dropna().tolist() if str(value).strip()]
    return sorted(pd.Index(values).unique().tolist(), reverse=reverse)


def _filter_offer(
    oferta: pd.DataFrame,
    selected_years: list[str],
    selected_condition: str,
    selected_processes: list[str],
    fecha_inicio: str,
    fecha_fin: str,
) -> pd.DataFrame:
    filtered = oferta.copy()
    filtered.loc[:, "anio_text"] = filtered.get("anio", pd.Series(index=filtered.index, dtype=object)).fillna("").astype(str).str.strip()
    filtered.loc[:, "Proceso Formativo"] = _build_process_label(filtered)

    if selected_years:
        filtered = filtered[filtered["anio_text"].isin(selected_years)]
    if selected_condition:
        filtered = filtered[filtered.get("condicion", "").fillna("").astype(str).str.strip() == selected_condition]
    if selected_processes:
        filtered = filtered[filtered["Proceso Formativo"].isin(selected_processes)]

    if fecha_inicio or fecha_fin:
        fechas = pd.to_datetime(filtered.get("implementacion_final"), errors="coerce")
        if fecha_inicio:
            filtered = filtered[fechas >= pd.to_datetime(fecha_inicio)]
            fechas = pd.to_datetime(filtered.get("implementacion_final"), errors="coerce")
        if fecha_fin:
            filtered = filtered[fechas <= pd.to_datetime(fecha_fin)]

    return filtered


def _filter_participants(bbdd: pd.DataFrame, selected_regions: list[str], selected_igeds: list[str]) -> pd.DataFrame:
    filtered = bbdd.copy()
    filtered.loc[:, "region_text"] = filtered.get("region", pd.Series(index=filtered.index, dtype=object)).fillna("").astype(str).str.strip()
    filtered.loc[:, "iged_text"] = filtered.get("nombre_iged", pd.Series(index=filtered.index, dtype=object)).fillna("").astype(str).str.strip()

    if selected_regions:
        filtered = filtered[filtered["region_text"].isin(selected_regions)]
    if selected_igeds:
        filtered = filtered[filtered["iged_text"].isin(selected_igeds)]
    return filtered


def _national_iged_totals(iged: pd.DataFrame) -> tuple[int, int]:
    if iged.empty:
        return 0, 0
    tipo = _normalize_iged_type(iged.get("tipo IGED", pd.Series(dtype=str)))
    nombre = iged.get("NOMBRE IGED", pd.Series(dtype=str)).fillna("").astype(str).str.strip()
    total_dre = int(nombre[tipo == "DRE/GRE"].nunique())
    total_ugel = int(nombre[tipo == "UGEL"].nunique())
    return total_dre, total_ugel


def _calculate_base_kpis(
    oferta: pd.DataFrame,
    bbdd: pd.DataFrame,
    iged: pd.DataFrame,
    group_by: list[str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Replica el nucleo del calculo de KPIs del dashboard legacy."""
    merged = pd.merge(bbdd, oferta, on="codigo", how="right", suffixes=("_b", "_o"))
    if merged.empty:
        return pd.DataFrame(columns=group_by), merged

    merged["Proceso Formativo"] = _build_process_label(merged)
    merged["tipo_iged_norm"] = _normalize_iged_type(merged.get("tipo_iged", pd.Series(dtype=str)))
    for _text_col in ["region", "nombre_iged", "tipo_iged"]:
        if _text_col in merged.columns:
            merged[_text_col] = merged[_text_col].fillna("").astype(str).str.strip()
            merged.loc[merged[_text_col].str.lower() == "none", _text_col] = ""
    merged["estado_num"] = pd.to_numeric(merged.get("estado"), errors="coerce")
    merged["compromiso_num"] = pd.to_numeric(merged.get("compromiso"), errors="coerce")
    merged["aprobados_certificados_flag"] = _to_flag(merged.get("aprobados_certificados", pd.Series(dtype=object)))
    merged["desaprobado_permanente_flag"] = _to_flag(merged.get("desaprobado_permanente", pd.Series(dtype=object)))
    merged["retiros_flag"] = _to_flag(merged.get("retiros", pd.Series(dtype=object)))
    merged["genero_norm"] = _normalize_gender(merged.get("genero", pd.Series(index=merged.index, dtype=object)))

    merged["base_participa"] = (merged["estado_num"] == 2) & (merged["compromiso_num"].isin([20, 1]))
    merged["varones_base"] = (merged["base_participa"] & (merged["genero_norm"] == "M")).astype(int)
    merged["mujeres_base"] = (merged["base_participa"] & (merged["genero_norm"] == "F")).astype(int)
    merged["retiros_base"] = ((merged["base_participa"]) & (merged["retiros_flag"] == 1)).astype(int)
    merged["finalizados"] = (
        merged["base_participa"]
        & ((merged["aprobados_certificados_flag"] == 1) | (merged["desaprobado_permanente_flag"] == 1))
    ).astype(int)
    merged["cert_base"] = (merged["base_participa"] & (merged["aprobados_certificados_flag"] == 1)).astype(int)

    merged["cuestionario_entrada_num"] = pd.to_numeric(merged.get("cuestionario_entrada"), errors="coerce")
    merged["cuestionario_salida_num"] = pd.to_numeric(merged.get("cuestionario_salida"), errors="coerce")
    merged["tiene_cuestionarios"] = (
        merged["cuestionario_entrada_num"].notna()
        & merged["cuestionario_salida_num"].notna()
        & (merged["cuestionario_entrada_num"] > 0)
        & (merged["cuestionario_salida_num"] > 0)
    )
    merged["diferencia_cuestionarios"] = merged["cuestionario_salida_num"] - merged["cuestionario_entrada_num"]
    merged["evaluados_base"] = ((merged["cert_base"] == 1) & merged["tiene_cuestionarios"]).astype(int)
    merged["progreso_base"] = (
        (merged["cert_base"] == 1)
        & merged["tiene_cuestionarios"]
        & (merged["diferencia_cuestionarios"] > 0)
    ).astype(int)

    total_dre_nacional, total_ugel_nacional = _national_iged_totals(iged)
    agrupadores = [column for column in group_by if column in merged.columns]
    if not agrupadores:
        return pd.DataFrame(), merged

    for col in agrupadores:
        merged[col] = merged[col].fillna("").astype(str).str.strip()
        merged.loc[merged[col].str.lower() == "none", col] = ""

    kpis = merged.groupby(agrupadores, dropna=False).agg(
        Postulaciones=("dni", "count"),
        Matriculaciones=("estado_num", lambda values: int((values == 2).sum())),
        Participaciones=("base_participa", lambda values: int(values.fillna(False).sum())),
        Varones=("varones_base", "sum"),
        Mujeres=("mujeres_base", "sum"),
        Retiros=("retiros_base", "sum"),
        Finalizaciones=("finalizados", "sum"),
        Certificaciones=("cert_base", "sum"),
        Evaluados=("evaluados_base", "sum"),
        Progreso=("progreso_base", "sum"),
        **{
            "DRE/GRE Coberturada": (
                "nombre_iged",
                lambda values: int(values[merged.loc[values.index, "tipo_iged_norm"] == "DRE/GRE"].nunique()),
            ),
            "DRE/GRE Fortalecida": (
                "nombre_iged",
                lambda values: int(
                    values[
                        (merged.loc[values.index, "tipo_iged_norm"] == "DRE/GRE")
                        & (merged.loc[values.index, "cert_base"] == 1)
                    ].nunique()
                ),
            ),
            "UGEL Coberturada": (
                "nombre_iged",
                lambda values: int(values[merged.loc[values.index, "tipo_iged_norm"] == "UGEL"].nunique()),
            ),
            "UGEL Fortalecida": (
                "nombre_iged",
                lambda values: int(
                    values[
                        (merged.loc[values.index, "tipo_iged_norm"] == "UGEL")
                        & (merged.loc[values.index, "cert_base"] == 1)
                    ].nunique()
                ),
            ),
        },
    ).reset_index()

    kpis["Tasa Cobertura DRE/GRE"] = kpis["DRE/GRE Coberturada"] / total_dre_nacional if total_dre_nacional else pd.NA
    kpis["Tasa Cobertura UGEL"] = kpis["UGEL Coberturada"] / total_ugel_nacional if total_ugel_nacional else pd.NA
    kpis["Tasa DRE/GRE Fortalecida"] = kpis["DRE/GRE Fortalecida"] / kpis["DRE/GRE Coberturada"].replace(0, pd.NA)
    kpis["Tasa UGEL Fortalecida"] = kpis["UGEL Fortalecida"] / kpis["UGEL Coberturada"].replace(0, pd.NA)
    kpis["Tasa Varones"] = kpis["Varones"] / kpis["Participaciones"].replace(0, pd.NA)
    kpis["Tasa Mujeres"] = kpis["Mujeres"] / kpis["Participaciones"].replace(0, pd.NA)
    kpis["Tasa Retencion"] = kpis["Participaciones"] / kpis["Matriculaciones"].replace(0, pd.NA)
    kpis["Tasa Finalizacion"] = kpis["Finalizaciones"] / kpis["Participaciones"].replace(0, pd.NA)
    kpis["Tasa Certificacion"] = kpis["Certificaciones"] / kpis["Finalizaciones"].replace(0, pd.NA)
    kpis["Tasa Progreso"] = kpis["Progreso"] / kpis["Evaluados"].replace(0, pd.NA)
    kpis["Efectividad"] = ((kpis["Finalizaciones"] > 0) & ((kpis["Certificaciones"] / kpis["Finalizaciones"].replace(0, pd.NA)) >= 0.6)).astype(int)
    return kpis, merged


def _calculate_satisfaction_global(satisfaccion: pd.DataFrame, oferta_filtrada: pd.DataFrame) -> float | None:
    if satisfaccion.empty or oferta_filtrada.empty:
        return None

    satisf = satisfaccion.copy()
    satisf["codigo"] = satisf.get("codigo", "").fillna("").astype(str).str.strip()
    satisf["anio_text"] = satisf.get("anio", "").fillna("").astype(str).str.strip()

    oferta_keys = oferta_filtrada[["codigo", "anio_text"]].copy()
    oferta_keys["codigo"] = oferta_keys["codigo"].fillna("").astype(str).str.strip()
    oferta_keys = oferta_keys.drop_duplicates()
    sat_filtrada = satisf.merge(oferta_keys, on=["codigo", "anio_text"], how="inner")
    if sat_filtrada.empty:
        return None

    valores = sat_filtrada.get("satisfaccion", "").fillna("").astype(str).str.strip().str.lower()
    total = int(len(valores.index))
    if total == 0:
        return None
    satisfechos = int(valores.isin(["satisfecho", "satisfecha"]).sum())
    return satisfechos / total


def _calculate_capacitacion_kpis(
    oferta_filtrada: pd.DataFrame,
    bbdd: pd.DataFrame,
    satisfaccion: pd.DataFrame,
    iged: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, float | None]:
    kpis, merged = _calculate_base_kpis(
        oferta_filtrada,
        bbdd,
        iged,
        ["anio_text", "codigo", "Proceso Formativo", "especialista_cargo", "condicion", "objetivo_capacitacion"],
    )
    if kpis.empty:
        return kpis, merged, None

    satisf_global = _calculate_satisfaction_global(satisfaccion, oferta_filtrada)
    if not satisfaccion.empty:
        sat = satisfaccion.copy()
        sat["codigo"] = sat.get("codigo", "").fillna("").astype(str).str.strip()
        sat["anio_text"] = sat.get("anio", "").fillna("").astype(str).str.strip()
        sat["es_satisfecho"] = sat.get("satisfaccion", "").fillna("").astype(str).str.strip().str.lower().isin(["satisfecho", "satisfecha"]).astype(int)
        sat_resumen = sat.groupby(["codigo", "anio_text"]).agg(total=("es_satisfecho", "count"), satisfechos=("es_satisfecho", "sum")).reset_index()
        sat_resumen["Tasa Satisfaccion"] = sat_resumen["satisfechos"] / sat_resumen["total"].replace(0, pd.NA)
        kpis = kpis.merge(sat_resumen[["codigo", "anio_text", "Tasa Satisfaccion"]], on=["codigo", "anio_text"], how="left")
    else:
        kpis["Tasa Satisfaccion"] = pd.NA

    kpis["Capacitacion"] = 1
    kpis = kpis.rename(columns={"anio_text": "Año", "codigo": "Codigo", "especialista_cargo": "Especialista", "condicion": "Condicion", "objetivo_capacitacion": "Objetivo"})
    ordered_columns = [
        "Año",
        "Codigo",
        "Proceso Formativo",
        "Especialista",
        "Condicion",
        "Postulaciones",
        "Matriculaciones",
        "Participaciones",
        "Retiros",
        "Finalizaciones",
        "Certificaciones",
        "DRE/GRE Coberturada",
        "UGEL Coberturada",
        "DRE/GRE Fortalecida",
        "UGEL Fortalecida",
        "Evaluados",
        "Progreso",
        "Tasa Cobertura DRE/GRE",
        "Tasa Cobertura UGEL",
        "Tasa DRE/GRE Fortalecida",
        "Tasa UGEL Fortalecida",
        "Tasa Retencion",
        "Tasa Finalizacion",
        "Tasa Certificacion",
        "Tasa Progreso",
        "Tasa Satisfaccion",
        "Efectividad",
    ]
    result = kpis[[column for column in ordered_columns if column in kpis.columns]]
    sort_key = result["Proceso Formativo"].eq("") if "Proceso Formativo" in result.columns else pd.Series(False, index=result.index)
    result = result.assign(_blank=sort_key).sort_values(["_blank", "Año", "Proceso Formativo"] if "Año" in result.columns else ["_blank"], na_position="last").drop(columns="_blank").reset_index(drop=True)
    return result, merged, satisf_global


def _calculate_region_kpis(oferta_filtrada: pd.DataFrame, bbdd: pd.DataFrame, iged: pd.DataFrame) -> pd.DataFrame:
    kpis, merged = _calculate_base_kpis(oferta_filtrada, bbdd, iged, ["region"])
    if kpis.empty:
        return kpis

    iged_copy = iged.copy()
    iged_copy["tipo_norm"] = _normalize_iged_type(iged_copy.get("tipo IGED", pd.Series(dtype=str)))
    iged_copy["nombre_norm"] = iged_copy.get("NOMBRE IGED", pd.Series(dtype=str)).fillna("").astype(str).str.strip()
    total_dre = iged_copy[iged_copy["tipo_norm"] == "DRE/GRE"].groupby("region")["nombre_norm"].nunique()
    total_ugel = iged_copy[iged_copy["tipo_norm"] == "UGEL"].groupby("region")["nombre_norm"].nunique()
    capacidades = merged.groupby("region")["codigo"].nunique()

    kpis["Total DRE/GRE"] = kpis["region"].map(total_dre).fillna(0).astype(int)
    kpis["Total UGEL"] = kpis["region"].map(total_ugel).fillna(0).astype(int)
    kpis["Capacitacion"] = kpis["region"].map(capacidades).fillna(0).astype(int)
    kpis["Tasa Cobertura DRE/GRE"] = kpis["DRE/GRE Coberturada"] / kpis["Total DRE/GRE"].replace(0, pd.NA)
    kpis["Tasa Cobertura UGEL"] = kpis["UGEL Coberturada"] / kpis["Total UGEL"].replace(0, pd.NA)
    kpis = kpis.rename(columns={"region": "Region"})
    ordered_columns = [
        "Region",
        "Capacitacion",
        "Postulaciones",
        "Matriculaciones",
        "Participaciones",
        "Finalizaciones",
        "Certificaciones",
        "DRE/GRE Coberturada",
        "DRE/GRE Fortalecida",
        "UGEL Coberturada",
        "UGEL Fortalecida",
        "Tasa Cobertura DRE/GRE",
        "Tasa Cobertura UGEL",
        "Tasa DRE/GRE Fortalecida",
        "Tasa UGEL Fortalecida",
        "Tasa Retencion",
        "Tasa Finalizacion",
        "Tasa Certificacion",
    ]
    result = kpis[[column for column in ordered_columns if column in kpis.columns]]
    result = result.assign(_blank=result["Region"].eq("")).sort_values(["_blank", "Region"], na_position="last").drop(columns="_blank").reset_index(drop=True)
    return result


def _calculate_iged_kpis(oferta_filtrada: pd.DataFrame, bbdd: pd.DataFrame, iged: pd.DataFrame) -> pd.DataFrame:
    kpis, merged = _calculate_base_kpis(oferta_filtrada, bbdd, iged, ["region", "nombre_iged", "tipo_iged"])
    if kpis.empty:
        return kpis

    capacidades = merged.groupby(["region", "nombre_iged", "tipo_iged"], dropna=False)["codigo"].nunique().reset_index(name="Capacitacion")
    kpis = kpis.merge(capacidades, on=["region", "nombre_iged", "tipo_iged"], how="left")
    kpis["tipo_iged_norm"] = _normalize_iged_type(kpis.get("tipo_iged", pd.Series(dtype=str)))
    kpis["Tasa Cobertura DRE/GRE"] = kpis.apply(lambda row: 1.0 if row["tipo_iged_norm"] == "DRE/GRE" and row["DRE/GRE Coberturada"] > 0 else pd.NA, axis=1)
    kpis["Tasa Cobertura UGEL"] = kpis.apply(lambda row: 1.0 if row["tipo_iged_norm"] == "UGEL" and row["UGEL Coberturada"] > 0 else pd.NA, axis=1)
    kpis["Tasa DRE/GRE Fortalecida"] = kpis["DRE/GRE Fortalecida"] / kpis["DRE/GRE Coberturada"].replace(0, pd.NA)
    kpis["Tasa UGEL Fortalecida"] = kpis["UGEL Fortalecida"] / kpis["UGEL Coberturada"].replace(0, pd.NA)
    kpis = kpis.rename(columns={"region": "Region", "nombre_iged": "IGED", "tipo_iged": "Tipo IGED"})
    ordered_columns = [
        "Region",
        "IGED",
        "Tipo IGED",
        "Capacitacion",
        "Postulaciones",
        "Participaciones",
        "Finalizaciones",
        "Certificaciones",
        "DRE/GRE Coberturada",
        "DRE/GRE Fortalecida",
        "UGEL Coberturada",
        "UGEL Fortalecida",
        "Tasa Cobertura DRE/GRE",
        "Tasa Cobertura UGEL",
        "Tasa DRE/GRE Fortalecida",
        "Tasa UGEL Fortalecida",
        "Tasa Retencion",
        "Tasa Finalizacion",
        "Tasa Certificacion",
    ]
    result = kpis[[column for column in ordered_columns if column in kpis.columns]]
    blank_region = result["Region"].eq("") if "Region" in result.columns else pd.Series(False, index=result.index)
    blank_iged = result["IGED"].eq("") if "IGED" in result.columns else pd.Series(False, index=result.index)
    result = result.assign(_br=blank_region, _bi=blank_iged).sort_values(["_br", "Region", "_bi", "IGED"], na_position="last").drop(columns=["_br", "_bi"]).reset_index(drop=True)
    return result


def _calculate_dni_tables(oferta_filtrada: pd.DataFrame, bbdd: pd.DataFrame, dni_query: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    merged = pd.merge(bbdd, oferta_filtrada, on="codigo", how="inner", suffixes=("_b", "_o"))
    if merged.empty:
        return pd.DataFrame(), pd.DataFrame()

    merged["Proceso Formativo"] = _build_process_label(merged)
    merged["Participante"] = (merged.get("apellidos", "").fillna("").astype(str).str.strip() + " " + merged.get("nombres", "").fillna("").astype(str).str.strip()).str.strip()
    merged["certificado_flag"] = _to_flag(merged.get("aprobados_certificados", pd.Series(dtype=object)))
    if dni_query:
        mask = merged.get("dni", "").fillna("").astype(str).str.contains(dni_query, case=False, na=False)
        merged = merged[mask]
    if merged.empty:
        return pd.DataFrame(), pd.DataFrame()

    participantes = merged.groupby("dni", dropna=False).agg(
        Participante=("Participante", "first"),
        Region=("region", "first"),
        IGED=("nombre_iged", "first"),
        Email=("email", "first"),
        Capacitaciones=("codigo", "nunique"),
        Certificaciones=("certificado_flag", "sum"),
    ).reset_index().rename(columns={"dni": "DNI"})

    detalle = merged[["dni", "Participante", "codigo", "Proceso Formativo", "estado", "compromiso", "region", "nombre_iged", "certificado_flag"]].copy()
    detalle = detalle.rename(columns={"dni": "DNI", "codigo": "Codigo", "estado": "Estado", "compromiso": "Compromiso", "region": "Region", "nombre_iged": "IGED", "certificado_flag": "Certificado"})
    return participantes, detalle


def _format_cell(column: str, value: Any) -> str:
    if pd.isna(value):
        return "-"
    if column == "Efectividad":
        return "Si" if int(value) == 1 else "No"
    if column == "Certificado":
        return "Si" if int(value) == 1 else "No"
    if column in RATE_COLUMNS:
        try:
            return f"{float(value) * 100:.2f}%"
        except Exception:
            return "-"
    if column in COUNT_COLUMNS:
        try:
            return f"{int(float(value)):,}".replace(",", ".")
        except Exception:
            return str(value)
    return str(value)


def _table_payload(df: pd.DataFrame, *, limit: int = 200) -> dict[str, Any]:
    if df.empty:
        return {"columns": [], "rows": [], "total_rows": 0, "truncated": False}

    rows = []
    for _, row in df.head(limit).iterrows():
        rows.append([_format_cell(column, row[column]) for column in df.columns])
    return {"columns": list(df.columns), "rows": rows, "total_rows": int(len(df.index)), "truncated": len(df.index) > limit}


def _summary_cards(active_tab: str, merged: pd.DataFrame, iged_df: pd.DataFrame, satisf_global: float | None) -> list[dict[str, str]]:
    """Calcula las tarjetas KPI desde la data cruda filtrada (merged), no desde las tablas agrupadas."""
    if merged.empty:
        return []

    postulaciones = len(merged)
    matriculaciones = int((merged.get("estado_num", pd.Series(dtype=float)) == 2).sum())
    participaciones = int(merged.get("base_participa", pd.Series(dtype=bool)).fillna(False).sum())
    varones = int(merged.get("varones_base", pd.Series(dtype=int)).fillna(0).sum())
    mujeres = int(merged.get("mujeres_base", pd.Series(dtype=int)).fillna(0).sum())
    retiros = int(merged.get("retiros_base", pd.Series(dtype=int)).fillna(0).sum())
    finalizaciones = int(merged.get("finalizados", pd.Series(dtype=int)).fillna(0).sum())
    certificaciones = int(merged.get("cert_base", pd.Series(dtype=int)).fillna(0).sum())
    evaluados = int(merged.get("evaluados_base", pd.Series(dtype=int)).fillna(0).sum())
    progreso = int(merged.get("progreso_base", pd.Series(dtype=int)).fillna(0).sum())

    codigos = merged.get("codigo", pd.Series(dtype=str)).fillna("").astype(str).str.strip()
    num_capacitaciones = int(codigos[codigos != ""].nunique())
    regiones = merged.get("region", pd.Series(dtype=str)).fillna("").astype(str).str.strip()
    num_regiones = int(regiones[regiones != ""].nunique())

    tipo_norm = merged.get("tipo_iged_norm", pd.Series(dtype=str)).fillna("")
    nombre_iged = merged.get("nombre_iged", pd.Series(dtype=str)).fillna("").astype(str).str.strip()
    dre_cobertura = int(nombre_iged[tipo_norm == "DRE/GRE"].nunique())
    ugel_cobertura = int(nombre_iged[tipo_norm == "UGEL"].nunique())
    dre_fortalecida = int(nombre_iged[(tipo_norm == "DRE/GRE") & (merged.get("cert_base", 0) == 1)].nunique())
    ugel_fortalecida = int(nombre_iged[(tipo_norm == "UGEL") & (merged.get("cert_base", 0) == 1)].nunique())

    total_dre, total_ugel = _national_iged_totals(iged_df)
    tasa_retencion = participaciones / matriculaciones if matriculaciones else pd.NA
    tasa_finalizacion = finalizaciones / participaciones if participaciones else pd.NA
    tasa_certificacion = certificaciones / finalizaciones if finalizaciones else pd.NA
    tasa_progreso = progreso / evaluados if evaluados else pd.NA
    tasa_cobertura_dre = dre_cobertura / total_dre if total_dre else pd.NA
    tasa_cobertura_ugel = ugel_cobertura / total_ugel if total_ugel else pd.NA
    tasa_dre_fortalecida = dre_fortalecida / dre_cobertura if dre_cobertura else pd.NA
    tasa_ugel_fortalecida = ugel_fortalecida / ugel_cobertura if ugel_cobertura else pd.NA
    tasa_varones = varones / participaciones if participaciones else pd.NA
    tasa_mujeres = mujeres / participaciones if participaciones else pd.NA
    efectividad = 1 if (finalizaciones > 0 and (certificaciones / finalizaciones) >= 0.6) else 0

    scope_card = {
        "capacitacion": {"label": "Capacitaciones", "value": _format_cell("Capacitacion", num_capacitaciones), "meta": "Procesos formativos con los filtros aplicados"},
        "region": {"label": "Regiones", "value": _format_cell("Capacitacion", num_regiones), "meta": "Ámbitos territoriales con los filtros aplicados"},
        "iged": {"label": "IGED", "value": _format_cell("Capacitacion", dre_cobertura + ugel_cobertura), "meta": "Instancias de gestión con los filtros aplicados"},
    }.get(active_tab)

    cards: list[dict[str, str]] = [scope_card] if scope_card else []

    CARD_DEFS: dict[str, list[tuple[str, str, Any, str]]] = {
        "capacitacion": [
            ("Postulaciones", "Capacitacion", postulaciones, "Total con filtros aplicados"),
            ("Matriculaciones", "Capacitacion", matriculaciones, "Total con filtros aplicados"),
            ("Participaciones", "Capacitacion", participaciones, "Total con filtros aplicados"),
            ("Retiros", "Capacitacion", retiros, "Total con filtros aplicados"),
            ("Finalizaciones", "Capacitacion", finalizaciones, "Total con filtros aplicados"),
            ("Certificaciones", "Capacitacion", certificaciones, "Total con filtros aplicados"),
            ("DRE/GRE Coberturada", "Capacitacion", dre_cobertura, "IGED DRE/GRE con participantes"),
            ("UGEL Coberturada", "Capacitacion", ugel_cobertura, "IGED UGEL con participantes"),
            ("DRE/GRE Fortalecida", "Capacitacion", dre_fortalecida, "IGED DRE/GRE con certificados"),
            ("UGEL Fortalecida", "Capacitacion", ugel_fortalecida, "IGED UGEL con certificados"),
            ("Evaluados", "Capacitacion", evaluados, "Total con filtros aplicados"),
            ("Progreso", "Capacitacion", progreso, "Total con filtros aplicados"),
            ("Tasa Cobertura DRE/GRE", "Tasa Cobertura DRE/GRE", tasa_cobertura_dre, "Coberturadas / Total nacional"),
            ("Tasa Cobertura UGEL", "Tasa Cobertura UGEL", tasa_cobertura_ugel, "Coberturadas / Total nacional"),
            ("Tasa DRE/GRE Fortalecida", "Tasa DRE/GRE Fortalecida", tasa_dre_fortalecida, "Fortalecidas / Coberturadas"),
            ("Tasa UGEL Fortalecida", "Tasa UGEL Fortalecida", tasa_ugel_fortalecida, "Fortalecidas / Coberturadas"),
            ("Tasa Retencion", "Tasa Retencion", tasa_retencion, "Participaciones / Matriculaciones"),
            ("Tasa Finalizacion", "Tasa Finalizacion", tasa_finalizacion, "Finalizaciones / Participaciones"),
            ("Tasa Certificacion", "Tasa Certificacion", tasa_certificacion, "Certificaciones / Finalizaciones"),
            ("Tasa Progreso", "Tasa Progreso", tasa_progreso, "Progreso / Evaluados"),
            ("Tasa Satisfaccion", "Tasa Satisfaccion", satisf_global, "Tasa global de satisfacción"),
            ("Efectividad", "Tasa Finalizacion", tasa_certificacion if finalizaciones else pd.NA, f"{'Si' if efectividad else 'No'} — Cert/Fin {'≥' if efectividad else '<'} 60%"),
        ],
        "region": [
            ("Capacitacion", "Capacitacion", num_capacitaciones, "Total con filtros aplicados"),
            ("Postulaciones", "Capacitacion", postulaciones, "Total con filtros aplicados"),
            ("Matriculaciones", "Capacitacion", matriculaciones, "Total con filtros aplicados"),
            ("Participaciones", "Capacitacion", participaciones, "Total con filtros aplicados"),
            ("Finalizaciones", "Capacitacion", finalizaciones, "Total con filtros aplicados"),
            ("Certificaciones", "Capacitacion", certificaciones, "Total con filtros aplicados"),
            ("DRE/GRE Coberturada", "Capacitacion", dre_cobertura, "Total con filtros aplicados"),
            ("DRE/GRE Fortalecida", "Capacitacion", dre_fortalecida, "Total con filtros aplicados"),
            ("UGEL Coberturada", "Capacitacion", ugel_cobertura, "Total con filtros aplicados"),
            ("UGEL Fortalecida", "Capacitacion", ugel_fortalecida, "Total con filtros aplicados"),
            ("Tasa Cobertura DRE/GRE", "Tasa Cobertura DRE/GRE", tasa_cobertura_dre, "Coberturadas / Total nacional"),
            ("Tasa Cobertura UGEL", "Tasa Cobertura UGEL", tasa_cobertura_ugel, "Coberturadas / Total nacional"),
            ("Tasa DRE/GRE Fortalecida", "Tasa DRE/GRE Fortalecida", tasa_dre_fortalecida, "Fortalecidas / Coberturadas"),
            ("Tasa UGEL Fortalecida", "Tasa UGEL Fortalecida", tasa_ugel_fortalecida, "Fortalecidas / Coberturadas"),
            ("Tasa Retencion", "Tasa Retencion", tasa_retencion, "Participaciones / Matriculaciones"),
            ("Tasa Finalizacion", "Tasa Finalizacion", tasa_finalizacion, "Finalizaciones / Participaciones"),
            ("Tasa Certificacion", "Tasa Certificacion", tasa_certificacion, "Certificaciones / Finalizaciones"),
        ],
        "iged": [
            ("Capacitacion", "Capacitacion", num_capacitaciones, "Total con filtros aplicados"),
            ("Postulaciones", "Capacitacion", postulaciones, "Total con filtros aplicados"),
            ("Participaciones", "Capacitacion", participaciones, "Total con filtros aplicados"),
            ("Finalizaciones", "Capacitacion", finalizaciones, "Total con filtros aplicados"),
            ("Certificaciones", "Capacitacion", certificaciones, "Total con filtros aplicados"),
            ("DRE/GRE Coberturada", "Capacitacion", dre_cobertura, "Total con filtros aplicados"),
            ("DRE/GRE Fortalecida", "Capacitacion", dre_fortalecida, "Total con filtros aplicados"),
            ("UGEL Coberturada", "Capacitacion", ugel_cobertura, "Total con filtros aplicados"),
            ("UGEL Fortalecida", "Capacitacion", ugel_fortalecida, "Total con filtros aplicados"),
            ("Tasa Cobertura DRE/GRE", "Tasa Cobertura DRE/GRE", tasa_cobertura_dre, "Coberturadas / Total nacional"),
            ("Tasa Cobertura UGEL", "Tasa Cobertura UGEL", tasa_cobertura_ugel, "Coberturadas / Total nacional"),
            ("Tasa DRE/GRE Fortalecida", "Tasa DRE/GRE Fortalecida", tasa_dre_fortalecida, "Fortalecidas / Coberturadas"),
            ("Tasa UGEL Fortalecida", "Tasa UGEL Fortalecida", tasa_ugel_fortalecida, "Fortalecidas / Coberturadas"),
            ("Tasa Retencion", "Tasa Retencion", tasa_retencion, "Participaciones / Matriculaciones"),
            ("Tasa Finalizacion", "Tasa Finalizacion", tasa_finalizacion, "Finalizaciones / Participaciones"),
            ("Tasa Certificacion", "Tasa Certificacion", tasa_certificacion, "Certificaciones / Finalizaciones"),
        ],
    }

    for label, fmt_col, value, meta in CARD_DEFS.get(active_tab, []):
        if value is None or value is pd.NA or (isinstance(value, float) and pd.isna(value)):
            continue
        cards.append({"label": label, "value": _format_cell(fmt_col, value), "meta": meta})

    return cards


def _build_dashboard_data(query_data: Any) -> dict[str, Any]:
    """Calcula una sola vez los datasets base del dashboard y sus filtros activos."""
    _getlist = getattr(query_data, "getlist", None)

    oferta, bbdd, satisfaccion, iged = _load_base_tables()
    oferta = oferta.copy()
    oferta.loc[:, "anio_text"] = oferta.get("anio", pd.Series(index=oferta.index, dtype=object)).fillna("").astype(str).str.strip()
    oferta.loc[:, "Proceso Formativo"] = _build_process_label(oferta)

    year_options = _series_options(oferta.get("anio_text", pd.Series(dtype=str)), reverse=True)
    condition_options = _series_options(oferta.get("condicion", pd.Series(dtype=str)))

    current_year = str(datetime.now().year)
    raw_years = _getlist("anio") if _getlist else query_data.get("anio")
    selected_years = _selected_values(raw_years, year_options, current_year if current_year in year_options else "")

    default_condition = "Cerrado" if "Cerrado" in condition_options else ""
    selected_condition = _selected_value(query_data.get("condicion"), condition_options, default_condition)

    fecha_inicio = str(query_data.get("fecha_inicio", "")).strip()
    fecha_fin = str(query_data.get("fecha_fin", "")).strip()

    oferta_for_processes = _filter_offer(oferta, selected_years, selected_condition, [], fecha_inicio, fecha_fin)
    process_options = _series_options(oferta_for_processes.get("Proceso Formativo", pd.Series(dtype=str)))
    raw_processes = _getlist("proceso") if _getlist else query_data.get("proceso")
    selected_processes = _selected_values(raw_processes, process_options, "")

    oferta_filtrada_base = _filter_offer(oferta, selected_years, selected_condition, selected_processes, fecha_inicio, fecha_fin)
    codigos_filtrados = oferta_filtrada_base[["codigo"]].copy() if "codigo" in oferta_filtrada_base.columns else pd.DataFrame(columns=["codigo"])
    participantes_base = pd.merge(bbdd, codigos_filtrados.drop_duplicates(), on="codigo", how="inner") if not codigos_filtrados.empty else pd.DataFrame(columns=bbdd.columns)
    region_options = _series_options(participantes_base.get("region", pd.Series(dtype=str)))
    raw_regions = _getlist("region") if _getlist else query_data.get("region")
    selected_regions = _selected_values(raw_regions, region_options, "")

    participantes_iged = participantes_base.copy()
    if selected_regions and not participantes_iged.empty:
        participantes_iged = participantes_iged[participantes_iged.get("region", "").fillna("").astype(str).str.strip().isin(selected_regions)]
    iged_options = _series_options(participantes_iged.get("nombre_iged", pd.Series(dtype=str)))
    raw_igeds = _getlist("iged") if _getlist else query_data.get("iged")
    selected_igeds = _selected_values(raw_igeds, iged_options, "")

    active_tab = str(query_data.get("vista", "capacitacion")).strip().lower() or "capacitacion"
    if active_tab not in {"capacitacion", "region", "iged"}:
        active_tab = "capacitacion"

    bbdd_filtrada = _filter_participants(participantes_base, selected_regions, selected_igeds)
    oferta_filtrada = oferta_filtrada_base
    if not bbdd_filtrada.empty and (selected_regions or selected_igeds):
        codigos_visibles = bbdd_filtrada.get("codigo", pd.Series(dtype=object)).fillna("").astype(str).str.strip().unique().tolist()
        oferta_filtrada = oferta_filtrada_base[oferta_filtrada_base.get("codigo", pd.Series(dtype=object)).fillna("").astype(str).str.strip().isin(codigos_visibles)]
    elif selected_regions or selected_igeds:
        oferta_filtrada = oferta_filtrada_base.iloc[0:0].copy()

    df_cap, _merged_cap, _sat_global = _calculate_capacitacion_kpis(oferta_filtrada, bbdd_filtrada, satisfaccion, iged)
    df_region = _calculate_region_kpis(oferta_filtrada, bbdd_filtrada, iged)
    df_iged = _calculate_iged_kpis(oferta_filtrada, bbdd_filtrada, iged)

    df_difoca = bbdd_filtrada.copy()
    difoca_columns = [
        "ID", "obs", "numero", "codigo", "tipo_documento", "dni",
        "apellidos", "nombres", "genero", "fecha_nacimiento",
        "telefono_celular", "email", "actualizo_datos",
        "region", "tipo_iged", "codigo_iged", "nombre_iged",
        "ambitos", "nivel_puesto", "nombre_puesto", "regimen_laboral",
        "publico_objetivo", "ultimo_acceso_curso", "dias_ausencia",
        "ingreso_curso", "estado", "compromiso",
        "promedio_final_general", "promedio_final_condicion",
        "avance_curso_certificacion", "estado_participante_curso",
        "situacion_participante", "retiros", "aprobados_certificados",
        "desaprobado_permanente", "desaprobado_abandono",
        "cuestionario_entrada", "cuestionario_salida",
        "encuesta", "ev_progreso_aprendizaje", "mantuvo_o_progreso",
        "progreso", "nivel_c_entrada", "nivel_c_salida", "telefono_fijo",
    ]
    df_difoca = df_difoca[[c for c in difoca_columns if c in df_difoca.columns]]

    tabs = [
        {"slug": "capacitacion", "title": "Por Capacitacion"},
        {"slug": "region", "title": "Por Region"},
        {"slug": "iged", "title": "Por IGED"},
    ]

    dataframes = {
        "capacitacion": df_cap,
        "region": df_region,
        "iged": df_iged,
        "difoca": df_difoca,
    }

    return {
        "filters": {
            "year_options": year_options,
            "condition_options": condition_options,
            "process_options": process_options,
            "region_options": region_options,
            "iged_options": iged_options,
            "selected_years": selected_years,
            "selected_condition": selected_condition,
            "selected_processes": selected_processes,
            "selected_regions": selected_regions,
            "selected_igeds": selected_igeds,
            "fecha_inicio": fecha_inicio,
            "fecha_fin": fecha_fin,
        },
        "tabs": tabs,
        "active_tab": active_tab,
        "summary_cards": _summary_cards(active_tab, _merged_cap, iged, _sat_global),
        "dataframes": dataframes,
    }


def _excel_bytes(sheets: list[tuple[str, pd.DataFrame]]) -> bytes:
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        for sheet_name, dataframe in sheets:
            export_df = dataframe.copy()
            export_df.to_excel(writer, index=False, sheet_name=sheet_name[:31] or "Datos")
    buffer.seek(0)
    return buffer.getvalue()


def build_indicadores_download(query_data: Any, download_kind: str, download_format: str) -> dict[str, Any] | None:
    """Construye un archivo descargable para el dashboard de indicadores."""
    raw_kinds = str(download_kind or "").strip().lower()
    export_format = str(download_format or "xlsx").strip().lower()
    dashboard = _build_dashboard_data(query_data)
    dataframes = dashboard.get("dataframes", {})

    VALID_KINDS = {"capacitacion", "region", "iged", "difoca"}
    SHEET_NAMES = {
        "capacitacion": "Por Capacitacion",
        "region": "Por Region",
        "iged": "Por IGED",
        "difoca": "Base DIFOCA",
    }

    kinds = [k.strip() for k in raw_kinds.split(",") if k.strip() in VALID_KINDS]
    if not kinds:
        return None
    if export_format not in {"xlsx", "csv"}:
        return None

    if len(kinds) == 1:
        kind = kinds[0]
        dataframe = dataframes.get(kind, pd.DataFrame())
        if export_format == "xlsx":
            payload = _excel_bytes([(SHEET_NAMES.get(kind, kind.title()), dataframe)])
            return {
                "content": payload,
                "content_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                "filename": f"indicadores_{kind}.xlsx",
            }
        return {
            "content": dataframe.to_csv(index=False).encode("utf-8-sig"),
            "content_type": "text/csv; charset=utf-8",
            "filename": f"indicadores_{kind}.csv",
        }

    sheets = [(SHEET_NAMES.get(k, k.title()), dataframes.get(k, pd.DataFrame())) for k in kinds]
    payload = _excel_bytes(sheets)
    return {
        "content": payload,
        "content_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "filename": "indicadores_descarga.xlsx",
    }


def build_indicadores_dashboard_context(query_data: Any) -> dict[str, Any]:
    """Construye el contexto Django para el dashboard de indicadores."""
    dashboard = _build_dashboard_data(query_data)
    dataframes = dashboard.get("dataframes", {})

    return {
        "indicadores_filters": dashboard.get("filters", {}),
        "indicadores_tabs": dashboard.get("tabs", []),
        "indicadores_active_tab": dashboard.get("active_tab", "capacitacion"),
        "indicadores_summary_cards": dashboard.get("summary_cards", []),
        "indicadores_table_capacitacion": _table_payload(dataframes.get("capacitacion", pd.DataFrame())),
        "indicadores_table_region": _table_payload(dataframes.get("region", pd.DataFrame())),
        "indicadores_table_iged": _table_payload(dataframes.get("iged", pd.DataFrame())),
        "indicadores_data_state": {
            "total_capacitaciones": int(len(dataframes.get("capacitacion", pd.DataFrame()).index)),
            "total_regiones": int(len(dataframes.get("region", pd.DataFrame()).index)),
            "total_iged": int(len(dataframes.get("iged", pd.DataFrame()).index)),
            "total_participantes": int(len(dataframes.get("dni", pd.DataFrame()).index)),
        },
    }