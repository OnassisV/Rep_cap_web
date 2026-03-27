"""Adaptadores para migrar el dashboard legacy de indicadores a Django."""

from __future__ import annotations

from datetime import datetime
from typing import Any

import pandas as pd

from accounts.db import get_connection


RATE_COLUMNS = {
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
            "codigo",
            "dni",
            "nombres",
            "apellidos",
            "email",
            "telefono_celular",
            "estado",
            "compromiso",
            "aprobados_certificados",
            "desaprobado_permanente",
            "cuestionario_entrada",
            "cuestionario_salida",
            "region",
            "nombre_iged",
            "tipo_iged",
            "retiros",
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


def _selected_values(values: list[str], options: list[str], default: list[str]) -> list[str]:
    filtered = [value for value in values if value in options]
    return filtered or default


def _filter_offer(
    oferta: pd.DataFrame,
    selected_years: list[str],
    selected_conditions: list[str],
    selected_processes: list[str],
    fecha_inicio: str,
    fecha_fin: str,
) -> pd.DataFrame:
    filtered = oferta.copy()
    filtered.loc[:, "anio_text"] = filtered.get("anio", pd.Series(index=filtered.index, dtype=object)).fillna("").astype(str).str.strip()
    filtered.loc[:, "Proceso Formativo"] = _build_process_label(filtered)

    if selected_years:
        filtered = filtered[filtered["anio_text"].isin(selected_years)]
    if selected_conditions:
        filtered = filtered[filtered.get("condicion", "").fillna("").astype(str).isin(selected_conditions)]
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
    merged["estado_num"] = pd.to_numeric(merged.get("estado"), errors="coerce")
    merged["compromiso_num"] = pd.to_numeric(merged.get("compromiso"), errors="coerce")
    merged["aprobados_certificados_flag"] = _to_flag(merged.get("aprobados_certificados", pd.Series(dtype=object)))
    merged["desaprobado_permanente_flag"] = _to_flag(merged.get("desaprobado_permanente", pd.Series(dtype=object)))
    merged["retiros_flag"] = _to_flag(merged.get("retiros", pd.Series(dtype=object)))

    merged["base_participa"] = (merged["estado_num"] == 2) & (merged["compromiso_num"].isin([20, 1]))
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

    kpis = merged.groupby(agrupadores, dropna=False).agg(
        Postulaciones=("dni", "count"),
        Matriculaciones=("estado_num", lambda values: int((values == 2).sum())),
        Participaciones=("base_participa", lambda values: int(values.fillna(False).sum())),
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
        "Evaluados",
        "Progreso",
        "Tasa Retencion",
        "Tasa Finalizacion",
        "Tasa Certificacion",
        "Tasa Progreso",
        "Tasa Satisfaccion",
        "Efectividad",
    ]
    return kpis[[column for column in ordered_columns if column in kpis.columns]], merged, satisf_global


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
        "UGEL Coberturada",
        "Tasa Cobertura DRE/GRE",
        "Tasa Cobertura UGEL",
        "Tasa Retencion",
        "Tasa Finalizacion",
        "Tasa Certificacion",
    ]
    return kpis[[column for column in ordered_columns if column in kpis.columns]]


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
        "Tasa Cobertura DRE/GRE",
        "Tasa Cobertura UGEL",
        "Tasa DRE/GRE Fortalecida",
        "Tasa UGEL Fortalecida",
        "Tasa Retencion",
        "Tasa Finalizacion",
        "Tasa Certificacion",
    ]
    return kpis[[column for column in ordered_columns if column in kpis.columns]]


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


def _summary_cards(df_capacitacion: pd.DataFrame, sat_global: float | None) -> list[dict[str, str]]:
    if df_capacitacion.empty:
        return []

    matriculaciones = df_capacitacion.get("Matriculaciones", pd.Series(dtype=float)).sum()
    participaciones = df_capacitacion.get("Participaciones", pd.Series(dtype=float)).sum()
    retencion = participaciones / matriculaciones if matriculaciones else pd.NA
    return [
        {"label": "Capacitaciones", "value": _format_cell("Capacitacion", len(df_capacitacion.index)), "meta": "Procesos visibles con los filtros aplicados"},
        {"label": "Postulaciones", "value": _format_cell("Postulaciones", df_capacitacion.get("Postulaciones", pd.Series(dtype=float)).sum()), "meta": "Registros totales en la base filtrada"},
        {"label": "Participaciones", "value": _format_cell("Participaciones", participaciones), "meta": "Participantes activos con compromiso valido"},
        {"label": "Certificaciones", "value": _format_cell("Certificaciones", df_capacitacion.get("Certificaciones", pd.Series(dtype=float)).sum()), "meta": "Participantes certificados en el conjunto filtrado"},
        {"label": "Retencion global", "value": _format_cell("Tasa Retencion", retencion), "meta": "Relacion entre matriculaciones y participaciones"},
        {"label": "Satisfaccion global", "value": _format_cell("Tasa Satisfaccion", sat_global if sat_global is not None else pd.NA), "meta": "Respuesta satisfecha sobre encuestas visibles"},
    ]


def build_indicadores_dashboard_context(query_data: Any) -> dict[str, Any]:
    """Construye el contexto Django para el dashboard de indicadores."""
    oferta, bbdd, satisfaccion, iged = _load_base_tables()
    oferta = oferta.copy()
    oferta.loc[:, "anio_text"] = oferta.get("anio", pd.Series(index=oferta.index, dtype=object)).fillna("").astype(str).str.strip()
    oferta.loc[:, "Proceso Formativo"] = _build_process_label(oferta)

    year_options = sorted([value for value in oferta.get("anio_text", pd.Series(dtype=str)).dropna().unique().tolist() if str(value).strip()], reverse=True)
    condition_options = sorted([str(value).strip() for value in oferta.get("condicion", pd.Series(dtype=str)).dropna().unique().tolist() if str(value).strip()])

    selected_years_raw = [str(value).strip() for value in query_data.getlist("anios") if str(value).strip()]
    current_year = str(datetime.now().year)
    default_years = [current_year] if current_year in year_options else year_options[:]
    selected_years = _selected_values(selected_years_raw, year_options, default_years)

    selected_conditions_raw = [str(value).strip() for value in query_data.getlist("condiciones") if str(value).strip()]
    default_conditions = ["Cerrado"] if "Cerrado" in condition_options else condition_options[:]
    selected_conditions = _selected_values(selected_conditions_raw, condition_options, default_conditions)

    oferta_for_processes = _filter_offer(oferta, selected_years, selected_conditions, [], "", "")
    process_options = sorted([value for value in oferta_for_processes.get("Proceso Formativo", pd.Series(dtype=str)).dropna().unique().tolist() if str(value).strip()])
    selected_processes_raw = [str(value).strip() for value in query_data.getlist("procesos") if str(value).strip()]
    selected_processes = _selected_values(selected_processes_raw, process_options, process_options[:])

    fecha_inicio = str(query_data.get("fecha_inicio", "")).strip()
    fecha_fin = str(query_data.get("fecha_fin", "")).strip()
    dni_query = str(query_data.get("dni", "")).strip()
    active_tab = str(query_data.get("vista", "capacitacion")).strip().lower() or "capacitacion"
    if active_tab not in {"capacitacion", "region", "iged", "dni"}:
        active_tab = "capacitacion"

    oferta_filtrada = _filter_offer(oferta, selected_years, selected_conditions, selected_processes, fecha_inicio, fecha_fin)
    df_cap, _merged_cap, sat_global = _calculate_capacitacion_kpis(oferta_filtrada, bbdd, satisfaccion, iged)
    df_region = _calculate_region_kpis(oferta_filtrada, bbdd, iged)
    df_iged = _calculate_iged_kpis(oferta_filtrada, bbdd, iged)
    df_participantes, df_detalle = _calculate_dni_tables(oferta_filtrada, bbdd, dni_query)

    tabs = [
        {"slug": "capacitacion", "title": "Por Capacitacion"},
        {"slug": "region", "title": "Por Region"},
        {"slug": "iged", "title": "Por IGED"},
        {"slug": "dni", "title": "Por DNI"},
    ]

    return {
        "indicadores_filters": {
            "year_options": year_options,
            "condition_options": condition_options,
            "process_options": process_options,
            "selected_years": selected_years,
            "selected_conditions": selected_conditions,
            "selected_processes": selected_processes,
            "fecha_inicio": fecha_inicio,
            "fecha_fin": fecha_fin,
            "dni_query": dni_query,
        },
        "indicadores_tabs": tabs,
        "indicadores_active_tab": active_tab,
        "indicadores_summary_cards": _summary_cards(df_cap, sat_global),
        "indicadores_table_capacitacion": _table_payload(df_cap),
        "indicadores_table_region": _table_payload(df_region),
        "indicadores_table_iged": _table_payload(df_iged),
        "indicadores_table_dni": _table_payload(df_participantes, limit=120),
        "indicadores_table_dni_detalle": _table_payload(df_detalle, limit=180),
        "indicadores_data_state": {
            "total_capacitaciones": int(len(df_cap.index)),
            "total_regiones": int(len(df_region.index)),
            "total_iged": int(len(df_iged.index)),
            "total_participantes": int(len(df_participantes.index)),
        },
    }