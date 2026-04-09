"""
Modulo para procesamiento de datos de formularios.
Replica la funcionalidad de herramientas/procesamiento_datos.py (app Streamlit).
"""

from io import BytesIO
from typing import Any
import re
import difflib

import pandas as pd
import numpy as np
import openpyxl
from openpyxl.styles import Font


# ---------------------------------------------------------------------------
# 1. Limpieza de formularios
# ---------------------------------------------------------------------------

def limpiar_respuestas_preguntas(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].astype(str)
            df[col] = df[col].str.replace("_x000D_", "", regex=False)
            df[col] = df[col].str.replace("\r", "", regex=False)
            df[col] = df[col].str.replace("\n", "", regex=False)
            df[col] = df[col].str.replace(r"[\s\u00A0\t]+", " ", regex=True)
            df[col] = df[col].str.strip()
            df[col] = df[col].str.replace(r"^\d+\.\s*", "", regex=True)
            df[col] = df[col].str.replace(r"^[a-zA-Z]\)\s*", "", regex=True)
            df[col] = df[col].str.replace(r"^[a-zA-Z]\.\s*", "", regex=True)
            df[col] = df[col].str.rstrip(".")
            df[col] = df[col].str.strip()
    return df


def procesar_archivo_individual(df: pd.DataFrame) -> pd.DataFrame:
    from copy import deepcopy

    df_procesado = deepcopy(df)

    columnas_a_eliminar = [
        col for col in df_procesado.columns
        if str(col).startswith("Puntos:") or str(col).startswith("Comentarios:")
    ]

    columnas_iged = [
        col for col in df_procesado.columns
        if str(col).startswith("Indique la IGED a la que pertenece")
    ]

    df_procesado = limpiar_respuestas_preguntas(df_procesado)

    if columnas_iged:
        df_procesado["IGED"] = ""
        for index, row in df_procesado.iterrows():
            valores_iged = []
            for col_iged in columnas_iged:
                valor = str(row[col_iged]) if pd.notna(row[col_iged]) else ""
                if valor and valor != "nan" and valor.strip():
                    valores_iged.append(valor.strip())
            df_procesado.at[index, "IGED"] = "; ".join(set(valores_iged)) if valores_iged else ""
        columnas_a_eliminar.extend(columnas_iged)

    if columnas_a_eliminar:
        df_procesado = df_procesado.drop(columns=columnas_a_eliminar, errors="ignore")

    # Eliminar columnas completamente vacias
    columnas_vacias = []
    for col in df_procesado.columns:
        serie_str = df_procesado[col].astype(str).str.strip()
        if (
            df_procesado[col].isna().all()
            or (serie_str == "").all()
            or (serie_str == "nan").all()
            or (serie_str == "None").all()
        ):
            columnas_vacias.append(col)
    if columnas_vacias:
        df_procesado = df_procesado.drop(columns=columnas_vacias, errors="ignore")

    # Renombrar columnas conocidas
    if "Indique la Región a la que pertenece" in df_procesado.columns:
        df_procesado = df_procesado.rename(
            columns={"Indique la Región a la que pertenece": "REGION"}
        )
    if "Total de puntos" in df_procesado.columns:
        df_procesado = df_procesado.rename(columns={"Total de puntos": "NOTA"})

    # Reordenar IGED despues de REGION
    if "REGION" in df_procesado.columns and "IGED" in df_procesado.columns:
        cols = list(df_procesado.columns)
        cols.remove("IGED")
        pos = cols.index("REGION")
        cols.insert(pos + 1, "IGED")
        df_procesado = df_procesado[cols]

    # Eliminar columnas adicionales
    for col_del in ("Correo electrónico", "Nombre"):
        if col_del in df_procesado.columns:
            df_procesado = df_procesado.drop(columns=[col_del], errors="ignore")

    # Formatear DNI
    columna_dni = None
    if "Documento de Identidad" in df_procesado.columns:
        columna_dni = "Documento de Identidad"
    elif "DNI" in df_procesado.columns:
        columna_dni = "DNI"
    if columna_dni:
        if columna_dni != "DNI":
            df_procesado = df_procesado.rename(columns={columna_dni: "DNI"})
        df_procesado["DNI"] = df_procesado["DNI"].astype(str).str.replace("O", "0").str.replace("o", "0")

        def formatear_dni(dni):
            if pd.isna(dni):
                return ""
            dni_str = str(dni).strip()
            if "." in dni_str:
                dni_str = dni_str.split(".")[0]
            return dni_str.zfill(8) if dni_str.isdigit() else dni_str

        df_procesado["DNI"] = df_procesado["DNI"].apply(formatear_dni)

    # Eliminar filas con DNI duplicado
    if "DNI" in df_procesado.columns and "Hora de inicio" in df_procesado.columns:
        try:
            df_procesado["Hora de inicio"] = pd.to_datetime(
                df_procesado["Hora de inicio"], errors="coerce"
            )
            mask_valido = (df_procesado["DNI"] != "") & df_procesado["DNI"].notna()
            df_con = df_procesado[mask_valido]
            df_sin = df_procesado[~mask_valido]
            df_con = df_con.sort_values("Hora de inicio").drop_duplicates(
                subset=["DNI"], keep="first"
            )
            df_procesado = pd.concat([df_con, df_sin], ignore_index=True)
            df_procesado = df_procesado.sort_values("Hora de inicio").reset_index(drop=True)
        except Exception:
            pass

    return df_procesado


def limpiar_y_exportar(file_bytes: bytes, filename: str) -> tuple[bytes, str]:
    df = pd.read_excel(BytesIO(file_bytes))
    df_limpio = procesar_archivo_individual(df)
    buf = BytesIO()
    df_limpio.to_excel(buf, index=False)
    buf.seek(0)
    stem = filename.rsplit(".", 1)[0] if "." in filename else filename
    return buf.getvalue(), f"{stem}_limpio.xlsx"


def limpiar_multiples_y_exportar(archivos: list[tuple[bytes, str]]) -> tuple[bytes, str, str]:
    """Limpia multiples archivos. Si es 1, devuelve Excel directo. Si son varios, devuelve ZIP."""
    resultados = []
    for raw, name in archivos:
        data, fname = limpiar_y_exportar(raw, name)
        resultados.append((data, fname))

    if len(resultados) == 1:
        data, fname = resultados[0]
        return data, fname, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

    import zipfile
    zip_buf = BytesIO()
    with zipfile.ZipFile(zip_buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for data, fname in resultados:
            zf.writestr(fname, data)
    zip_buf.seek(0)
    return zip_buf.getvalue(), "formularios_limpios.zip", "application/zip"


# ---------------------------------------------------------------------------
# 2. Transposicion de datos
# ---------------------------------------------------------------------------

def transponer_a_largo(
    df: pd.DataFrame,
    columnas_id: list[str] | None = None,
    columnas_preguntas: list[str] | None = None,
) -> pd.DataFrame:
    if columnas_id is None:
        columnas_id = []
    if columnas_preguntas is None:
        columnas_preguntas = [c for c in df.columns if c not in columnas_id]
    return pd.melt(
        df,
        id_vars=columnas_id,
        value_vars=columnas_preguntas,
        var_name="Pregunta",
        value_name="Respuesta",
    )


def transponer_a_ancho(
    df: pd.DataFrame,
    col_pregunta: str,
    col_respuesta: str,
    columnas_id: list[str] | None = None,
) -> pd.DataFrame:
    df_trabajo = df.copy()
    if columnas_id:
        df_trans = df_trabajo[columnas_id + [col_pregunta, col_respuesta]].copy()
        df_trans["ID_UNICO"] = df_trans[columnas_id].astype(str).agg("_".join, axis=1)
        df_transpuesto = df_trans.pivot_table(
            index="ID_UNICO", columns=col_pregunta, values=col_respuesta, aggfunc="first"
        ).reset_index()
        df_ids = df_trans[["ID_UNICO"] + columnas_id].drop_duplicates()
        df_transpuesto = df_transpuesto.merge(df_ids, on="ID_UNICO", how="left")
        conservadas = ["ID_UNICO"] + columnas_id
        resto = [c for c in df_transpuesto.columns if c not in conservadas]
        df_transpuesto = df_transpuesto[conservadas + resto]
    else:
        df_trans = df_trabajo[[col_pregunta, col_respuesta]].copy()
        df_trans["PARTICIPANTE"] = df_trans.groupby(col_pregunta).cumcount() + 1
        df_transpuesto = df_trans.pivot_table(
            index="PARTICIPANTE", columns=col_pregunta, values=col_respuesta, aggfunc="first"
        ).reset_index()
    df_transpuesto.columns.name = None
    return df_transpuesto


def transponer_y_exportar(
    file_bytes: bytes,
    filename: str,
    tipo: str,
    col_pregunta: str = "",
    col_respuesta: str = "",
    columnas_id: list[str] | None = None,
    columnas_preguntas: list[str] | None = None,
) -> tuple[bytes, str]:
    df = pd.read_excel(BytesIO(file_bytes))
    if tipo == "largo_a_ancho":
        df_result = transponer_a_ancho(df, col_pregunta, col_respuesta, columnas_id or None)
        sufijo = "transpuesto"
    else:
        df_result = transponer_a_largo(df, columnas_id or None, columnas_preguntas or None)
        sufijo = "largo"
    buf = BytesIO()
    df_result.to_excel(buf, index=False)
    buf.seek(0)
    stem = filename.rsplit(".", 1)[0] if "." in filename else filename
    return buf.getvalue(), f"{stem}_{sufijo}.xlsx"


def obtener_columnas_excel(file_bytes: bytes) -> list[str]:
    df = pd.read_excel(BytesIO(file_bytes), nrows=0)
    return list(df.columns)


# ---------------------------------------------------------------------------
# 3. Alpha de Cronbach
# ---------------------------------------------------------------------------

def calcular_alpha_cronbach_completo(
    file_bytes_list: list[bytes],
    columnas_analizar: list[str],
    mapeo_columnas: dict[str, str] | None = None,
) -> dict[str, Any]:
    dfs = []
    for fb in file_bytes_list:
        df = pd.read_excel(BytesIO(fb))
        df = limpiar_respuestas_preguntas(df)
        dfs.append(df)

    # Aplicar mapeo si hay 2 archivos
    if len(dfs) == 2 and mapeo_columnas:
        dfs[1] = dfs[1].rename(columns=mapeo_columnas)

    # Unir y filtrar columnas
    df_unido = pd.concat([df[columnas_analizar] for df in dfs], ignore_index=True)
    df_unido = limpiar_respuestas_preguntas(df_unido)

    # Recodificar categoricas
    detalles: dict[str, dict] = {}
    for col in df_unido.columns:
        if not pd.api.types.is_numeric_dtype(df_unido[col]):
            df_unido[col] = df_unido[col].replace("nan", np.nan)
            mask = df_unido[col].notna()
            recod, uniques = pd.factorize(df_unido.loc[mask, col], sort=True)
            recodificada = pd.Series(np.nan, index=df_unido.index)
            recodificada[mask] = recod + 1
            df_unido[col] = recodificada
            vals_orig = [v for v in list(uniques) if pd.notna(v)]
            detalles[col] = {"tipo": "Recodificada", "valores_unicos": len(vals_orig), "valores_originales": vals_orig}
        else:
            detalles[col] = {"tipo": "Numérica", "valores_unicos": int(df_unido[col].nunique()), "valores_originales": "Numérica"}

    def _alpha(df_calc):
        k = df_calc.shape[1]
        var_items = df_calc.var(axis=0, ddof=1)
        var_total = df_calc.sum(axis=1).var(ddof=1)
        if k > 1 and var_total > 0:
            return (k / (k - 1)) * (1 - var_items.sum() / var_total)
        return float("nan")

    resultados = []

    # Listwise
    df_lw = df_unido.dropna()
    resultados.append({"metodo": "Excluir casos con vacíos (listwise)", "alpha": _alpha(df_lw)})

    # Imputar media
    df_media = df_unido.copy()
    for col in df_media.columns:
        if pd.api.types.is_numeric_dtype(df_media[col]):
            df_media[col] = df_media[col].fillna(df_media[col].mean())
    resultados.append({"metodo": "Imputar vacíos por la media del ítem", "alpha": _alpha(df_media)})

    # Pairwise
    corr = df_unido.corr(numeric_only=True)
    k_pw = corr.shape[0]
    if k_pw > 1:
        mean_corr = corr.values[np.triu_indices(k_pw, 1)].mean()
        alpha_pw = (k_pw * mean_corr) / (1 + (k_pw - 1) * mean_corr)
    else:
        alpha_pw = float("nan")
    resultados.append({"metodo": "Excluir por ítem (pairwise)", "alpha": alpha_pw})

    # Sin tratar
    resultados.append({"metodo": "No tratar los datos vacíos", "alpha": _alpha(df_unido)})

    return {"resultados": resultados, "detalles": detalles, "n_filas": len(df_unido), "n_items": df_unido.shape[1]}


def exportar_alpha_excel(
    file_bytes_list: list[bytes],
    columnas_analizar: list[str],
    metodo: str,
    mapeo_columnas: dict[str, str] | None = None,
) -> bytes:
    dfs = []
    for fb in file_bytes_list:
        df = pd.read_excel(BytesIO(fb))
        df = limpiar_respuestas_preguntas(df)
        dfs.append(df)

    if len(dfs) == 2 and mapeo_columnas:
        dfs[1] = dfs[1].rename(columns=mapeo_columnas)

    df_unido = pd.concat([df[columnas_analizar] for df in dfs], ignore_index=True)
    df_unido = limpiar_respuestas_preguntas(df_unido)

    detalles: dict[str, dict] = {}
    for col in df_unido.columns:
        if not pd.api.types.is_numeric_dtype(df_unido[col]):
            df_unido[col] = df_unido[col].replace("nan", np.nan)
            mask = df_unido[col].notna()
            recod, uniques = pd.factorize(df_unido.loc[mask, col], sort=True)
            recodificada = pd.Series(np.nan, index=df_unido.index)
            recodificada[mask] = recod + 1
            df_unido[col] = recodificada
            vals_orig = [v for v in list(uniques) if pd.notna(v)]
            detalles[col] = {"tipo": "Recodificada", "valores_unicos": len(vals_orig)}
        else:
            detalles[col] = {"tipo": "Numérica", "valores_unicos": int(df_unido[col].nunique())}

    def _alpha(df_calc):
        k = df_calc.shape[1]
        vi = df_calc.var(axis=0, ddof=1)
        vt = df_calc.sum(axis=1).var(ddof=1)
        if k > 1 and vt > 0:
            return (k / (k - 1)) * (1 - vi.sum() / vt), vi, vt
        return float("nan"), vi, vt

    if metodo == "listwise":
        df_export = df_unido.dropna()
    elif metodo == "media":
        df_export = df_unido.copy()
        for col in df_export.columns:
            if pd.api.types.is_numeric_dtype(df_export[col]):
                df_export[col] = df_export[col].fillna(df_export[col].mean())
    else:
        df_export = df_unido.copy()

    is_pairwise = metodo == "pairwise"

    if is_pairwise:
        corr = df_export.corr(numeric_only=True)
        k = corr.shape[0]
        if k > 1:
            mc = corr.values[np.triu_indices(k, 1)].mean()
            alpha = (k * mc) / (1 + (k - 1) * mc)
        else:
            alpha = float("nan")
        var_items = None
        var_total = None
    else:
        alpha, var_items, var_total = _alpha(df_export)
        k = df_export.shape[1]

    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df_export.to_excel(writer, sheet_name="Datos_Alpha", index=False)
        df_export.corr(numeric_only=True).to_excel(writer, sheet_name="Correlacion", index=True)

        vacios = df_export.isnull().sum()
        total = len(df_export)
        porcentaje = (vacios / total * 100).round(2)
        pd.DataFrame({"Vacíos": vacios, "% Vacíos": porcentaje}).to_excel(
            writer, sheet_name="Vacios", index=True
        )

        rows_res = [["Sección", "Parámetro", "Valor"]]
        rows_res.append(["Resultados", "Alpha de Cronbach", f"{alpha:.4f}" if not pd.isna(alpha) else "N/A"])
        rows_res.append(["Resultados", "N° de ítems", str(k)])

        if not is_pairwise and var_total is not None:
            rows_res.append(["Resultados", "Varianza total", f"{var_total:.4f}"])
            df_export.var(numeric_only=True).rename("Varianza").to_frame().to_excel(
                writer, sheet_name="Varianza", index=True
            )
            # Alpha sin cada item
            rows_res.append(["", "", ""])
            rows_res.append(["Alpha sin Ítem", "Columna eliminada", "Alpha resultante"])
            for col_rm in df_export.columns:
                df_tmp = df_export.drop(columns=[col_rm])
                if df_tmp.shape[1] > 1:
                    a_tmp, _, _ = _alpha(df_tmp)
                    rows_res.append(["Alpha sin Ítem", col_rm, f"{a_tmp:.4f}" if not pd.isna(a_tmp) else "N/A"])

        for col_name, info in detalles.items():
            rows_res.append(["Recodificación", col_name, str(info)])

        pd.DataFrame(rows_res[1:], columns=rows_res[0]).to_excel(
            writer, sheet_name="Resultados_Alpha", index=False
        )

    buf.seek(0)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# 4. Comparativo CE vs CS
# ---------------------------------------------------------------------------

def normalizar_nombre_columna(nombre: str) -> str:
    nombre = str(nombre).upper()
    nombre = re.sub(r"[^A-Z0-9 ]", "", nombre)
    nombre = re.sub(r"\s+", " ", nombre).strip()
    return nombre


def calcular_similitud_preguntas(p1: str, p2: str) -> float:
    n1 = normalizar_nombre_columna(p1)
    n2 = normalizar_nombre_columna(p2)
    if n1 == n2:
        return 1.0
    comunes = {"DE", "LA", "EL", "EN", "UN", "UNA", "CON", "POR", "PARA", "QUE", "DEL", "LOS", "LAS", "ES", "SE", "SU", "SUS", "AL", "O", "Y", "A"}
    w1 = set(n1.split()) - comunes
    w2 = set(n2.split()) - comunes
    if not w1 or not w2:
        return 0.0
    inter = len(w1 & w2)
    union = len(w1 | w2)
    return inter / union if union > 0 else 0.0


def emparejar_preguntas_auto(cols_entrada: list[str], cols_salida: list[str]) -> list[dict]:
    emps = []
    disponibles = cols_salida.copy()
    for ce in cols_entrada:
        mejor = None
        mejor_sim = 0.0
        for cs in disponibles:
            sim = calcular_similitud_preguntas(ce, cs)
            if sim > mejor_sim and sim > 0.6:
                mejor_sim = sim
                mejor = cs
        if mejor:
            emps.append({"entrada": ce, "salida": mejor, "similitud": round(mejor_sim, 2)})
            disponibles.remove(mejor)
    return emps


def limpiar_categoria(cat) -> str:
    if not isinstance(cat, str):
        return str(cat)
    cat = cat.strip().lower()
    cat = cat.replace("á", "a").replace("é", "e").replace("í", "i").replace("ó", "o").replace("ú", "u")
    cat = cat.replace("ü", "u").replace("ñ", "n")
    cat = " ".join(cat.split())
    return cat


def agrupar_categorias_similares(categorias: list[str], umbral: float = 0.85) -> list[list[str]]:
    grupos: list[list[str]] = []
    usados: set[int] = set()
    for i, c1 in enumerate(categorias):
        if i in usados:
            continue
        grupo = [c1]
        usados.add(i)
        for j, c2 in enumerate(categorias):
            if j != i and j not in usados:
                if difflib.SequenceMatcher(None, c1, c2).ratio() >= umbral:
                    grupo.append(c2)
                    usados.add(j)
        grupos.append(grupo)
    return grupos


def realizar_comparacion(
    file_bytes_entrada: bytes,
    file_bytes_salida: bytes,
    emparejamientos: list[dict],
) -> tuple[list[dict], bytes]:
    df_entrada = pd.read_excel(BytesIO(file_bytes_entrada))
    df_salida = pd.read_excel(BytesIO(file_bytes_salida))

    resultados = []
    for emp in emparejamientos:
        col_e = emp["entrada"]
        col_s = emp["salida"]
        datos_e = df_entrada[col_e].dropna()
        datos_s = df_salida[col_s].dropna()

        r: dict[str, Any] = {
            "pregunta_entrada": col_e,
            "pregunta_salida": col_s,
            "similitud": emp["similitud"],
            "n_entrada": len(datos_e),
            "n_salida": len(datos_s),
            "tipo_entrada": "Numérica" if pd.api.types.is_numeric_dtype(datos_e) else "Categórica",
            "tipo_salida": "Numérica" if pd.api.types.is_numeric_dtype(datos_s) else "Categórica",
        }

        if r["tipo_entrada"] == "Numérica" and r["tipo_salida"] == "Numérica":
            de_num = pd.to_numeric(datos_e, errors="coerce").dropna()
            ds_num = pd.to_numeric(datos_s, errors="coerce").dropna()
            me = de_num.mean() if len(de_num) > 0 else 0
            ms = ds_num.mean() if len(ds_num) > 0 else 0
            dif = ms - me
            mejora = (dif / me * 100) if me != 0 else 0
            if mejora > 5:
                interp = "MEJORA SIGNIFICATIVA"
            elif mejora > 0:
                interp = "MEJORA LEVE"
            elif mejora > -5:
                interp = "SIN CAMBIO SIGNIFICATIVO"
            else:
                interp = "DISMINUCIÓN"
            r.update({"media_entrada": round(me, 2), "media_salida": round(ms, 2), "mejora_porcentual": round(mejora, 1), "interpretacion": interp})
        else:
            dist_e = datos_e.value_counts(normalize=True)
            dist_s = datos_s.value_counts(normalize=True)
            cfe = dist_e.index[0] if len(dist_e) > 0 else "N/A"
            cfs = dist_s.index[0] if len(dist_s) > 0 else "N/A"
            interp = "MISMA CATEGORÍA PREDOMINANTE" if cfe == cfs else "CAMBIO EN CATEGORÍA PREDOMINANTE"
            r.update({
                "categoria_freq_entrada": str(cfe),
                "categoria_freq_salida": str(cfs),
                "porcentaje_entrada": round(dist_e.iloc[0] * 100, 1) if len(dist_e) > 0 else 0,
                "porcentaje_salida": round(dist_s.iloc[0] * 100, 1) if len(dist_s) > 0 else 0,
                "interpretacion": interp,
            })

        resultados.append(r)

    # Generar Excel
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        # Resumen
        res_data = [
            ["Archivo Entrada", emp.get("nombre_entrada", "entrada.xlsx"), ""],
            ["Archivo Salida", emp.get("nombre_salida", "salida.xlsx"), ""],
            ["Total Preguntas Comparadas", str(len(resultados)), ""],
        ]
        pd.DataFrame(res_data, columns=["Concepto", "Valor", "Nota"]).to_excel(
            writer, sheet_name="Resumen", index=False
        )

        # Datos
        df_entrada.to_excel(writer, sheet_name="Datos_Entrada", index=False)
        df_salida.to_excel(writer, sheet_name="Datos_Salida", index=False)

        # Analisis detallado
        rows_an = []
        for i, r in enumerate(resultados, 1):
            if "mejora_porcentual" in r:
                resultado_txt = f"Mejora: {r['mejora_porcentual']:.1f}%"
            else:
                resultado_txt = f"{r.get('categoria_freq_entrada', '')} → {r.get('categoria_freq_salida', '')}"
            rows_an.append({
                "N°": i,
                "Pregunta_Entrada": r["pregunta_entrada"],
                "Pregunta_Salida": r["pregunta_salida"],
                "Similitud": r["similitud"],
                "N_Entrada": r["n_entrada"],
                "N_Salida": r["n_salida"],
                "Resultado": resultado_txt,
                "Interpretación": r["interpretacion"],
            })
        pd.DataFrame(rows_an).to_excel(writer, sheet_name="Analisis_Detallado", index=False)

    buf.seek(0)
    return resultados, buf.getvalue()
