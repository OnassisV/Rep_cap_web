"""
Adaptador de Satisfacción - Procesa y carga encuestas de satisfacción.

Emula la funcionalidad de app_difoca/herramientas/satisfaccion/procesamiento_satisfaccion.py
pero para Django (no Streamlit).
"""

import re
import html
import unicodedata
import pandas as pd
import numpy as np
from typing import Any
from datetime import datetime
from django.db import connection
from pathlib import Path


# ============================================================
#   UTILIDADES DE LIMPIEZA Y NORMALIZACIÓN
# ============================================================

def limpiar_texto(texto: str | None) -> str | None:
    """Limpia etiquetas HTML y decodifica entidades."""
    if texto is None:
        return None

    texto = str(texto)
    # Remueve tags básicos
    texto = texto.replace("<p>", "").replace("</p>", "")
    # Decodifica entidades
    texto = html.unescape(texto)
    # Remueve cualquier tag restante
    texto = re.sub(r"<[^>]+>", "", texto)
    # Normaliza espacios
    texto = re.sub(r"\s+", " ", texto).strip()

    return texto if texto else None


def _norm_token(texto: str) -> str:
    """Normaliza tokens para detección de patrones."""
    s = str(texto or "").strip().lower()
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"\s+", " ", s)
    return s


def extraer_aspectos(pregunta: str) -> tuple[str | None, str | None, str | None]:
    """
    Extrae (aspecto, aspecto2, pregunta_limpia) desde texto de pregunta.

    Soporta patrones:
    - Aspecto-Subaspecto-Pregunta
    - Aspecto / Subaspecto : Pregunta
    - Aspecto / Subaspecto - Pregunta
    - Aspecto : Pregunta
    """
    if pregunta is None:
        return (None, None, None)

    p = str(pregunta).strip()
    if not p:
        return (None, None, None)

    # Normalizar separadores comunes
    p_norm = re.sub(r"\s+", " ", p)

    # 1) Patrón con ':' (muy común: 'Aspecto / Subaspecto : Pregunta...')
    if ":" in p_norm:
        left, right = [x.strip() for x in p_norm.split(":", 1)]
        aspecto = None
        aspecto2 = None

        if "/" in left:
            a, b = [x.strip() for x in left.split("/", 1)]
            aspecto = a or None
            aspecto2 = b or None
        elif "-" in left:
            partes_left = [x.strip() for x in left.split("-", 1)]
            if len(partes_left) == 2:
                aspecto = partes_left[0] or None
                aspecto2 = partes_left[1] or None
            else:
                aspecto = left or None
        else:
            aspecto = left or None

        pregunta_limpia = right or None
        return (aspecto, aspecto2, pregunta_limpia)

    # 2) Patrón 'Aspecto / Subaspecto - Pregunta...'
    if "/" in p_norm and "-" in p_norm:
        left, right = [x.strip() for x in p_norm.split("-", 1)]
        if "/" in left:
            a, b = [x.strip() for x in left.split("/", 1)]
            return (a or None, b or None, right or None)

    # 3) Patrón con guiones (clásico)
    if "-" in p_norm:
        partes = [x.strip() for x in p_norm.split("-", 2)]
        if len(partes) == 3:
            return (partes[0] or None, partes[1] or None, partes[2] or None)
        if len(partes) == 2:
            return (partes[0] or None, None, partes[1] or None)

    # 4) Solo '/': puede venir sin pregunta separada
    if "/" in p_norm:
        a, b = [x.strip() for x in p_norm.split("/", 1)]
        return (a or None, b or None, None)

    return (None, None, p_norm)


def mapear_satisfaccion(respuesta: str) -> str | None:
    """
    Mapea respuesta de encuesta a categoría de satisfacción.

    Retorna:
    - "Satisfecho"
    - "Ni satisfecho ni insatisfecho"
    - "Insatisfecho"
    - None (si no aplica)
    """
    if respuesta is None:
        return None

    r = str(respuesta).strip().lower()
    r = re.sub(r"\s+", " ", r)
    r_simple = _norm_token(r)

    # Encuestas sí/no con emoji
    if r_simple == "si":
        return "Satisfecho"
    if r_simple == "no":
        return "Insatisfecho"

    mapa = {
        "muy de acuerdo": "Satisfecho",
        "de acuerdo": "Satisfecho",
        "totalmente de acuerdo": "Satisfecho",
        "medianamente de acuerdo": "Ni satisfecho ni insatisfecho",
        "ni de acuerdo ni en desacuerdo": "Ni satisfecho ni insatisfecho",
        "poco de acuerdo": "Insatisfecho",
        "nada de acuerdo": "Insatisfecho",
        "en desacuerdo": "Insatisfecho",
        "muy en desacuerdo": "Insatisfecho",
        "totalmente en desacuerdo": "Insatisfecho",
        "no aplica": None,
        "no aplica.": None,
        "si 😀": "Satisfecho",
        "sí 😀": "Satisfecho",
        "no 😐": "Insatisfecho",
    }

    return mapa.get(r)


def inferir_anio(codigo_cap: str | None) -> int | None:
    """
    Intenta inferir el año desde el código de capacitación.

    Reglas:
    - Si empieza con 4 dígitos tipo 2025 -> 2025
    - Si empieza con 2 dígitos tipo 25 -> 2025
    """
    if not codigo_cap:
        return None

    s = str(codigo_cap).strip()

    if len(s) >= 4 and s[:4].isdigit() and 1900 <= int(s[:4]) <= 2100:
        return int(s[:4])

    if len(s) >= 2 and s[:2].isdigit():
        yy = int(s[:2])
        return 2000 + yy

    return None


# ============================================================
#   PROCESAMIENTO DE ARCHIVOS EXCEL
# ============================================================

def procesar_excel_historico(archivo_excel) -> dict[str, Any]:
    """
    Procesa archivo Excel de histórico de satisfacción.

    Retorna:
    {
        'exito': bool,
        'registros': int,
        'errores': list[str],
        'df': pd.DataFrame,
    }
    """
    try:
        df = pd.read_excel(archivo_excel)
    except Exception as e:
        return {
            'exito': False,
            'registros': 0,
            'errores': [f"No se pudo leer el archivo: {e}"],
            'df': None,
        }

    if df.empty:
        return {
            'exito': False,
            'registros': 0,
            'errores': ["El archivo está vacío"],
            'df': None,
        }

    # Normalización de campos
    required_cols = ['codigo', 'anio', 'dni', 'pregunta', 'respuesta']
    missing_cols = [col for col in required_cols if col not in df.columns]

    if missing_cols:
        return {
            'exito': False,
            'registros': 0,
            'errores': [f"Faltan columnas: {', '.join(missing_cols)}"],
            'df': None,
        }

    df = df.copy()

    # Limpiar y normalizar
    df['codigo'] = df['codigo'].astype(str).str.strip()
    df['anio'] = df['anio'].astype(str).str.strip().astype(int)
    df['pregunta'] = df['pregunta'].apply(limpiar_texto)
    df['respuesta'] = df['respuesta'].apply(limpiar_texto)

    # DNI a 8 dígitos
    df['dni'] = df['dni'].astype(str).str.replace('.0', '', regex=False).str.strip()
    df['dni'] = df['dni'].apply(lambda x: x.zfill(8) if x.isdigit() else x)

    # Extraer aspectos
    tmp = df['pregunta'].apply(extraer_aspectos)
    df['aspecto'] = tmp.apply(lambda x: x[0])
    df['aspecto2'] = tmp.apply(lambda x: x[1])
    df['pregunta'] = tmp.apply(lambda x: x[2])

    # Construir campo aspectos
    df['aspectos'] = df.apply(
        lambda r: (
            f"{r.get('aspecto')} / {r.get('aspecto2')}"
            if r.get('aspecto') and r.get('aspecto2')
            else r.get('aspecto')
        ),
        axis=1
    )

    # Mapear satisfacción
    df['satisfaccion'] = df['respuesta'].apply(mapear_satisfaccion)

    # Nulos
    df = df.replace({np.nan: None})

    return {
        'exito': True,
        'registros': len(df),
        'errores': [],
        'df': df,
    }


# ============================================================
#   GUARDAR EN BASE DE DATOS
# ============================================================

def guardar_en_satisfaccion(df: pd.DataFrame, reemplazar_codigo: str | None = None) -> dict[str, Any]:
    """
    Guarda DataFrame en tabla satisfaccion.

    Si reemplazar_codigo es provided, borra todos los registros con ese código primero.

    Retorna:
    {
        'exito': bool,
        'registros_guardados': int,
        'errores': list[str],
    }
    """
    if df is None or df.empty:
        return {
            'exito': False,
            'registros_guardados': 0,
            'errores': ['DataFrame vacío'],
        }

    # Validaciones
    required = ['codigo', 'anio', 'dni', 'pregunta', 'aspecto', 'satisfaccion', 'aspectos']
    missing = [col for col in required if col not in df.columns]
    if missing:
        return {
            'exito': False,
            'registros_guardados': 0,
            'errores': [f'Faltan columnas: {", ".join(missing)}'],
        }

    try:
        with connection.cursor() as cursor:
            # Si reemplazar, borra primero
            if reemplazar_codigo:
                sql_delete = "DELETE FROM satisfaccion WHERE codigo = %s"
                cursor.execute(sql_delete, [reemplazar_codigo])

            # Prepara datos para inserción
            columns = [col for col in df.columns if col in required + ['respuesta', 'aspecto2']]

            for idx, row in df.iterrows():
                placeholders = ', '.join(['%s'] * len(columns))
                col_names = ', '.join([f'`{col}`' for col in columns])
                sql_insert = f"INSERT INTO satisfaccion ({col_names}) VALUES ({placeholders})"

                values = [row[col] for col in columns]
                cursor.execute(sql_insert, values)

            return {
                'exito': True,
                'registros_guardados': len(df),
                'errores': [],
            }

    except Exception as e:
        return {
            'exito': False,
            'registros_guardados': 0,
            'errores': [str(e)],
        }


def obtener_resumen_por_codigo(df: pd.DataFrame) -> pd.DataFrame:
    """
    Retorna resumen de registros por código.
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=['codigo', 'registros', 'anios'])

    resumen = df.groupby('codigo').agg({
        'anio': lambda x: ', '.join(map(str, sorted(x.unique()))),
    }).reset_index()
    resumen.columns = ['codigo', 'anios']
    resumen['registros'] = df.groupby('codigo').size().values

    return resumen[['codigo', 'registros', 'anios']]


def procesar_aula_virtual_para_guardar(
    df: pd.DataFrame,
    codigo_compuesto: str,
    anio: int = None,
) -> pd.DataFrame:
    """
    Procesa DataFrame de Aula Virtual para que sea compatible con guardar_en_satisfaccion().

    Transforma:
    - dni, c_id, pregunta, respuesta

    A:
    - codigo, anio, dni, pregunta, aspecto, satisfaccion, aspectos
    """
    from datetime import datetime

    if df is None or df.empty:
        return df

    df = df.copy()

    # Agregar codigo y anio
    df['codigo'] = codigo_compuesto
    df['anio'] = anio if anio else datetime.now().year

    # Procesar pregunta: extraer aspectos y limpiar
    aspectos_data = df['pregunta'].apply(
        lambda p: extraer_aspectos(limpiar_texto(p))
    )
    df['aspecto'] = aspectos_data.apply(lambda x: x[0])
    df['aspecto2'] = aspectos_data.apply(lambda x: x[1])

    # Procesar respuesta: mapear a satisfacción
    df['satisfaccion'] = df['respuesta'].apply(
        lambda r: mapear_satisfaccion(limpiar_texto(r))
    )

    # Crear columna 'aspectos' (plural, lista como string)
    # Agrupa aspectos únicos por dni-pregunta
    df['aspectos'] = df['aspecto'].fillna('General')

    # Reordenar y seleccionar columnas necesarias
    columnas_finales = ['codigo', 'anio', 'dni', 'pregunta', 'aspecto', 'satisfaccion', 'aspectos']
    df = df[[col for col in columnas_finales if col in df.columns]]

    return df
