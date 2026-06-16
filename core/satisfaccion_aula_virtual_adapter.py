"""
Adaptador para extraer datos de satisfacción desde Aula Virtual (Chamilo).

Conecta a la BD de Chamilo, extrae respuestas de encuestas, procesa datos
y guarda en tabla local 'satisfaccion'.
"""

import pandas as pd
import logging
from typing import Any
from django.db import connection
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)


def obtener_conexion_aula():
    """
    Obtiene conexión SQLAlchemy a BD de Aula Virtual (Chamilo).

    Usa las credenciales del environment o configuración.
    """
    from django.conf import settings

    # Si hay configuración específica para Aula Virtual
    if hasattr(settings, 'AULA_DB'):
        db = settings.AULA_DB
        url = f"mysql+pymysql://{db.get('user')}:{db.get('password')}@{db.get('host')}:{db.get('port', 3306)}/{db.get('database')}?charset=utf8mb4"
        return create_engine(url)

    raise RuntimeError(
        "No se configuró conexión a Aula Virtual. "
        "Configura settings.AULA_DB"
    )


def obtener_columnas_tabla(conexion, tabla: str) -> list[str]:
    """Obtiene lista de columnas de una tabla (en minúsculas)."""
    try:
        query = f"SHOW COLUMNS FROM `{tabla}`"
        df = pd.read_sql(query, conexion)
        return [c.lower() for c in df['Field'].tolist()]
    except Exception as e:
        logger.error(f"Error obteniendo columnas de {tabla}: {e}")
        return []


def encontrar_columna(columnas: list[str], opciones_preferidas: list[str]) -> str:
    """
    Encuentra la primera columna que existe de una lista de opciones preferidas.
    Útil porque Chamilo cambia nombres de columnas entre versiones.
    """
    cols_set = set(columnas)
    for opcion in opciones_preferidas:
        if opcion.lower() in cols_set:
            return opcion
    return None


def extraer_satisfaccion_aula_virtual(
    c_id: str,
    survey_id: str,
    codigo_capacitacion: str,
) -> dict[str, Any]:
    """
    Extrae respuestas de encuestas desde Aula Virtual.

    Args:
        c_id: ID del curso en Chamilo
        survey_id: ID de la encuesta en Chamilo
        codigo_capacitacion: Código de la capacitación en WEB-CAP (ej: 25001I)

    Returns:
        {
            'exito': bool,
            'registros': int,
            'errores': list[str],
            'df': pd.DataFrame o None,
            'codigo_compuesto': str,  # Código para guardar (25001I-299)
        }
    """
    try:
        conn_aula = obtener_conexion_aula()
    except Exception as e:
        return {
            'exito': False,
            'registros': 0,
            'errores': [f"No se pudo conectar a Aula Virtual: {str(e)}"],
            'df': None,
            'codigo_compuesto': None,
        }

    try:
        # Detectar nombres de columnas (Chamilo cambia entre versiones)
        cols_a = obtener_columnas_tabla(conn_aula, "c_survey_answer")
        cols_b = obtener_columnas_tabla(conn_aula, "c_survey_question")
        cols_c = obtener_columnas_tabla(conn_aula, "c_survey_question_option")
        cols_u = obtener_columnas_tabla(conn_aula, "user")

        # Encontrar columnas correctas con fallbacks
        a_c = encontrar_columna(cols_a, ["c_id", "c_c_id"])
        a_sid = encontrar_columna(cols_a, ["survey_id"])
        a_qid = encontrar_columna(cols_a, ["question_id", "questionid"])
        a_oid = encontrar_columna(cols_a, ["option_id", "optionid"])
        a_uid = encontrar_columna(cols_a, ["user_id", "id_user", "author_id", "author"])

        b_c = encontrar_columna(cols_b, ["c_id", "c_c_id"])
        b_sid = encontrar_columna(cols_b, ["survey_id"])
        b_qid = encontrar_columna(cols_b, ["question_id", "questionid", "id"])
        b_qtxt = encontrar_columna(cols_b, ["survey_question", "question", "question_text"])

        c_c = encontrar_columna(cols_c, ["c_id", "c_c_id"])
        c_sid = encontrar_columna(cols_c, ["survey_id"])
        c_qid = encontrar_columna(cols_c, ["question_id", "questionid"])
        c_oid = encontrar_columna(cols_c, ["id"])
        c_otxt = encontrar_columna(cols_c, ["option_text", "text", "value", "label"])

        u_id = encontrar_columna(cols_u, ["user_id", "id"])
        u_dni = encontrar_columna(cols_u, ["official_code", "username", "login", "email"])

        # Validar que todas las columnas se encontraron
        missing = []
        if not a_c: missing.append("c_survey_answer.c_id (intentó: c_id, c_c_id)")
        if not a_sid: missing.append("c_survey_answer.survey_id")
        if not a_qid: missing.append("c_survey_answer.question_id")
        if not a_oid: missing.append("c_survey_answer.option_id")
        if not a_uid: missing.append("c_survey_answer.user_id")
        if not b_qtxt: missing.append("c_survey_question.question_text")
        if not c_otxt: missing.append("c_survey_question_option.option_text")
        if not u_dni: missing.append("user.dni")

        if missing:
            debug_info = f"\n\nColumnas encontradas:\n"
            debug_info += f"c_survey_answer: {', '.join(cols_a)}\n"
            debug_info += f"c_survey_question: {', '.join(cols_b)}\n"
            debug_info += f"c_survey_question_option: {', '.join(cols_c)}\n"
            debug_info += f"user: {', '.join(cols_u)}"

            return {
                'exito': False,
                'registros': 0,
                'errores': [f"No se encontraron columnas en Chamilo: {', '.join(missing)}" + debug_info],
                'df': None,
                'codigo_compuesto': None,
            }

        # Construir SQL dinámico
        sql = f"""
            SELECT
                d.{u_dni} AS dni,
                a.{a_c} AS c_id,
                REPLACE(REPLACE(b.{b_qtxt}, '<p>', ''), '</p>', '') AS pregunta,
                c.{c_otxt} AS respuesta
            FROM c_survey_answer a
            LEFT JOIN c_survey_question b
                ON a.{a_c} = b.{b_c}
               AND a.{a_sid} = b.{b_sid}
               AND a.{a_qid} = b.{b_qid}
            LEFT JOIN c_survey_question_option c
                ON a.{a_c} = c.{c_c}
               AND a.{a_sid} = c.{c_sid}
               AND a.{a_qid} = c.{c_qid}
               AND a.{a_oid} = c.{c_oid}
            LEFT JOIN user d
                ON a.{a_uid} = d.{u_id}
            WHERE
                a.{a_c} = %s
                AND a.{a_sid} = %s
        """

        # Ejecutar query
        df = pd.read_sql(sql, conn_aula, params=(c_id, survey_id))

        if df.empty:
            return {
                'exito': False,
                'registros': 0,
                'errores': ["No se encontraron respuestas para ese curso y encuesta"],
                'df': None,
                'codigo_compuesto': None,
            }

        # Procesar datos
        df['pregunta'] = df['pregunta'].fillna('').astype(str)
        df['respuesta'] = df['respuesta'].fillna('').astype(str)
        df['dni'] = df['dni'].fillna('').astype(str).str.strip()

        # Normalizar DNI a 8 dígitos
        df['dni'] = df['dni'].apply(lambda x: x.zfill(8) if x and x.isdigit() else x)

        # Construir código compuesto
        pref = str(codigo_capacitacion).strip()
        codigo_compuesto = f"{pref}-{c_id}" if "-" not in pref else f"{pref.split('-')[0]}-{c_id}"

        return {
            'exito': True,
            'registros': len(df),
            'errores': [],
            'df': df,
            'codigo_compuesto': codigo_compuesto,
        }

    except Exception as e:
        logger.error(f"Error extrayendo satisfacción desde Aula Virtual: {e}", exc_info=True)
        return {
            'exito': False,
            'registros': 0,
            'errores': [f"Error procesando datos: {str(e)}"],
            'df': None,
            'codigo_compuesto': None,
        }
