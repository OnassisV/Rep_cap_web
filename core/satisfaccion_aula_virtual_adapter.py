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




def extraer_satisfaccion_aula_virtual(
    c_id: str,
    survey_id: str,
    codigo_capacitacion: str,
) -> dict[str, Any]:
    """
    Extrae respuestas de encuestas desde Aula Virtual (Chamilo).

    Args:
        c_id: ID del curso en Chamilo (e.g., 299)
        survey_id: ID de la encuesta en Chamilo (e.g., 394)
        codigo_capacitacion: Código de la capacitación en WEB-CAP (e.g., 25001I)

    Returns:
        {
            'exito': bool,
            'registros': int,
            'errores': list[str],
            'df': pd.DataFrame o None,
            'codigo_compuesto': str,  # e.g., 25001I-299
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
        # SQL basado en estructura real de Chamilo del backup
        # c_survey_answer.user es varchar (DNI/username del usuario)
        # c_survey_answer.option_id es longtext (se compara como string con question_option_id)
        sql = f"""
            SELECT
                a.user AS dni,
                a.c_id,
                REPLACE(REPLACE(b.survey_question, '<p>', ''), '</p>', '') AS pregunta,
                c.option_text AS respuesta
            FROM c_survey_answer a
            LEFT JOIN c_survey_question b
                ON a.c_id = b.c_id
               AND a.survey_id = b.survey_id
               AND a.question_id = b.question_id
            LEFT JOIN c_survey_question_option c
                ON a.c_id = c.c_id
               AND a.survey_id = c.survey_id
               AND a.question_id = c.question_id
               AND CAST(a.option_id AS CHAR) = CAST(c.question_option_id AS CHAR)
            WHERE
                a.c_id = %s
                AND a.survey_id = %s
            ORDER BY a.iid
        """

        # Ejecutar query
        df = pd.read_sql(sql, conn_aula, params=(c_id, survey_id))

        if df.empty:
            return {
                'exito': False,
                'registros': 0,
                'errores': [f"No se encontraron respuestas para c_id={c_id}, survey_id={survey_id}"],
                'df': None,
                'codigo_compuesto': None,
            }

        # Procesar datos
        df['pregunta'] = df['pregunta'].fillna('').astype(str)
        df['respuesta'] = df['respuesta'].fillna('').astype(str)
        df['dni'] = df['dni'].fillna('').astype(str).str.strip()

        # Normalizar DNI a 8 dígitos si es numérico
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
