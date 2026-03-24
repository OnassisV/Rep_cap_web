"""Funciones auxiliares para leer usuarios desde la base legacy sincronizada."""

# Logging estandar para registrar errores de conexion/lectura.
import logging
# Ayuda de tipado para anotaciones explicitas.
from typing import Any

# Cliente MySQL usado para consultas directas a tablas legacy.
import pymysql
# Import de settings para leer credenciales LEGACY_DB.
from django.conf import settings


# Logger de modulo para mensajes de diagnostico.
logger = logging.getLogger(__name__)


class LegacyDatabaseError(Exception):
    """Error personalizado cuando la base legacy no esta disponible."""


def get_connection() -> pymysql.connections.Connection:
    """Crea y retorna una conexion MySQL usando settings.LEGACY_DB."""
    # Lee host/user/password/database/port desde settings del proyecto.
    db_config = settings.LEGACY_DB
    # Abre conexion configurada en UTF-8 y con filas en formato diccionario.
    return pymysql.connect(
        host=db_config["host"],
        user=db_config["user"],
        password=db_config["password"],
        database=db_config["database"],
        port=int(db_config["port"]),
        autocommit=True,  # No requiere commit explicito para lecturas.
        charset="utf8mb4",  # Soporta Unicode completo desde MySQL.
        cursorclass=pymysql.cursors.DictCursor,  # Devuelve filas como diccionarios.
    )


def fetch_usernames(limit: int = 500) -> list[str]:
    """Obtiene una lista ordenada de usuarios para datalist/autocompletar en login."""
    # Consulta limitada para mantener rapido el tiempo de carga del login.
    query = "SELECT usuario FROM usuarios ORDER BY usuario LIMIT %s"
    try:
        # Abre conexion y cursor con context manager para cierre automatico.
        with get_connection() as connection:
            with connection.cursor() as cursor:
                # Usa consulta parametrizada para evitar inyeccion SQL en limit.
                cursor.execute(query, (limit,))
                # Lee todas las filas devueltas por el cursor.
                rows = cursor.fetchall()
    except Exception:
        # Registra stacktrace y retorna lista vacia para no romper la UI.
        logger.exception("Could not fetch usernames from legacy database.")
        return []

    # Normaliza valores a strings limpios.
    usernames: list[str] = []
    for row in rows:
        username = str(row.get("usuario", "")).strip()
        if username:
            usernames.append(username)
    return usernames


def fetch_user_record(username: str) -> dict[str, Any] | None:
    """Obtiene un registro de usuario con username, nombre, hash de clave y cargo."""
    # Lee solo las columnas necesarias para el flujo de autenticacion.
    query = (
        "SELECT usuario, especialista_cargo, contrase\u00f1a AS password_hash, cargo "
        "FROM usuarios WHERE usuario = %s LIMIT 1"
    )
    try:
        # Abre conexion y cursor con context manager para cierre automatico.
        with get_connection() as connection:
            with connection.cursor() as cursor:
                # Usa consulta parametrizada para evitar inyeccion SQL por username.
                cursor.execute(query, (username,))
                # Retorna la primera fila encontrada o None.
                return cursor.fetchone()
    except Exception as error:
        # Registra el error completo incluyendo el usuario consultado.
        logger.exception("Could not fetch user %s from legacy database.", username)
        # Lanza error personalizado para que la vista muestre mensaje controlado.
        raise LegacyDatabaseError("Legacy database unavailable.") from error
