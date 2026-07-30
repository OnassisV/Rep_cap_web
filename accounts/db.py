"""Funciones auxiliares para leer usuarios desde la base legacy sincronizada."""

# Logging estandar para registrar errores de conexion/lectura.
import logging
# Generacion de claves temporales criptograficamente seguras.
import secrets
# Ayuda de tipado para anotaciones explicitas.
from typing import Any

# Hashing de claves con el mismo esquema que usa el login legacy.
import bcrypt
# Cliente MySQL usado para consultas directas a tablas legacy.
import pymysql
# Import de settings para leer credenciales LEGACY_DB.
from django.conf import settings


# Logger de modulo para mensajes de diagnostico.
logger = logging.getLogger(__name__)


class LegacyDatabaseError(Exception):
    """Error personalizado cuando la base legacy no esta disponible."""


def has_database_config(db_config: dict[str, Any] | None) -> bool:
    """Indica si una configuracion MySQL tiene lo minimo para abrir conexion."""
    db_config = db_config or {}
    return all(
        str(db_config.get(key, "") or "").strip()
        for key in ("host", "user", "database")
    )


def get_connection_from_config(db_config: dict[str, Any]) -> pymysql.connections.Connection:
    """Crea una conexion MySQL a partir de una configuracion explicita."""
    return pymysql.connect(
        host=db_config["host"],
        user=db_config["user"],
        password=db_config["password"],
        database=db_config["database"],
        port=int(db_config["port"]),
        autocommit=True,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )


def get_connection() -> pymysql.connections.Connection:
    """Crea y retorna una conexion MySQL usando settings.LEGACY_DB."""
    # Lee host/user/password/database/port desde settings del proyecto.
    db_config = settings.LEGACY_DB
    # Abre conexion configurada en UTF-8 y con filas en formato diccionario.
    return get_connection_from_config(db_config)


def get_shared_connection() -> pymysql.connections.Connection:
    """Abre la conexion MySQL compartida usada por integraciones globales."""
    shared_config = getattr(settings, "SHARED_MYSQL", None)
    if not has_database_config(shared_config):
        raise LegacyDatabaseError("Shared MySQL database unavailable.")
    return get_connection_from_config(shared_config)


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


def fetch_users_with_names(limit: int = 500) -> list[dict[str, str]]:
    """Retorna lista de usuarios con su username y nombre (especialista_cargo)."""
    query = "SELECT usuario, especialista_cargo FROM usuarios ORDER BY especialista_cargo LIMIT %s"
    try:
        with get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, (limit,))
                rows = cursor.fetchall()
    except Exception:
        logger.exception("Could not fetch users with names from legacy database.")
        return []

    result: list[dict[str, str]] = []
    for row in rows:
        usuario = str(row.get("usuario", "")).strip()
        nombre = str(row.get("especialista_cargo", "")).strip()
        if usuario:
            result.append({"usuario": usuario, "nombre": nombre or usuario})
    return result


def fetch_user_cargo(username: str) -> str:
    """Retorna el cargo legacy de un usuario, o cadena vacia si no existe."""
    username = str(username or "").strip()
    if not username:
        return ""
    try:
        with get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT cargo FROM usuarios WHERE usuario = %s LIMIT 1",
                    (username,),
                )
                row = cursor.fetchone()
    except Exception:
        logger.exception("No se pudo leer el cargo de %s", username)
        return ""
    return str((row or {}).get("cargo", "") or "").strip()


def generar_clave_temporal(longitud: int = 10) -> str:
    """Genera una clave temporal legible para entregar al visor."""
    # Excluye caracteres ambiguos (O/0, l/1) para dictarla sin errores.
    alfabeto = "ABCDEFGHJKMNPQRSTUVWXYZabcdefghijkmnpqrstuvwxyz23456789"
    return "".join(secrets.choice(alfabeto) for _ in range(max(8, longitud)))


def crear_usuario_visor(email: str, nombre: str = "") -> dict[str, Any]:
    """Crea (o reutiliza) una cuenta con cargo 'Visor' en la base legacy.

    Devuelve {"ok": bool, "creado": bool, "password": str|None, "error": str}.
    `password` solo viene con valor cuando la cuenta se acaba de crear: las
    claves se guardan como hash bcrypt y no pueden recuperarse despues.
    """
    email = str(email or "").strip()
    if not email:
        return {"ok": False, "creado": False, "password": None, "error": "El correo es obligatorio."}

    nombre = str(nombre or "").strip() or email.split("@")[0]

    try:
        with get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT usuario, cargo FROM usuarios WHERE usuario = %s LIMIT 1",
                    (email,),
                )
                existente = cursor.fetchone()

                if existente:
                    cargo = str(existente.get("cargo", "") or "").strip()
                    # Nunca degradar una cuenta interna a Visor por error.
                    if cargo.lower() != "visor":
                        return {
                            "ok": False,
                            "creado": False,
                            "password": None,
                            "error": (
                                f"El correo {email} ya existe como '{cargo}' (cuenta interna). "
                                "No se puede convertir en visor."
                            ),
                        }
                    return {"ok": True, "creado": False, "password": None, "error": ""}

                password = generar_clave_temporal()
                password_hash = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
                cursor.execute(
                    "INSERT INTO usuarios (especialista_cargo, usuario, `contraseña`, cargo, codigo_verif) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    (nombre, email, password_hash, "Visor", ""),
                )
    except Exception as error:
        logger.exception("No se pudo crear la cuenta visor %s", email)
        return {"ok": False, "creado": False, "password": None, "error": str(error)[:200]}

    return {"ok": True, "creado": True, "password": password, "error": ""}


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
