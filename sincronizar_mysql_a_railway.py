from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import unquote, urlparse

try:
    import mysql.connector
    from mysql.connector import Error as MySQLError
except ImportError:
    mysql = None
    MySQLError = Exception


SCRIPT_DIR = Path(__file__).resolve().parent
ENV_FILE = SCRIPT_DIR / ".env"
STATE_FILE = SCRIPT_DIR / "sync_state.json"
DEFAULT_AULA_SQL = (
    SCRIPT_DIR.parent.parent / "descargar tablas aula y sysdifo" / "operacion_backup_sql" / "edutalentos_aulavirtual.sql"
)
DEFAULT_SIDI_SQL = SCRIPT_DIR.parent.parent / "descargar tablas aula y sysdifo" / "operacion_backup_sql" / "db_sidifoca.sql"
INSERT_RE = re.compile(
    r"^INSERT INTO\s+`(?P<table>[^`]+)`\s*\((?P<columns>.*?)\)\s*VALUES\s*(?P<values>.*)$",
    re.IGNORECASE | re.DOTALL,
)
CREATE_RE = re.compile(r"^CREATE TABLE\s+`(?P<table>[^`]+)`", re.IGNORECASE)
PRIMARY_KEY_RE = re.compile(r"PRIMARY KEY\s*\((?P<columns>[^)]+)\)", re.IGNORECASE | re.DOTALL)
SYNC_STATUS_TABLE = "sync_runtime_status"


@dataclass(frozen=True)
class DBConfig:
    host: str
    port: int
    user: str
    password: str
    database: str
    ssl_disabled: bool = False


@dataclass(frozen=True)
class TableSpec:
    dump_key: str
    table_name: str
    marker_columns: tuple[str, ...] = ()
    marker_kind: str | None = None
    initial_mode: str = "full"
    force_primary_key: tuple[str, ...] = ()


@dataclass(frozen=True)
class CourseFilter:
    exact_ids: frozenset[int]
    ranged_ids: tuple[tuple[int, int | None], ...] = ()

    def matches(self, value: Any) -> bool:
        try:
            course_id = int(value)
        except (TypeError, ValueError):
            return False

        if course_id in self.exact_ids:
            return True

        for start_id, end_id in self.ranged_ids:
            if course_id < start_id:
                continue
            if end_id is None or course_id <= end_id:
                return True
        return False


@dataclass(frozen=True)
class AulaScope:
    course_filter: CourseFilter | None = None
    user_ids: frozenset[int] = frozenset()


TABLE_SPECS = {
    "course": TableSpec("aula", "course", ("last_edit", "creation_date", "last_visit"), "datetime", "lookback_datetime"),
    "course_rel_user": TableSpec("aula", "course_rel_user", ("id",), "integer", "last_fraction"),
    "user": TableSpec("aula", "user", ("updated_at", "created_at", "registration_date", "last_login"), "datetime", "lookback_datetime"),
    "c_quiz": TableSpec("aula", "c_quiz", ("id",), "integer", "last_fraction"),
    "c_quiz_question": TableSpec("aula", "c_quiz_question", ("id",), "integer", "last_fraction"),
    "c_quiz_answer": TableSpec("aula", "c_quiz_answer", ("id",), "integer", "last_fraction"),
    "c_quiz_rel_question": TableSpec("aula", "c_quiz_rel_question", (), None, "last_fraction"),
    "track_e_attempt": TableSpec("aula", "track_e_attempt", ("tms",), "datetime", "lookback_datetime"),
    "track_e_exercises": TableSpec("aula", "track_e_exercises", ("exe_date", "start_date"), "datetime", "lookback_datetime"),
    # Marcador por fecha (no por id): las calificaciones se aplican con UPDATE sobre filas
    # existentes (id no cambia), asi que un marcador entero nunca detecta una nota nueva en
    # una fila ya sincronizada. Con marcador datetime + lookback, la ventana de seguridad
    # (SYNC_SAFETY_DAYS) vuelve a incluir filas calificadas recientemente aunque su pk sea vieja.
    "c_student_publication": TableSpec("aula", "c_student_publication", ("date_of_qualification", "sent_date"), "datetime", "lookback_datetime"),
    "c_survey": TableSpec("aula", "c_survey", ("iid", "id"), "integer", "last_fraction"),
    "c_survey_question": TableSpec("aula", "c_survey_question", ("iid",), "integer", "last_fraction"),
    "c_survey_question_option": TableSpec("aula", "c_survey_question_option", ("iid",), "integer", "last_fraction"),
    "c_survey_answer": TableSpec("aula", "c_survey_answer", ("iid",), "integer", "last_fraction"),
    "uvw_usuarios_detalle_completo": TableSpec("sys", "uvw_usuarios_detalle_completo", ("fec_modifica",), "datetime", "full", ("id_usuario",)),
}


AULA_TABLE_COURSE_COLUMN = {
    "course_rel_user": "c_id",
    "c_quiz": "c_id",
    "c_quiz_question": "c_id",
    "c_quiz_answer": "c_id",
    "c_quiz_rel_question": "c_id",
    "track_e_attempt": "c_id",
    "track_e_exercises": "c_id",
    "c_student_publication": "c_id",
    "c_survey": "c_id",
    "c_survey_question": "c_id",
    "c_survey_question_option": "c_id",
    "c_survey_answer": "c_id",
}


def load_env_file(env_file: Path) -> None:
    if not env_file.is_file():
        return

    for raw_line in env_file.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if value and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]
        os.environ.setdefault(key, value)


def env_value(*names: str, default: str | None = None) -> str | None:
    for name in names:
        value = os.getenv(name)
        if value not in (None, ""):
            return value
    return default


def env_flag(*names: str, default: bool = False) -> bool:
    value = env_value(*names)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def env_int(*names: str, default: int) -> int:
    value = env_value(*names)
    if value is None:
        return default
    return int(value)


def env_float(*names: str, default: float) -> float:
    value = env_value(*names)
    if value is None:
        return default
    return float(value)


def parse_course_filter(raw_value: str | None) -> CourseFilter | None:
    if raw_value is None:
        return None

    exact_ids: set[int] = set()
    ranged_ids: list[tuple[int, int | None]] = []
    for token in raw_value.split(","):
        piece = token.strip()
        if not piece:
            continue

        if piece.endswith("+"):
            start_text = piece[:-1].strip()
            if not start_text.isdigit():
                raise ValueError(f"Filtro de curso invalido: {piece}")
            ranged_ids.append((int(start_text), None))
            continue

        if "-" in piece:
            start_text, end_text = piece.split("-", 1)
            start_text = start_text.strip()
            end_text = end_text.strip()
            if not start_text.isdigit() or not end_text.isdigit():
                raise ValueError(f"Filtro de curso invalido: {piece}")
            start_id = int(start_text)
            end_id = int(end_text)
            if end_id < start_id:
                raise ValueError(f"Rango de curso invalido: {piece}")
            ranged_ids.append((start_id, end_id))
            continue

        if not piece.isdigit():
            raise ValueError(f"Filtro de curso invalido: {piece}")
        exact_ids.add(int(piece))

    if not exact_ids and not ranged_ids:
        return None
    return CourseFilter(exact_ids=frozenset(exact_ids), ranged_ids=tuple(ranged_ids))


def normalize_course_filter(raw_value: str | None) -> str | None:
    course_filter = parse_course_filter(raw_value)
    if course_filter is None:
        return None

    normalized: list[str] = [str(course_id) for course_id in sorted(course_filter.exact_ids)]
    for start_id, end_id in course_filter.ranged_ids:
        normalized.append(f"{start_id}+" if end_id is None else f"{start_id}-{end_id}")
    return ",".join(normalized)


def row_matches_aula_scope(table_name: str, row_map: dict[str, Any], scope: AulaScope | None) -> bool:
    if scope is None or scope.course_filter is None:
        return True

    if table_name == "course":
        return scope.course_filter.matches(row_map.get("id"))
    if table_name == "user":
        return row_map.get("id") in scope.user_ids

    course_column = AULA_TABLE_COURSE_COLUMN.get(table_name)
    if course_column is None:
        return True
    return scope.course_filter.matches(row_map.get(course_column))


def build_aula_scope(sql_file: Path, course_filter: CourseFilter | None) -> AulaScope | None:
    if course_filter is None:
        return None
    if not sql_file.is_file():
        raise FileNotFoundError(f"No se encontro el archivo SQL: {sql_file}")

    user_ids: set[int] = set()
    for statement in iter_sql_statements(sql_file):
        insert_parts = extract_insert_parts(statement)
        if not insert_parts:
            continue

        table_name, columns, values_sql = insert_parts
        if table_name != "course_rel_user":
            continue

        for row in iter_insert_rows(values_sql):
            row_map = dict(zip(columns, row))
            if not course_filter.matches(row_map.get("c_id")):
                continue
            user_id = row_map.get("user_id")
            try:
                user_ids.add(int(user_id))
            except (TypeError, ValueError):
                continue

    return AulaScope(course_filter=course_filter, user_ids=frozenset(user_ids))


def should_trim_initial_aula_rows(dump_key: str, aula_scope: AulaScope | None) -> bool:
    return not (dump_key == "aula" and aula_scope is not None)


def parse_mysql_url(raw_url: str) -> dict[str, str | int]:
    parsed = urlparse(raw_url)
    scheme = parsed.scheme.lower()
    if scheme not in {"mysql", "mysql+pymysql", "mysql+mysqlconnector"}:
        raise ValueError("La URL configurada no es MySQL.")

    database = parsed.path.lstrip("/")
    if not database:
        raise ValueError("La URL MySQL no incluye nombre de base de datos.")

    return {
        "host": parsed.hostname or "",
        "port": parsed.port or 3306,
        "user": unquote(parsed.username or ""),
        "password": unquote(parsed.password or ""),
        "database": unquote(database),
    }


def build_target_config() -> DBConfig:
    raw_url = env_value("RAILWAY_MYSQL_URL", "MYSQL_URL", "DATABASE_URL")
    parsed = parse_mysql_url(raw_url) if raw_url else {}
    host = env_value("RAILWAY_DB_HOST", "MYSQLHOST", default=str(parsed.get("host", "")))
    port = env_int("RAILWAY_DB_PORT", "MYSQLPORT", default=int(parsed.get("port", 3306)))
    user = env_value("RAILWAY_DB_USER", "MYSQLUSER", default=str(parsed.get("user", "")))
    password = env_value("RAILWAY_DB_PASSWORD", "MYSQLPASSWORD", default=str(parsed.get("password", "")))
    database = env_value("RAILWAY_DB_NAME", "MYSQLDATABASE", default=str(parsed.get("database", "")))

    missing = [
        name
        for name, value in {
            "RAILWAY_MYSQL_URL o RAILWAY_DB_HOST": host,
            "RAILWAY_DB_USER": user,
            "RAILWAY_DB_PASSWORD": password,
            "RAILWAY_DB_NAME": database,
        }.items()
        if not value
    ]
    if missing:
        raise ValueError("Faltan datos de Railway: " + ", ".join(missing))

    return DBConfig(host=host, port=port, user=user, password=password, database=database, ssl_disabled=env_flag("RAILWAY_DB_SSL_DISABLED", default=False))


def resolve_dump_path(raw_value: str | None, default_path: Path) -> Path:
    if raw_value:
        candidate = Path(raw_value).expanduser()
        if not candidate.is_absolute():
            candidate = (SCRIPT_DIR / candidate).resolve()
        return candidate
    return default_path.resolve()


def _onedrive_lock_backoff_delay(attempt: int, *, base_delay: float = 5.0, max_delay: float = 30.0) -> float:
    """Backoff exponencial (5s, 7.5s, 11.25s, ... tope 30s) para reintentos ante bloqueos de OneDrive."""
    return min(base_delay * (1.5 ** attempt), max_delay)


def _compute_sha256(file_path: Path, *, retries: int = 12) -> str:
    """Calcula SHA256 del archivo con reintentos ante bloqueos transitorios de OneDrive (EDEADLK)."""
    import errno as _errno_mod
    for attempt in range(retries):
        try:
            h = hashlib.sha256()
            with file_path.open("rb") as f:
                while True:
                    chunk = f.read(1024 * 1024)
                    if not chunk:
                        break
                    h.update(chunk)
            return h.hexdigest()
        except OSError as exc:
            if exc.errno == _errno_mod.EDEADLK and attempt < retries - 1:
                retry_delay = _onedrive_lock_backoff_delay(attempt)
                print(
                    f"[WARN] El archivo esta bloqueado por OneDrive (EDEADLK, intento {attempt + 1}/{retries}). "
                    f"Reintentando en {retry_delay:.1f}s..."
                )
                time.sleep(retry_delay)
                continue
            raise


def validate_dump_integrity(dump_path: Path, manifest_path: Path | None) -> bool:
    """Valida el SHA256 del dump contra el manifiesto ULTIMO_BACKUP.json.

    Retorna True si el hash coincide o si no hay manifiesto disponible
    (para no bloquear la sincronización cuando no existe el manifiesto).
    """
    if manifest_path is None or not manifest_path.is_file():
        print("[WARN] No se encontro manifiesto de backup. Se omite la validacion SHA256.")
        return True

    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        print(f"[WARN] No se pudo leer el manifiesto: {exc}")
        return True

    dump_name = dump_path.name
    expected_sha = None
    for entry in manifest.get("files", []):
        latest = entry.get("latest_file", "")
        if latest.endswith(dump_name) or Path(latest).name == dump_name:
            expected_sha = entry.get("sha256")
            break

    if not expected_sha:
        print(f"[WARN] El manifiesto no contiene SHA256 para '{dump_name}'. Se omite la validacion.")
        return True

    expected_size = entry.get("size_bytes")

    print(f"[INFO] Validando SHA256 de {dump_name} ...")
    try:
        actual_sha = _compute_sha256(dump_path)
    except OSError as exc:
        print(f"[WARN] No se pudo leer el archivo para SHA256 ({exc}). Se omite la validacion.")
        return True
    if actual_sha == expected_sha:
        print(f"[INFO] SHA256 OK: {actual_sha[:16]}...")
        return True

    # SHA256 no coincide — determinar si es corrupcion o version anterior
    actual_size = dump_path.stat().st_size

    if expected_size is not None and actual_size != expected_size:
        # Tamano diferente = archivo de una version anterior del backup (OneDrive aun no sincronizo el SQL)
        print(f"[WARN] SHA256 no coincide para {dump_name}, pero el tamano difiere del manifiesto:")
        print(f"  Tamano manifiesto: {expected_size:,} bytes | Tamano local: {actual_size:,} bytes")
        print(f"  SHA256 manifiesto: {expected_sha[:16]}... | SHA256 local: {actual_sha[:16]}...")
        print("[WARN] El archivo local parece ser de una version anterior del backup (OneDrive no ha terminado de sincronizar).")
        print("[WARN] Se procede con la sincronizacion usando la version disponible.")
        return True

    print(f"[ERROR] SHA256 NO COINCIDE para {dump_name}")
    print(f"  Esperado (manifiesto): {expected_sha}")
    print(f"  Actual (archivo local): {actual_sha}")
    if expected_size is not None:
        print(f"  Tamano esperado: {expected_size:,} bytes | Tamano actual: {actual_size:,} bytes (mismos)")
    print("[ERROR] El archivo tiene el mismo tamano pero contenido diferente. Posible corrupcion.")
    return False


def open_target_connection(config: DBConfig):
    if mysql is None:
        raise RuntimeError("Falta mysql-connector-python. Ejecuta instalar_dependencias.sh.")

    kwargs = {
        "host": config.host,
        "port": config.port,
        "user": config.user,
        "password": config.password,
        "database": config.database,
        "charset": "utf8mb4",
        "autocommit": False,
        "use_unicode": True,
    }
    if config.ssl_disabled:
        kwargs["ssl_disabled"] = True
    conn = mysql.connector.connect(**kwargs)
    cursor = conn.cursor()
    cursor.execute("SET SESSION sql_mode = ''")
    cursor.close()
    return conn


def open_target_connection_with_retry(config: DBConfig, attempts: int = 5, delay_seconds: int = 5):
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return open_target_connection(config)
        except (RuntimeError, MySQLError) as error:
            last_error = error
            if attempt == attempts:
                break
            print(
                f"[WARN] Conexion inicial a Railway fallo en intento {attempt}/{attempts}: {error}",
                file=sys.stderr,
                flush=True,
            )
            time.sleep(delay_seconds)
    raise last_error or RuntimeError("No se pudo abrir la conexion a Railway")


def ensure_sync_status_table(connection: Any) -> None:
    cursor = connection.cursor()
    try:
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {SYNC_STATUS_TABLE} (
                sync_key VARCHAR(100) NOT NULL PRIMARY KEY,
                sync_label VARCHAR(150) NOT NULL,
                last_success_at DATETIME NULL,
                last_attempt_at DATETIME NULL,
                last_status VARCHAR(20) NOT NULL,
                last_error TEXT NULL,
                updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        )
        connection.commit()
    finally:
        cursor.close()


def update_sync_runtime_status(
    connection: Any,
    *,
    status: str,
    error_message: str | None = None,
    success_timestamp: str | None = None,
) -> None:
    sync_key = env_value("SYNC_STATUS_KEY", default="railway_hourly_sync") or "railway_hourly_sync"
    sync_label = env_value("SYNC_STATUS_LABEL", default="Sincronizacion horaria Railway") or "Sincronizacion horaria Railway"
    now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    ensure_sync_status_table(connection)
    cursor = connection.cursor()
    try:
        cursor.execute(
            f"""
            INSERT INTO {SYNC_STATUS_TABLE}
                (sync_key, sync_label, last_success_at, last_attempt_at, last_status, last_error)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                sync_label = VALUES(sync_label),
                last_success_at = COALESCE(VALUES(last_success_at), last_success_at),
                last_attempt_at = VALUES(last_attempt_at),
                last_status = VALUES(last_status),
                last_error = VALUES(last_error)
            """,
            (
                sync_key,
                sync_label,
                success_timestamp,
                now_text,
                status,
                error_message,
            ),
        )
        connection.commit()
    finally:
        cursor.close()


def load_state(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {"tables": {}}
    return json.loads(path.read_text(encoding="utf-8"))


def save_state(path: Path, state: dict[str, Any]) -> None:
    path.write_text(json.dumps(state, indent=2, ensure_ascii=True, sort_keys=True), encoding="utf-8")


def quote_identifier(name: str) -> str:
    return f"`{name.replace('`', '``')}`"


def iter_sql_statements(sql_file: Path, *, retries: int = 12) -> Iterable[str]:
    import errno as _errno_mod

    raw_content: str | None = None
    for attempt in range(retries):
        try:
            raw_content = sql_file.read_text(encoding="utf-8", errors="replace")
            break
        except OSError as exc:
            if exc.errno == _errno_mod.EDEADLK and attempt < retries - 1:
                retry_delay = _onedrive_lock_backoff_delay(attempt)
                print(
                    f"[WARN] Archivo SQL bloqueado por OneDrive al leer (EDEADLK, intento {attempt + 1}/{retries}). "
                    f"Reintentando en {retry_delay:.1f}s..."
                )
                time.sleep(retry_delay)
                continue
            raise

    buffer: list[str] = []
    in_single = False
    in_double = False
    in_backtick = False
    in_block_comment = False
    escape_next = False

    for raw_line in raw_content.splitlines(keepends=True):
            if not any((in_single, in_double, in_backtick, in_block_comment)):
                stripped = raw_line.lstrip()
                if stripped.startswith("--") or stripped.startswith("#"):
                    continue

            index = 0
            while index < len(raw_line):
                char = raw_line[index]
                next_char = raw_line[index + 1] if index + 1 < len(raw_line) else ""

                if in_block_comment:
                    if char == "*" and next_char == "/":
                        in_block_comment = False
                        index += 2
                        continue
                    index += 1
                    continue

                if not any((in_single, in_double, in_backtick)):
                    if char == "/" and next_char == "*":
                        in_block_comment = True
                        index += 2
                        continue
                    if char == "-" and next_char == "-":
                        break
                    if char == "#":
                        break

                buffer.append(char)

                if in_single:
                    if escape_next:
                        escape_next = False
                    elif char == "\\":
                        escape_next = True
                    elif char == "'":
                        in_single = False
                elif in_double:
                    if escape_next:
                        escape_next = False
                    elif char == "\\":
                        escape_next = True
                    elif char == '"':
                        in_double = False
                elif in_backtick:
                    if char == "`":
                        in_backtick = False
                else:
                    if char == "'":
                        in_single = True
                    elif char == '"':
                        in_double = True
                    elif char == "`":
                        in_backtick = True
                    elif char == ";":
                        statement = "".join(buffer).strip()
                        buffer.clear()
                        if statement:
                            yield statement
                index += 1

    trailing = "".join(buffer).strip()
    if trailing:
        yield trailing


def table_exists(connection: Any, table_name: str) -> bool:
    cursor = connection.cursor()
    try:
        cursor.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = %s",
            (table_name,),
        )
        return cursor.fetchone() is not None
    finally:
        cursor.close()


def sanitize_create_table_sql(create_sql: str) -> str:
    filtered: list[str] = []
    for line in create_sql.splitlines():
        stripped = line.strip()
        if stripped.startswith("CONSTRAINT") and "FOREIGN KEY" in stripped:
            continue
        filtered.append(line)

    for index in range(len(filtered) - 1):
        if filtered[index].rstrip().endswith(",") and filtered[index + 1].lstrip().startswith(")"):
            filtered[index] = filtered[index].rstrip().rstrip(",")

    sanitized = "\n".join(filtered)
    sanitized = re.sub(r"AUTO_INCREMENT=\d+\s*", "", sanitized)
    return sanitized


def extract_primary_keys(create_sql: str) -> list[str]:
    match = PRIMARY_KEY_RE.search(create_sql)
    if not match:
        return []
    return re.findall(r"`([^`]+)`", match.group("columns"))


def ensure_target_table(connection: Any, table_name: str, create_sql: str, reset_target: bool) -> list[str]:
    primary_keys = extract_primary_keys(create_sql)
    cursor = connection.cursor()
    try:
        if reset_target and table_exists(connection, table_name):
            cursor.execute("SET FOREIGN_KEY_CHECKS=0")
            cursor.execute(f"DROP TABLE {quote_identifier(table_name)}")
            cursor.execute("SET FOREIGN_KEY_CHECKS=1")
            connection.commit()

        if not table_exists(connection, table_name):
            cursor.execute("SET FOREIGN_KEY_CHECKS=0")
            cursor.execute(sanitize_create_table_sql(create_sql))
            cursor.execute("SET FOREIGN_KEY_CHECKS=1")
            connection.commit()
    finally:
        cursor.close()
    return primary_keys


def apply_forced_primary_key(connection: Any, table_name: str, pk_columns: tuple[str, ...]) -> list[str]:
    pk_sql = ", ".join(quote_identifier(c) for c in pk_columns)
    cursor = connection.cursor()
    try:
        cursor.execute(
            f"SELECT COUNT(*) FROM information_schema.table_constraints "
            f"WHERE table_schema = DATABASE() AND table_name = %s AND constraint_type = 'PRIMARY KEY'",
            (table_name,),
        )
        if cursor.fetchone()[0] > 0:
            return list(pk_columns)
        cursor.execute(f"ALTER TABLE {quote_identifier(table_name)} ADD PRIMARY KEY ({pk_sql})")
        connection.commit()
        print(f"[INFO] PRIMARY KEY forzada en {table_name}: ({pk_sql})")
    except Exception as exc:
        print(f"[WARN] No se pudo agregar PRIMARY KEY a {table_name}: {exc}")
        return []
    finally:
        cursor.close()
    return list(pk_columns)


def reset_managed_tables(connection: Any) -> None:
    cursor = connection.cursor()
    try:
        cursor.execute("SET FOREIGN_KEY_CHECKS=0")
        for table_name in sorted(TABLE_SPECS):
            if table_exists(connection, table_name):
                cursor.execute(f"DROP TABLE {quote_identifier(table_name)}")
        cursor.execute("SET FOREIGN_KEY_CHECKS=1")
        connection.commit()
    finally:
        cursor.close()


def unescape_mysql_string(value: str) -> str:
    replacements = {
        "0": "\0",
        "n": "\n",
        "r": "\r",
        "t": "\t",
        "Z": "\x1a",
        "\\": "\\",
        "'": "'",
        '"': '"',
    }
    result: list[str] = []
    index = 0
    while index < len(value):
        char = value[index]
        if char == "\\" and index + 1 < len(value):
            result.append(replacements.get(value[index + 1], value[index + 1]))
            index += 2
            continue
        result.append(char)
        index += 1
    return "".join(result)


def parse_sql_token(token: str) -> Any:
    token = token.strip()
    if token.upper() == "NULL":
        return None
    if token.startswith("'") and token.endswith("'"):
        return unescape_mysql_string(token[1:-1])
    if token.startswith("0x") or token.startswith("0X"):
        return bytes.fromhex(token[2:])
    if re.fullmatch(r"-?\d+", token):
        return int(token)
    if re.fullmatch(r"-?\d+\.\d+", token):
        return float(token)
    return token


def iter_insert_rows(values_sql: str) -> Iterable[list[Any]]:
    index = 0
    length = len(values_sql)
    while index < length:
        while index < length and values_sql[index] in " \t\r\n,":
            index += 1
        if index >= length:
            return
        if values_sql[index] != "(":
            raise ValueError("Formato inesperado al parsear VALUES del dump SQL.")

        index += 1
        row: list[Any] = []
        token: list[str] = []
        in_single = False
        escape_next = False

        while index < length:
            char = values_sql[index]
            if in_single:
                token.append(char)
                if escape_next:
                    escape_next = False
                elif char == "\\":
                    escape_next = True
                elif char == "'":
                    in_single = False
                index += 1
                continue

            if char == "'":
                in_single = True
                token.append(char)
                index += 1
                continue
            if char == ",":
                row.append(parse_sql_token("".join(token)))
                token.clear()
                index += 1
                continue
            if char == ")":
                row.append(parse_sql_token("".join(token)))
                token.clear()
                index += 1
                break

            token.append(char)
            index += 1

        yield row


def extract_insert_parts(statement: str) -> tuple[str, list[str], str] | None:
    match = INSERT_RE.match(statement)
    if not match:
        return None
    table_name = match.group("table")
    columns = re.findall(r"`([^`]+)`", match.group("columns"))
    values_sql = match.group("values").rstrip().rstrip(";")
    return table_name, columns, values_sql


def serialize_marker(value: Any, marker_kind: str | None) -> str | int | None:
    if value is None:
        return None
    if marker_kind == "integer":
        return int(value)
    return str(value)


def choose_marker(row_map: dict[str, Any], spec: TableSpec) -> Any:
    for column in spec.marker_columns:
        value = row_map.get(column)
        if value not in (None, "", "0000-00-00 00:00:00"):
            return value
    return None


def choose_pk_value(row_map: dict[str, Any], primary_keys: list[str]) -> Any:
    if len(primary_keys) == 1:
        return row_map.get(primary_keys[0])
    if "id" in row_map:
        return row_map.get("id")
    return None


def marker_passes_initial_window(spec: TableSpec, marker_value: Any, cutoff_text: str) -> bool:
    if spec.initial_mode != "lookback_datetime":
        return True
    if spec.marker_kind != "datetime":
        return True
    if marker_value is None:
        return False
    return str(marker_value) >= cutoff_text


def needs_initial_load(state_entry: dict[str, Any] | None) -> bool:
    if state_entry is None:
        return True
    return state_entry.get("last_marker") is None and state_entry.get("last_pk") is None


def count_rows_in_values(values_sql: str) -> int:
    return sum(1 for _ in iter_insert_rows(values_sql))


def collect_initial_row_totals(
    sql_file: Path,
    active_specs: dict[str, TableSpec],
    state: dict[str, Any],
    aula_scope: AulaScope | None,
) -> dict[str, int]:
    trim_initial_rows = should_trim_initial_aula_rows(next(iter(active_specs.values())).dump_key, aula_scope) if active_specs else True

    targets = {
        table_name
        for table_name, spec in active_specs.items()
        if spec.initial_mode == "last_fraction"
        and trim_initial_rows
        and needs_initial_load(state.setdefault("tables", {}).get(f"{spec.dump_key}.{table_name}"))
    }
    if not targets:
        return {}

    totals = {table_name: 0 for table_name in targets}
    for statement in iter_sql_statements(sql_file):
        insert_parts = extract_insert_parts(statement)
        if not insert_parts:
            continue
        table_name, columns, values_sql = insert_parts
        if table_name not in targets:
            continue

        if active_specs[table_name].dump_key != "aula" or aula_scope is None:
            totals[table_name] += count_rows_in_values(values_sql)
            continue

        for row in iter_insert_rows(values_sql):
            row_map = dict(zip(columns, row))
            if row_matches_aula_scope(table_name, row_map, aula_scope):
                totals[table_name] += 1
    return totals


def calculate_rows_to_skip(total_rows: int, keep_fraction: float) -> int:
    if total_rows <= 0:
        return 0
    keep_rows = max(1, math.ceil(total_rows * keep_fraction))
    return max(0, total_rows - keep_rows)


def table_env_suffix(table_name: str) -> str:
    return re.sub(r"[^A-Z0-9]+", "_", table_name.upper())


def resolve_no_date_keep_fraction(total_rows: int) -> float:
    large_min_rows = env_int("SYNC_LARGE_TABLE_MIN_ROWS", default=50000)
    medium_min_rows = env_int("SYNC_MEDIUM_TABLE_MIN_ROWS", default=10000)
    large_fraction = env_float("SYNC_LARGE_TABLE_FRACTION", default=0.02)
    medium_fraction = env_float("SYNC_MEDIUM_TABLE_FRACTION", default=0.05)
    small_fraction = env_float("SYNC_SMALL_TABLE_FRACTION", default=0.10)

    if total_rows >= large_min_rows:
        return large_fraction
    if total_rows >= medium_min_rows:
        return medium_fraction
    return small_fraction


def resolve_table_keep_fraction(table_name: str, total_rows: int) -> float:
    override = env_value(f"SYNC_TABLE_{table_env_suffix(table_name)}_FRACTION")
    if override is not None:
        return float(override)
    return resolve_no_date_keep_fraction(total_rows)


def _within_safety_window(
    spec: TableSpec,
    marker_value: Any,
    safety_cutoff: str | None,
    *,
    last_marker: Any = None,
    integer_safety_margin: int = 0,
) -> bool:
    """Retorna True si el registro cae dentro de la ventana de seguridad reciente.

    Protege contra el caso en que el watermark global de pk avanza por otros cursos/tablas
    y deja registros intermedios fuera del alcance incremental normal.
    """
    if spec.marker_kind == "datetime":
        if spec.initial_mode != "lookback_datetime":
            return False
        if safety_cutoff is None or marker_value is None:
            return False
        return str(marker_value) >= safety_cutoff

    if spec.marker_kind == "integer":
        # Reprocesa un margen de IDs por debajo del marcador para absorber filas cuya
        # transaccion confirmo (commit) fuera de orden respecto a otra con id mayor
        # (comun bajo escrituras concurrentes, ej. muchas matriculas al mismo tiempo).
        if integer_safety_margin <= 0 or marker_value is None or last_marker is None:
            return False
        return int(marker_value) >= int(last_marker) - integer_safety_margin

    return False


def marker_is_newer(spec: TableSpec, marker_value: Any, pk_value: Any, state_entry: dict[str, Any] | None) -> bool:
    if state_entry is None:
        return True

    last_marker = state_entry.get("last_marker")
    last_pk = state_entry.get("last_pk")

    if last_marker is None and last_pk is None:
        return False

    if spec.marker_kind is None:
        return pk_value is not None and last_pk is not None and _pk_gt(pk_value, last_pk)

    if spec.marker_kind == "integer":
        if marker_value is None:
            return False
        if last_marker is None:
            return True
        return int(marker_value) > int(last_marker)

    if spec.marker_kind == "datetime":
        if pk_value is not None and last_pk is not None:
            try:
                if int(pk_value) > int(last_pk):
                    return True
            except (ValueError, TypeError):
                if str(pk_value) > str(last_pk):
                    return True
        if marker_value is None:
            return False
        if last_marker is None:
            return True
        current = str(marker_value)
        previous = str(last_marker)
        if current > previous:
            return True
        if current == previous and pk_value is not None and last_pk is not None:
            return _pk_gt(pk_value, last_pk)
    return False


_DRE_GRE_PREFIX_RE = re.compile(r"^(DRE|GRE)-", re.IGNORECASE)
_REGION_ALIASES: dict[str, str] = {
    "MADREDEDIOS": "MADRE DE DIOS",
    "LIMA PROVINCIA": "LIMA PROVINCIAS",
    "NO REGISTRA": "",
    "NONE": "",
}

def _strip_accents(text: str) -> str:
    import unicodedata
    return "".join(c for c in unicodedata.normalize("NFKD", text) if unicodedata.category(c) != "Mn")

def _normalize_bbdd_row(columns: list[str], row: list[Any]) -> list[Any]:
    """Normaliza region, tipo_iged y nombre_iged antes de insertar en Railway."""
    row = list(row)
    if "region" in columns:
        idx = columns.index("region")
        val = _strip_accents(str(row[idx] or "").strip()).upper()
        val = " ".join(val.split())
        val = _REGION_ALIASES.get(val, val)
        row[idx] = val
    if "tipo_iged" in columns:
        idx = columns.index("tipo_iged")
        val = str(row[idx] or "").strip()
        if val.replace(" ", "").upper() == "DRE/GRE":
            row[idx] = "DRE/GRE"
    if "nombre_iged" in columns:
        idx = columns.index("nombre_iged")
        val = str(row[idx] or "").strip()
        val = _DRE_GRE_PREFIX_RE.sub(lambda m: m.group(1) + " ", val)
        if val.upper() == "DRE LIMA PROVINCIA":
            val = "DRE LIMA PROVINCIAS"
        row[idx] = val
    return row


def build_upsert_sql(table_name: str, columns: list[str], primary_keys: list[str]) -> str:
    table_sql = quote_identifier(table_name)
    column_sql = ", ".join(quote_identifier(column) for column in columns)
    placeholders = ", ".join(["%s"] * len(columns))
    updatable = [column for column in columns if column not in primary_keys]
    if not primary_keys or not updatable:
        return f"INSERT INTO {table_sql} ({column_sql}) VALUES ({placeholders})"
    update_sql = ", ".join(
        f"{quote_identifier(column)}=VALUES({quote_identifier(column)})" for column in updatable
    )
    return f"INSERT INTO {table_sql} ({column_sql}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {update_sql}"


def execute_batch_skipping_conflicts(
    cursor: Any,
    connection: Any,
    table_name: str,
    upsert_sql: str,
    rows: list[list[Any]],
) -> int:
    """Ejecuta un lote de upserts; si el lote falla, reintenta fila por fila.

    Filas que violan una restriccion unica ajena a su clave primaria (ej:
    emails duplicados en cuentas legado del Aula Virtual) se registran y se
    omiten, en vez de abortar toda la sincronizacion.
    """
    try:
        cursor.executemany(upsert_sql, rows)
        connection.commit()
        return len(rows)
    except MySQLError:
        connection.rollback()

    synced = 0
    for row in rows:
        try:
            cursor.execute(upsert_sql, row)
            connection.commit()
            synced += 1
        except MySQLError as exc:
            connection.rollback()
            print(f"[WARN] {table_name}: fila omitida por conflicto ({exc})")
    return synced


def _pk_gt(a: Any, b: Any) -> bool:
    try:
        return int(a) > int(b)
    except (ValueError, TypeError):
        return str(a) > str(b)


def build_active_specs() -> dict[str, TableSpec]:
    specs = dict(TABLE_SPECS)
    if not env_flag("SYNC_INCLUDE_SURVEY_ANSWER", default=False):
        specs.pop("c_survey_answer", None)
    if not env_flag("SYNC_INCLUDE_STUDENT_PUBLICATION", default=False):
        specs.pop("c_student_publication", None)
    if not env_flag("SYNC_INCLUDE_SURVEYS", default=False):
        specs.pop("c_survey", None)
        specs.pop("c_survey_question", None)
        specs.pop("c_survey_question_option", None)
    return specs


def sync_dump_file(
    dump_key: str,
    sql_file: Path,
    target_connection: Any,
    state: dict[str, Any],
    batch_size: int,
    cutoff_text: str,
    reset_target: bool,
    aula_scope: AulaScope | None = None,
    safety_cutoff: str | None = None,
    integer_safety_margin: int = 0,
) -> None:
    active_specs = {name: spec for name, spec in build_active_specs().items() if spec.dump_key == dump_key}
    if not sql_file.is_file():
        raise FileNotFoundError(f"No se encontro el archivo SQL: {sql_file}")

    trim_initial_rows = should_trim_initial_aula_rows(dump_key, aula_scope)
    initial_row_totals = collect_initial_row_totals(sql_file, active_specs, state, aula_scope)

    print(f"[INFO] Procesando dump {dump_key}: {sql_file}")
    table_meta: dict[str, dict[str, Any]] = {}
    target_cursor = target_connection.cursor()

    try:
        for statement in iter_sql_statements(sql_file):
            create_match = CREATE_RE.match(statement)
            if create_match:
                table_name = create_match.group("table")
                spec = active_specs.get(table_name)
                if not spec:
                    continue
                state_key = f"{dump_key}.{table_name}"
                state_entry = state.setdefault("tables", {}).get(state_key)
                primary_keys = ensure_target_table(target_connection, table_name, statement, reset_target and state_entry is None)
                if not primary_keys and spec.force_primary_key:
                    primary_keys = apply_forced_primary_key(target_connection, table_name, spec.force_primary_key)
                initial_load = needs_initial_load(state_entry)
                table_meta[table_name] = {
                    "spec": spec,
                    "primary_keys": primary_keys,
                    "upsert_sql": None,
                    "rows_synced": 0,
                    "initial_load": initial_load,
                    "rows_seen": 0,
                    "rows_to_skip": calculate_rows_to_skip(
                        initial_row_totals.get(table_name, 0),
                        resolve_table_keep_fraction(table_name, initial_row_totals.get(table_name, 0)),
                    )
                    if trim_initial_rows and initial_load and spec.initial_mode == "last_fraction"
                    else 0,
                    "last_marker": state_entry.get("last_marker") if state_entry else None,
                    "last_pk": state_entry.get("last_pk") if state_entry else None,
                }
                continue

            insert_parts = extract_insert_parts(statement)
            if not insert_parts:
                continue

            table_name, columns, values_sql = insert_parts
            spec = active_specs.get(table_name)
            if not spec:
                continue

            state_key = f"{dump_key}.{table_name}"
            state_entry = state.setdefault("tables", {}).get(state_key)
            meta = table_meta.setdefault(
                table_name,
                {
                    "spec": spec,
                    "primary_keys": [],
                    "upsert_sql": None,
                    "rows_synced": 0,
                    "initial_load": needs_initial_load(state_entry),
                    "rows_seen": 0,
                    "rows_to_skip": calculate_rows_to_skip(
                        initial_row_totals.get(table_name, 0),
                        resolve_table_keep_fraction(table_name, initial_row_totals.get(table_name, 0)),
                    )
                    if trim_initial_rows and needs_initial_load(state_entry) and spec.initial_mode == "last_fraction"
                    else 0,
                    "last_marker": state_entry.get("last_marker") if state_entry else None,
                    "last_pk": state_entry.get("last_pk") if state_entry else None,
                },
            )
            if meta["upsert_sql"] is None:
                meta["upsert_sql"] = build_upsert_sql(table_name, columns, meta["primary_keys"])

            pending_rows: list[list[Any]] = []
            for row in iter_insert_rows(values_sql):
                row_map = dict(zip(columns, row))
                if dump_key == "aula" and not row_matches_aula_scope(table_name, row_map, aula_scope):
                    continue

                meta["rows_seen"] += 1
                marker_value = choose_marker(row_map, spec)
                pk_value = choose_pk_value(row_map, meta["primary_keys"])

                if meta["initial_load"]:
                    if trim_initial_rows and spec.initial_mode == "last_fraction" and meta["rows_seen"] <= meta["rows_to_skip"]:
                        continue
                    if trim_initial_rows and not marker_passes_initial_window(spec, marker_value, cutoff_text):
                        continue
                else:
                    if not marker_is_newer(spec, marker_value, pk_value, state_entry):
                        if not _within_safety_window(
                            spec,
                            marker_value,
                            safety_cutoff,
                            last_marker=state_entry.get("last_marker") if state_entry else None,
                            integer_safety_margin=integer_safety_margin,
                        ):
                            continue

                if table_name in ("bbdd_difoca", "H2017-2025", "bbdd_2026"):
                    row = _normalize_bbdd_row(columns, row)
                pending_rows.append(row)

                if spec.marker_kind == "datetime":
                    current_marker = serialize_marker(marker_value, spec.marker_kind)
                    if current_marker is not None:
                        if meta["last_marker"] is None or str(current_marker) > str(meta["last_marker"]):
                            meta["last_marker"] = current_marker
                            meta["last_pk"] = pk_value
                        elif str(current_marker) == str(meta["last_marker"]) and pk_value is not None:
                            if meta["last_pk"] is None or _pk_gt(pk_value, meta["last_pk"]):
                                meta["last_pk"] = pk_value
                    if pk_value is not None and (meta["last_pk"] is None or _pk_gt(pk_value, meta["last_pk"])):
                        meta["last_pk"] = pk_value
                elif spec.marker_kind == "integer":
                    current_marker = serialize_marker(marker_value, spec.marker_kind)
                    if current_marker is not None:
                        if meta["last_marker"] is None or int(current_marker) > int(meta["last_marker"]):
                            meta["last_marker"] = current_marker
                    if pk_value is not None and (meta["last_pk"] is None or _pk_gt(pk_value, meta["last_pk"])):
                        meta["last_pk"] = pk_value
                elif pk_value is not None and (meta["last_pk"] is None or _pk_gt(pk_value, meta["last_pk"])):
                    meta["last_pk"] = pk_value

                if len(pending_rows) >= batch_size:
                    meta["rows_synced"] += execute_batch_skipping_conflicts(
                        target_cursor, target_connection, table_name, meta["upsert_sql"], pending_rows,
                    )
                    print(f"[INFO] {dump_key}.{table_name}: {meta['rows_synced']} filas sincronizadas")
                    pending_rows.clear()

            if pending_rows:
                meta["rows_synced"] += execute_batch_skipping_conflicts(
                    target_cursor, target_connection, table_name, meta["upsert_sql"], pending_rows,
                )
                print(f"[INFO] {dump_key}.{table_name}: {meta['rows_synced']} filas sincronizadas")

            state.setdefault("tables", {})[state_key] = {
                "last_marker": meta["last_marker"],
                "last_pk": meta["last_pk"],
                "marker_kind": spec.marker_kind,
                "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "initial_mode": spec.initial_mode,
            }
            save_state(STATE_FILE, state)
    finally:
        target_cursor.close()


def fill_missing_enrolled_users(aula_sql: Path, target_connection: Any) -> int:
    """
    Detecta usuarios que están en course_rel_user de Railway pero no en la tabla user,
    los busca en el dump de Aula y los inserta.
    Retorna el número de usuarios insertados.
    """
    cursor = target_connection.cursor()
    try:
        cursor.execute(
            """
            SELECT DISTINCT cru.user_id
            FROM course_rel_user cru
            LEFT JOIN user u ON u.user_id = cru.user_id
            WHERE u.user_id IS NULL
            ORDER BY cru.user_id
            """
        )
        missing_ids = {row[0] for row in cursor.fetchall()}
    finally:
        cursor.close()

    if not missing_ids:
        return 0

    print(f"[INFO] Usuarios matriculados sin perfil en Railway: {len(missing_ids)}. Buscando en dump...")

    # Buscar sus registros en el dump de Aula
    columns: list[str] = []
    found_rows: list[list[Any]] = []

    for statement in iter_sql_statements(aula_sql):
        create_match = CREATE_RE.match(statement)
        if create_match and create_match.group("table") == "user":
            columns = []
            continue

        insert_parts = extract_insert_parts(statement)
        if not insert_parts:
            continue
        table_name, stmt_columns, values_sql = insert_parts
        if table_name != "user":
            continue

        if not columns:
            columns = stmt_columns

        for row in iter_insert_rows(values_sql):
            row_map = dict(zip(stmt_columns, row))
            uid = row_map.get("user_id")
            try:
                uid_int = int(uid)
            except (TypeError, ValueError):
                continue
            if uid_int in missing_ids:
                found_rows.append(row)
                missing_ids.discard(uid_int)

        if not missing_ids:
            break

    if not found_rows or not columns:
        if missing_ids:
            print(f"[WARN] {len(missing_ids)} usuarios matriculados no encontrados en el dump. "
                  "Posiblemente fueron eliminados del Aula Virtual.")
        return 0

    # Columnas sin 'id' (auto_increment en Railway puede diferir)
    cols_no_id = [c for c in columns if c != "id"]
    id_index = columns.index("id") if "id" in columns else None

    # Columnas que se actualizan si el usuario ya existía con otro username
    update_cols = [
        "username", "username_canonical", "email_canonical", "email",
        "official_code", "firstname", "lastname", "phone", "status",
        "enabled", "active", "last_login", "updated_at",
    ]
    update_cols_present = [c for c in update_cols if c in cols_no_id]
    col_list_sql = ", ".join(f"`{c}`" for c in cols_no_id)
    placeholders = ", ".join(["%s"] * len(cols_no_id))
    update_sql = ", ".join(f"`{c}`=VALUES(`{c}`)" for c in update_cols_present)
    upsert_sql = (
        f"INSERT INTO `user` ({col_list_sql}) VALUES ({placeholders}) "
        f"ON DUPLICATE KEY UPDATE {update_sql}"
    )

    inserted = 0
    cursor = target_connection.cursor()
    try:
        for row in found_rows:
            values = [row[i] for i, c in enumerate(columns) if c != "id"]
            cursor.execute(upsert_sql, values)
            if cursor.rowcount == 1:
                inserted += 1
        target_connection.commit()
    finally:
        cursor.close()

    print(f"[INFO] fill_missing_enrolled_users: {inserted} usuarios insertados en Railway.")
    if missing_ids:
        print(f"[WARN] {len(missing_ids)} usuarios siguen sin perfil (no estaban en el dump).")

    return inserted


def fill_missing_exercise_users(aula_sql: Path, target_connection: Any) -> int:
    """
    Igual que fill_missing_enrolled_users, pero para participantes que solo
    aparecen en track_e_exercises (ej: tomaron un ejercicio sin pasar por
    course_rel_user, como sesiones o inscripciones que no matriculan al curso).
    """
    cursor = target_connection.cursor()
    try:
        cursor.execute(
            """
            SELECT DISTINCT t.exe_user_id
            FROM track_e_exercises t
            LEFT JOIN user u ON u.user_id = t.exe_user_id
            WHERE u.user_id IS NULL AND t.exe_user_id IS NOT NULL
            ORDER BY t.exe_user_id
            """
        )
        missing_ids = {row[0] for row in cursor.fetchall()}
    finally:
        cursor.close()

    if not missing_ids:
        return 0

    print(f"[INFO] Usuarios con intentos de ejercicio sin perfil en Railway: {len(missing_ids)}. Buscando en dump...")

    columns: list[str] = []
    found_rows: list[list[Any]] = []

    for statement in iter_sql_statements(aula_sql):
        create_match = CREATE_RE.match(statement)
        if create_match and create_match.group("table") == "user":
            columns = []
            continue

        insert_parts = extract_insert_parts(statement)
        if not insert_parts:
            continue
        table_name, stmt_columns, values_sql = insert_parts
        if table_name != "user":
            continue

        if not columns:
            columns = stmt_columns

        for row in iter_insert_rows(values_sql):
            row_map = dict(zip(stmt_columns, row))
            uid = row_map.get("user_id")
            try:
                uid_int = int(uid)
            except (TypeError, ValueError):
                continue
            if uid_int in missing_ids:
                found_rows.append(row)
                missing_ids.discard(uid_int)

        if not missing_ids:
            break

    if not found_rows or not columns:
        if missing_ids:
            print(f"[WARN] {len(missing_ids)} usuarios (por ejercicio) no encontrados en el dump. "
                  "Posiblemente fueron eliminados del Aula Virtual.")
        return 0

    cols_no_id = [c for c in columns if c != "id"]
    update_cols = [
        "username", "username_canonical", "email_canonical", "email",
        "official_code", "firstname", "lastname", "phone", "status",
        "enabled", "active", "last_login", "updated_at",
    ]
    update_cols_present = [c for c in update_cols if c in cols_no_id]
    col_list_sql = ", ".join(f"`{c}`" for c in cols_no_id)
    placeholders = ", ".join(["%s"] * len(cols_no_id))
    update_sql = ", ".join(f"`{c}`=VALUES(`{c}`)" for c in update_cols_present)
    upsert_sql = (
        f"INSERT INTO `user` ({col_list_sql}) VALUES ({placeholders}) "
        f"ON DUPLICATE KEY UPDATE {update_sql}"
    )

    inserted = 0
    cursor = target_connection.cursor()
    try:
        for row in found_rows:
            values = [row[i] for i, c in enumerate(columns) if c != "id"]
            try:
                cursor.execute(upsert_sql, values)
                target_connection.commit()
                if cursor.rowcount == 1:
                    inserted += 1
            except MySQLError as exc:
                target_connection.rollback()
                print(f"[WARN] user (por ejercicio): fila omitida por conflicto ({exc})")
    finally:
        cursor.close()

    print(f"[INFO] fill_missing_exercise_users: {inserted} usuarios insertados en Railway.")
    if missing_ids:
        print(f"[WARN] {len(missing_ids)} usuarios (por ejercicio) siguen sin perfil (no estaban en el dump).")
    return inserted


def prune_orphaned_course_rel_user(
    aula_sql: Path,
    target_connection: Any,
    aula_scope: AulaScope | None,
) -> int:
    """Elimina en Railway filas de course_rel_user que ya no existen en el dump de origen.

    El sync incremental solo hace upsert (nunca DELETE): si en Aula Virtual se elimina
    una matricula (retiro, limpieza de duplicados), Railway se queda con una fila
    "fantasma" para siempre. Esta funcion la detecta y la borra, acotado a los cursos
    del filtro (aula_scope) para no barrer datos fuera de alcance.
    """
    if aula_scope is None or aula_scope.course_filter is None:
        return 0

    ids_en_dump: set[int] = set()
    for statement in iter_sql_statements(aula_sql):
        insert_parts = extract_insert_parts(statement)
        if not insert_parts:
            continue
        table_name, columns, values_sql = insert_parts
        if table_name != "course_rel_user":
            continue
        for row in iter_insert_rows(values_sql):
            row_map = dict(zip(columns, row))
            if not aula_scope.course_filter.matches(row_map.get("c_id")):
                continue
            try:
                ids_en_dump.add(int(row_map.get("id")))
            except (TypeError, ValueError):
                continue

    cursor = target_connection.cursor()
    try:
        cursor.execute("SELECT id, c_id FROM course_rel_user")
        railway_rows = cursor.fetchall()
    finally:
        cursor.close()

    def _row_id(row: Any) -> Any:
        return row["id"] if isinstance(row, dict) else row[0]

    def _row_c_id(row: Any) -> Any:
        return row["c_id"] if isinstance(row, dict) else row[1]

    ids_a_borrar = [
        _row_id(row)
        for row in railway_rows
        if aula_scope.course_filter.matches(_row_c_id(row)) and _row_id(row) not in ids_en_dump
    ]
    if not ids_a_borrar:
        print("[INFO] prune_orphaned_course_rel_user: sin filas fantasma para eliminar.")
        return 0

    cursor = target_connection.cursor()
    try:
        placeholders = ", ".join(["%s"] * len(ids_a_borrar))
        cursor.execute(f"DELETE FROM course_rel_user WHERE id IN ({placeholders})", ids_a_borrar)
        target_connection.commit()
    finally:
        cursor.close()

    print(f"[INFO] prune_orphaned_course_rel_user: {len(ids_a_borrar)} filas fantasma eliminadas de Railway.")
    return len(ids_a_borrar)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sincroniza dumps SQL locales hacia Railway con estado incremental local.")
    parser.add_argument("--batch-size", type=int, default=env_int("SYNC_BATCH_SIZE", default=1000))
    parser.add_argument("--lookback-days", type=int, default=env_int("SYNC_LOOKBACK_DAYS", default=90))
    parser.add_argument("--safety-days", type=int, default=env_int("SYNC_SAFETY_DAYS", default=21))
    parser.add_argument(
        "--integer-safety-margin",
        type=int,
        default=env_int("SYNC_INTEGER_SAFETY_MARGIN", default=3000),
        help="Reprocesa filas con marcador entero hasta este margen por debajo del ultimo id sincronizado.",
    )
    parser.add_argument(
        "--aula-course-filter",
        default=env_value("SYNC_AULA_COURSE_FILTER"),
        help="Filtra Aula por c_id. Acepta valores como 305,311,315+ o 315-340.",
    )
    parser.add_argument(
        "--reset-target",
        action="store_true",
        default=env_flag("SYNC_RESET_TARGET", default=False),
        help="Recrea tablas destino solo en la primera corrida de cada tabla.",
    )
    return parser.parse_args()


def main() -> int:
    load_env_file(ENV_FILE)
    args = parse_args()

    try:
        course_filter = parse_course_filter(args.aula_course_filter)
    except ValueError as exc:
        print(f"[ERROR] No se pudo interpretar SYNC_AULA_COURSE_FILTER: {exc}", file=sys.stderr)
        return 1

    target_config = build_target_config()
    aula_sql = resolve_dump_path(env_value("AULA_SQL_PATH"), DEFAULT_AULA_SQL)
    sidi_sql = resolve_dump_path(env_value("SIDI_SQL_PATH"), DEFAULT_SIDI_SQL)
    manifest_path = resolve_dump_path(env_value("BACKUP_MANIFEST_PATH"), aula_sql.parent / "ULTIMO_BACKUP.json")

    if not validate_dump_integrity(aula_sql, manifest_path):
        print("[ERROR] Abortando sincronizacion: el archivo Aula no paso la validacion SHA256.", file=sys.stderr)
        return 1
    if sidi_sql.is_file() and sidi_sql.stat().st_size > 500:
        if not validate_dump_integrity(sidi_sql, manifest_path):
            print("[WARN] El archivo SYS no paso la validacion SHA256. Se continuara solo con Aula.")

    cutoff_text = (datetime.now() - timedelta(days=args.lookback_days)).strftime("%Y-%m-%d %H:%M:%S")
    safety_cutoff = (datetime.now() - timedelta(days=args.safety_days)).strftime("%Y-%m-%d %H:%M:%S")
    state = load_state(STATE_FILE)
    aula_scope = build_aula_scope(aula_sql, course_filter)

    try:
        target_connection = open_target_connection_with_retry(
            target_config,
            attempts=env_int("SYNC_CONNECT_ATTEMPTS", default=5),
            delay_seconds=env_int("SYNC_CONNECT_DELAY_SECONDS", default=5),
        )
    except (ValueError, RuntimeError, MySQLError) as exc:
        print(f"[ERROR] No se pudo abrir la conexion a Railway: {exc}", file=sys.stderr)
        return 1

    print(f"[INFO] Destino Railway: {target_config.host}:{target_config.port}/{target_config.database}")
    print(f"[INFO] SQL Aula: {aula_sql}")
    print(f"[INFO] SQL SYS: {sidi_sql}")
    print(f"[INFO] Estado local: {STATE_FILE}")
    if course_filter is not None:
        normalized_filter = normalize_course_filter(args.aula_course_filter) or args.aula_course_filter
        user_count = len(aula_scope.user_ids) if aula_scope is not None else 0
        print(f"[INFO] Filtro Aula por cursos: {normalized_filter}")
        print(f"[INFO] Usuarios Aula dentro del filtro: {user_count}")

    try:
        update_sync_runtime_status(target_connection, status="running")
        if args.reset_target:
            print("[INFO] Reiniciando tablas administradas en Railway...")
            reset_managed_tables(target_connection)
        sync_dump_file(
            "aula", aula_sql, target_connection, state, args.batch_size, cutoff_text, args.reset_target,
            aula_scope, safety_cutoff, integer_safety_margin=args.integer_safety_margin,
        )
        fill_missing_enrolled_users(aula_sql, target_connection)
        fill_missing_exercise_users(aula_sql, target_connection)
        prune_orphaned_course_rel_user(aula_sql, target_connection, aula_scope)
        if env_flag("SYNC_INCLUDE_SYS", default=True):
            sync_dump_file("sys", sidi_sql, target_connection, state, args.batch_size, cutoff_text, args.reset_target, safety_cutoff=safety_cutoff)
    except (FileNotFoundError, ValueError, RuntimeError, MySQLError) as exc:
        target_connection.rollback()
        save_state(STATE_FILE, state)
        try:
            update_sync_runtime_status(target_connection, status="error", error_message=str(exc)[:500])
        except Exception:
            pass
        print(f"[ERROR] Sincronizacion fallida: {exc}", file=sys.stderr)
        return 1
    finally:
        try:
            if not target_connection.is_connected():
                target_connection = open_target_connection_with_retry(
                    target_config,
                    attempts=env_int("SYNC_CONNECT_ATTEMPTS", default=5),
                    delay_seconds=env_int("SYNC_CONNECT_DELAY_SECONDS", default=5),
                )
        except Exception:
            pass
        target_connection.close()

    save_state(STATE_FILE, state)
    try:
        target_connection = open_target_connection_with_retry(
            target_config,
            attempts=env_int("SYNC_CONNECT_ATTEMPTS", default=5),
            delay_seconds=env_int("SYNC_CONNECT_DELAY_SECONDS", default=5),
        )
        update_sync_runtime_status(
            target_connection,
            status="success",
            success_timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )
        target_connection.close()
    except Exception:
        pass
    print("[INFO] Sincronizacion completada.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())