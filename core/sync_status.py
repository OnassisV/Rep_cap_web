"""Estado visible de la sincronizacion incremental Railway para la UI."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from django.conf import settings


@dataclass(frozen=True)
class SyncUiState:
    slug: str
    label: str
    tone: str
    detail: str


def _format_elapsed_seconds(total_seconds: int) -> str:
    total_seconds = max(int(total_seconds or 0), 0)
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    return f"{minutes:02d}:{seconds:02d}"


def _load_env_map(env_path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not env_path.is_file():
        return values

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip("\"'")
    return values


def _resolve_source_paths(sync_dir: Path) -> tuple[Path, Path]:
    env_map = _load_env_map(sync_dir / ".env")
    default_source_dir = sync_dir.parent.parent / "descargar tablas aula y sysdifo"
    aula_path = Path(env_map.get("AULA_SQL_PATH", default_source_dir / "edutalentos_aulavirtual.sql")).expanduser()
    sidi_path = Path(env_map.get("SIDI_SQL_PATH", default_source_dir / "db_sidifoca.sql")).expanduser()
    return aula_path, sidi_path


def _read_last_log_line(log_path: Path) -> str:
    if not log_path.is_file():
        return ""
    try:
        lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return ""
    for line in reversed(lines):
        if line.strip():
            return line.strip()
    return ""


def _read_latest_sync_log(sync_dir: Path) -> tuple[Path | None, str]:
    logs_dir = sync_dir / "logs"
    if not logs_dir.is_dir():
        return None, ""
    log_files = sorted(logs_dir.glob("sincronizacion_incremental_*.log"), key=lambda item: item.stat().st_mtime)
    if not log_files:
        return None, ""
    latest = log_files[-1]
    return latest, _read_last_log_line(latest)


def get_incremental_sync_status() -> dict[str, object]:
    runner_path = Path(getattr(settings, "INCREMENTAL_SYNC_RUNNER", "")).expanduser()
    sync_dir = runner_path.parent
    lock_dir = sync_dir / ".sincronizacion_incremental.lock"
    aula_path, sidi_path = _resolve_source_paths(sync_dir)
    latest_log_path, latest_log_line = _read_latest_sync_log(sync_dir)
    selection_enabled = bool(
        getattr(
            settings,
            "INCREMENTAL_SYNC_ON_CAP_SELECTION",
            getattr(settings, "INCREMENTAL_SYNC_ON_TEMPLATE_GENERATION", False),
        )
    )
    refresh_seconds = max(int(getattr(settings, "INCREMENTAL_SYNC_UI_REFRESH_SECONDS", 15) or 15), 5)

    source_available = runner_path.is_file() and aula_path.is_file() and sidi_path.is_file()
    source_detail = "Origen local listo para sincronizar."
    if not runner_path.is_file():
        source_detail = "No se encontro el runner de sincronizacion en esta maquina."
    elif not aula_path.is_file() or not sidi_path.is_file():
        missing_items = []
        if not aula_path.is_file():
            missing_items.append("edutalentos_aulavirtual.sql")
        if not sidi_path.is_file():
            missing_items.append("db_sidifoca.sql")
        source_detail = "Falta(n) archivo(s) fuente: " + ", ".join(missing_items) + "."

    is_running = lock_dir.exists()
    running_since_epoch = 0
    running_since = ""
    running_elapsed_seconds = 0
    if is_running:
        try:
            running_since_epoch = int(lock_dir.stat().st_mtime)
        except OSError:
            running_since_epoch = int(datetime.now().timestamp())
        running_since_dt = datetime.fromtimestamp(running_since_epoch)
        running_since = running_since_dt.strftime("%Y-%m-%d %H:%M:%S")
        running_elapsed_seconds = max(int(datetime.now().timestamp()) - running_since_epoch, 0)

    if is_running:
        sync_tone = "warning"
        sync_detail = "Hay una sincronizacion incremental corriendo ahora mismo. Generacion y descargas temporales bloqueadas."
    elif latest_log_line.endswith("Codigo: 0"):
        sync_tone = "success"
        sync_detail = "La ultima sincronizacion incremental termino correctamente."
    elif latest_log_line.endswith("Codigo: 1"):
        sync_tone = "danger"
        sync_detail = "La ultima sincronizacion incremental termino con error."
    elif source_available:
        sync_tone = "success"
        sync_detail = "No hay una sincronizacion activa; el sistema esta listo para lanzar una nueva."
    else:
        sync_tone = "danger"
        sync_detail = "La sincronizacion automatica no esta disponible desde esta maquina."

    if latest_log_path is not None:
        updated_at = datetime.fromtimestamp(latest_log_path.stat().st_mtime).strftime("%Y-%m-%d %H:%M")
        latest_detail = f"Ultimo registro: {updated_at}."
    else:
        latest_detail = "Todavia no hay historial local de sincronizacion incremental."

    return {
        "enabled": selection_enabled,
        "source_available": source_available,
        "is_running": is_running,
        "block_generation": is_running,
        "block_downloads": is_running,
        "running_since": running_since,
        "running_since_epoch": running_since_epoch,
        "running_elapsed_seconds": running_elapsed_seconds,
        "running_elapsed_label": _format_elapsed_seconds(running_elapsed_seconds),
        "auto_refresh_seconds": refresh_seconds,
        "states": [
            SyncUiState(
                slug="source",
                label="Origen local",
                tone="success" if source_available else "danger",
                detail=source_detail,
            ),
            SyncUiState(
                slug="sync",
                label="Carga incremental",
                tone=sync_tone,
                detail=sync_detail,
            ),
            SyncUiState(
                slug="latest",
                label="Historial",
                tone="success" if latest_log_path is not None else "warning",
                detail=latest_detail,
            ),
        ],
    }