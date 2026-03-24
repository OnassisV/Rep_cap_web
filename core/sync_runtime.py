"""Utilidades para exponer el estado horario de actualizacion de datos."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from django.conf import settings

from accounts.db import get_connection


logger = logging.getLogger(__name__)
SYNC_STATUS_TABLE = "sync_runtime_status"


def _local_timezone() -> ZoneInfo:
    """Retorna la zona horaria configurada para la aplicacion."""
    return ZoneInfo(getattr(settings, "TIME_ZONE", "America/Lima"))


def _local_now() -> datetime:
    """Devuelve la hora actual localizada en la zona horaria del proyecto."""
    return datetime.now(_local_timezone())


def _coerce_local_datetime(value: Any) -> datetime | None:
    """Convierte un valor de BD a datetime localizado en America/Lima."""
    if value is None:
        return None

    timezone = _local_timezone()
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone)

    text = str(value).strip()
    if not text:
        return None

    normalized = text.replace("T", " ")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(normalized, fmt).replace(tzinfo=timezone)
        except ValueError:
            continue
    return None


def _format_datetime_label(value: datetime | None) -> str:
    """Convierte un datetime a un formato legible para la barra de estado."""
    if value is None:
        return "Sin registro disponible"
    return value.strftime("%d/%m/%Y %H:%M")


def _format_countdown(delta: timedelta) -> str:
    """Resume un timedelta en horas/minutos legibles."""
    total_seconds = max(0, int(delta.total_seconds()))
    total_minutes = max(1, total_seconds // 60) if total_seconds else 0
    hours, minutes = divmod(total_minutes, 60)

    if total_seconds <= 0:
        return "Ahora"
    if hours <= 0:
        return f"{minutes} min"
    if minutes == 0:
        return f"{hours} h"
    return f"{hours} h {minutes} min"


def _build_schedule_points(base: datetime) -> list[datetime]:
    """Genera las horas programadas del dia segun la ventana horaria configurada."""
    points: list[datetime] = []
    for hour in range(settings.SYNC_STATUS_START_HOUR, settings.SYNC_STATUS_END_HOUR + 1):
        points.append(
            base.replace(
                hour=hour,
                minute=settings.SYNC_STATUS_MINUTE,
                second=0,
                microsecond=0,
            )
        )
    return points


def _previous_scheduled_run(now: datetime) -> datetime | None:
    """Retorna la ultima corrida programada previa al momento actual."""
    today_points = _build_schedule_points(now)
    previous = [point for point in today_points if point <= now]
    if previous:
        return previous[-1]

    yesterday = now - timedelta(days=1)
    return _build_schedule_points(yesterday)[-1]


def _next_scheduled_run(now: datetime) -> datetime:
    """Retorna la proxima corrida programada segun la ventana diaria."""
    for point in _build_schedule_points(now):
        if point > now:
            return point

    tomorrow = now + timedelta(days=1)
    return _build_schedule_points(tomorrow)[0]


def _read_sync_runtime_row() -> dict[str, Any] | None:
    """Lee el estado global de sincronizacion desde la base compartida."""
    query = f"""
        SELECT sync_key, sync_label, last_success_at, last_attempt_at, last_status, last_error
        FROM {SYNC_STATUS_TABLE}
        WHERE sync_key = %s
        LIMIT 1
    """
    try:
        with get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, (settings.SYNC_STATUS_KEY,))
                return cursor.fetchone()
    except Exception:
        logger.exception("Could not read sync runtime status from shared database.")
        return None


def build_sync_status_context() -> dict[str, Any]:
    """Construye el contexto de barra con ultima corrida y siguiente ventana."""
    now = _local_now()
    next_run = _next_scheduled_run(now)
    previous_run = _previous_scheduled_run(now)
    runtime_row = _read_sync_runtime_row() or {}

    last_success_at = _coerce_local_datetime(runtime_row.get("last_success_at"))
    last_attempt_at = _coerce_local_datetime(runtime_row.get("last_attempt_at"))
    last_status = str(runtime_row.get("last_status") or "unknown").strip().lower()
    last_error = str(runtime_row.get("last_error") or "").strip()

    if last_status == "running":
        sync_tone = "warning"
        sync_label = "Sincronizacion en curso"
        sync_detail = (
            f"Inicio registrado: {_format_datetime_label(last_attempt_at)}. "
            "La barra se actualizara al cerrar la corrida."
        )
    elif last_success_at is not None:
        sync_tone = "success"
        sync_label = "Ultima actualizacion"
        sync_detail = _format_datetime_label(last_success_at)
    elif last_status == "error":
        sync_tone = "danger"
        sync_label = "Ultima sincronizacion con error"
        sync_detail = _format_datetime_label(last_attempt_at)
    else:
        sync_tone = "warning"
        sync_label = "Ultima actualizacion"
        sync_detail = "Sin registro disponible"

    next_run_detail = next_run.strftime("%d/%m/%Y %H:%M")
    countdown_detail = _format_countdown(next_run - now)
    countdown_note = "Falta para la siguiente corrida programada"
    if next_run.date() != now.date():
        countdown_note = "La siguiente corrida cae en la proxima ventana del dia"

    if last_status == "error" and last_error:
        sync_detail = f"{sync_detail} | {last_error[:90]}"

    return {
        "enabled": True,
        "last_success_at": last_success_at,
        "last_success_label": _format_datetime_label(last_success_at),
        "last_attempt_label": _format_datetime_label(last_attempt_at),
        "next_run_at": next_run,
        "next_run_label": next_run_detail,
        "countdown_label": countdown_detail,
        "countdown_note": countdown_note,
        "previous_run_label": _format_datetime_label(previous_run),
        "states": [
            {
                "tone": sync_tone,
                "label": sync_label,
                "detail": sync_detail,
            },
            {
                "tone": "warning",
                "label": "Proxima actualizacion",
                "detail": next_run_detail,
            },
            {
                "tone": "success" if last_status != "error" else "danger",
                "label": "Tiempo restante",
                "detail": f"{countdown_label if (countdown_label := countdown_detail) else 'Ahora'} | {countdown_note}",
            },
        ],
    }