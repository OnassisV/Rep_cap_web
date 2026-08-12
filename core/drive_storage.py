"""Integracion con Google Drive para alojar evidencias de casuisticas.

Las evidencias (fotos/adjuntos) no se guardan en la BD ni en el filesystem
efimero de Railway: se suben a una carpeta del Drive personal del
administrador (compartida como Editor con la cuenta de servicio) usando las
credenciales de GOOGLE_SERVICE_ACCOUNT_JSON. La BD solo guarda el ID del
archivo devuelto por Drive (ver `core.models.CasuisticaEvidencia`).

El acceso nunca se hace publico ("cualquiera con el enlace"): todo se sirve
a traves de la app, que actua de intermediaria autenticada con la cuenta de
servicio (ver `casuistica_evidencia_ver_view` en views.py).
"""

from __future__ import annotations

import io
import json
import logging
from functools import lru_cache

from django.conf import settings

logger = logging.getLogger("core")

_SCOPES = ["https://www.googleapis.com/auth/drive"]


class DriveNoConfigurado(Exception):
    """Faltan o son invalidas las credenciales de Google Drive en el entorno."""


@lru_cache(maxsize=1)
def _credenciales():
    from google.oauth2 import service_account

    raw = getattr(settings, "GOOGLE_SERVICE_ACCOUNT_JSON", "")
    if not raw:
        raise DriveNoConfigurado("GOOGLE_SERVICE_ACCOUNT_JSON no esta configurada.")
    try:
        info = json.loads(raw)
    except ValueError as exc:
        raise DriveNoConfigurado("GOOGLE_SERVICE_ACCOUNT_JSON no es un JSON valido.") from exc
    return service_account.Credentials.from_service_account_info(info, scopes=_SCOPES)


def _servicio():
    from googleapiclient.discovery import build

    return build("drive", "v3", credentials=_credenciales(), cache_discovery=False)


def _carpeta_id() -> str:
    carpeta = getattr(settings, "GOOGLE_DRIVE_FOLDER_ID", "")
    if not carpeta:
        raise DriveNoConfigurado("GOOGLE_DRIVE_FOLDER_ID no esta configurada.")
    return carpeta


def subir_evidencia(*, nombre: str, contenido: bytes, mime_type: str) -> str:
    """Sube un archivo a la carpeta de evidencias y devuelve su file_id en Drive."""
    from googleapiclient.http import MediaInMemoryUpload

    servicio = _servicio()
    media = MediaInMemoryUpload(contenido, mimetype=mime_type or "application/octet-stream", resumable=False)
    metadata = {"name": nombre or "evidencia", "parents": [_carpeta_id()]}
    archivo = servicio.files().create(body=metadata, media_body=media, fields="id").execute()
    file_id = str(archivo.get("id", ""))
    if not file_id:
        raise RuntimeError("Google Drive no devolvio un ID de archivo al subir la evidencia.")
    return file_id


def descargar_evidencia(file_id: str) -> tuple[bytes, str]:
    """Devuelve (bytes, mime_type) de una evidencia guardada en Drive."""
    from googleapiclient.http import MediaIoBaseDownload

    servicio = _servicio()
    metadata = servicio.files().get(fileId=file_id, fields="mimeType").execute()
    mime_type = str(metadata.get("mimeType", "")) or "application/octet-stream"

    buffer = io.BytesIO()
    request = servicio.files().get_media(fileId=file_id)
    downloader = MediaIoBaseDownload(buffer, request)
    listo = False
    while not listo:
        _, listo = downloader.next_chunk()
    return buffer.getvalue(), mime_type


def eliminar_evidencia(file_id: str) -> bool:
    """Borra el binario en Drive. Devuelve True si se elimino (o ya no existia)."""
    from googleapiclient.errors import HttpError

    servicio = _servicio()
    try:
        servicio.files().delete(fileId=file_id).execute()
        return True
    except HttpError as exc:
        status = getattr(getattr(exc, "resp", None), "status", None)
        if status == 404:
            # Ya no existe en Drive: se considera purgada igual.
            return True
        logger.exception("No se pudo eliminar la evidencia %s en Drive.", file_id)
        return False
