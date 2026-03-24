"""Clase de configuracion para la app Django core."""

# Clase base Django para configuracion de aplicaciones.
from django.apps import AppConfig


class CoreConfig(AppConfig):
    """Define metadatos y valores por defecto para core."""

    # Tipo de clave primaria por defecto para modelos de esta app.
    default_auto_field = "django.db.models.BigAutoField"
    # Ruta/nombre Python que Django usa para registrar la app.
    name = "core"
