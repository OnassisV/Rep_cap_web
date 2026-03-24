"""Clase de configuracion para la app Django accounts."""

# Clase base Django para configuracion de aplicaciones.
from django.apps import AppConfig


class AccountsConfig(AppConfig):
    """Define metadatos y valores por defecto para accounts."""

    # Tipo de clave primaria por defecto para modelos de esta app.
    default_auto_field = "django.db.models.BigAutoField"
    # Ruta/nombre Python que Django usa para registrar la app.
    name = "accounts"
