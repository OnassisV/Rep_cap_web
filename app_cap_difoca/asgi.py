"""Punto de entrada ASGI usado por servidores asincronos."""

# Import de libreria estandar para definir variables de entorno.
import os

# Utilidad Django que construye el callable ASGI desde settings.
from django.core.asgi import get_asgi_application

# Indica a Django que modulo de settings debe cargar al iniciar.
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "app_cap_difoca.settings")

# Callable ASGI expuesto para servidores como uvicorn/daphne.
application = get_asgi_application()
