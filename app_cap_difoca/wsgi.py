"""Punto de entrada WSGI usado por servidores sincronos de produccion."""

# Import de libreria estandar para definir variables de entorno.
import os

# Utilidad Django que construye el callable WSGI desde settings.
from django.core.wsgi import get_wsgi_application

# Indica a Django que modulo de settings debe cargar al iniciar.
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "app_cap_difoca.settings")

# Callable WSGI expuesto para servidores como gunicorn/uWSGI.
application = get_wsgi_application()
