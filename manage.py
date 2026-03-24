#!/usr/bin/env python
"""Punto de entrada para comandos administrativos de Django."""

# Importaciones de la libreria estandar para variables de entorno y argumentos CLI.
import os
import sys


def main():
    """Configura settings y delega la ejecucion del comando a Django."""
    # Fuerza a Django a usar el archivo de configuracion de este proyecto.
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "app_cap_difoca.settings")
    try:
        # Importa el despachador de comandos solo cuando settings ya esta definido.
        from django.core.management import execute_from_command_line
    except ImportError as exc:
        # Mantiene el mensaje por defecto de Django si falta el framework.
        raise ImportError(
            "Couldn't import Django. Are you sure it's installed and "
            "available on your PYTHONPATH environment variable? Did you "
            "forget to activate a virtual environment?"
        ) from exc

    # Ejecuta el comando recibido desde terminal, por ejemplo runserver o migrate.
    execute_from_command_line(sys.argv)


# Ejecuta main solo cuando este archivo se ejecuta de forma directa.
if __name__ == "__main__":
    main()