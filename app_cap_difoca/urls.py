"""Enrutador URL central para la web CAP DIFOCA."""

# Importaciones Django para admin, redirecciones y definicion de rutas.
from django.contrib import admin
from django.shortcuts import redirect
from django.urls import include, path


def root_redirect(request):
    """Envia al usuario al inicio si tiene sesion, si no al login."""
    # Los usuarios autenticados van directo al inicio protegido.
    if request.user.is_authenticated:
        return redirect("core:home")

    # Los usuarios anonimos se redirigen al login.
    return redirect("accounts:login")


# Tabla global de rutas.
urlpatterns = [
    path("", root_redirect, name="root"),  # Entrada por defecto.
    path("admin/", admin.site.urls),  # Administrador Django.
    path("cuentas/", include("accounts.urls")),  # Rutas de autenticacion.
    path("app/", include("core.urls")),  # Rutas protegidas de la app.
]
