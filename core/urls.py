"""Definicion de rutas para la app core."""

# Utilidad Django para declarar rutas.
from django.urls import path

# Vistas protegidas de core.
from .views import home_view, section_detail_view, submenu_detail_view, switch_role_view


# Namespace para resolver URLs como `core:home`.
app_name = "core"

# Patrones de rutas locales de esta app.
urlpatterns = [
    path("", home_view, name="home"),  # Pagina de inicio protegida.
    path(
        "seccion/<slug:section_slug>/",
        section_detail_view,
        name="section_detail",
    ),  # Vista detalle por bloque del GeoMenu.
    path(
        "seccion/<slug:section_slug>/submenu/<slug:submenu_slug>/",
        submenu_detail_view,
        name="submenu_detail",
    ),  # Vista detalle para submenus internos de una seccion.
    path(
        "rol/cambiar/",
        switch_role_view,
        name="switch_role",
    ),  # Endpoint para cambiar modo de rol en sesion.
]
