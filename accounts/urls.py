"""Definicion de rutas para la app accounts."""

# Utilidad Django para declarar rutas.
from django.urls import path

# Vistas que implementan login y logout.
from .views import login_view, logout_view


# Namespace para resolver URLs como `accounts:login`.
app_name = "accounts"

# Patrones de rutas locales de esta app.
urlpatterns = [
    path("login/", login_view, name="login"),  # Pantalla de login y manejo de POST.
    path("logout/", logout_view, name="logout"),  # Endpoint para cerrar sesion.
]
