"""Vistas para los flujos de inicio y cierre de sesion."""

# Utilidades auth de Django para validar credenciales y crear/eliminar sesion.
from django.contrib.auth import authenticate, login, logout
# Decorador que exige usuario autenticado.
from django.contrib.auth.decorators import login_required
# Atajos para renderizar plantillas y retornar redirecciones.
from django.shortcuts import redirect, render
# Utilidad para validar que la URL `next` sea segura y local.
from django.utils.http import url_has_allowed_host_and_scheme

# Utilidades locales: usuarios para datalist y esquema de formulario.
from .db import fetch_usernames
from .forms import LoginForm


def login_view(request):
    """Renderiza login y autentica credenciales enviadas."""
    # Si el usuario ya tiene sesion activa, va directo al inicio.
    if request.user.is_authenticated:
        return redirect("core:home")

    # Vincula formulario con POST cuando exista; en GET queda sin vincular.
    form = LoginForm(request.POST or None)
    # Carga sugerencias de usuario desde BD legacy para mejorar UX.
    usernames = fetch_usernames()

    # Procesa envio del formulario de login.
    if request.method == "POST" and form.is_valid():
        # Extrae datos limpiados desde el formulario validado.
        username = form.cleaned_data["username"].strip()
        password = form.cleaned_data["password"]

        # Ejecuta pipeline de autenticacion (backend custom + fallbacks).
        user = authenticate(request, username=username, password=password)
        if user is not None:
            # Crea sesion autenticada para request/usuario.
            login(request, user)

            # Respeta URL next opcional si viene informada.
            next_url = request.POST.get("next") or request.GET.get("next")
            if next_url and url_has_allowed_host_and_scheme(
                url=next_url,
                allowed_hosts={request.get_host()},
                require_https=request.is_secure(),
            ):
                return redirect(next_url)

            # Destino por defecto despues de login exitoso.
            return redirect("core:home")

        # Si falla autenticacion, muestra error del backend o fallback.
        form.add_error(
            None,
            getattr(request, "auth_error", "Usuario o contrasena incorrectos."),
        )

    # Renderiza plantilla con formulario, sugerencias y next opcional.
    return render(
        request,
        "accounts/login.html",
        {
            "form": form,
            "usernames": usernames,
            "next_url": request.GET.get("next", ""),
        },
    )


@login_required
def logout_view(request):
    """Cierra sesion actual y redirige al login."""
    # Elimina datos de autenticacion de sesion.
    logout(request)
    # Envia al usuario de vuelta a la pantalla de login.
    return redirect("accounts:login")
