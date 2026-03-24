"""Definiciones de formularios Django para la interfaz de autenticacion."""

# Modulo de formularios Django para campos HTML y validacion.
from django import forms


class LoginForm(forms.Form):
    """Formulario simple de login con campos de usuario y contrasena."""

    # Campo de usuario como texto con soporte de autocompletado del navegador.
    username = forms.CharField(
        max_length=150,
        label="Usuario",
        widget=forms.TextInput(
            attrs={
                "placeholder": "Ingresa o selecciona tu usuario",  # Texto de ayuda del campo.
                "autocomplete": "username",  # Sugerencia de autofill para navegador.
                "list": "usuarios-list",  # Enlace al datalist de sugerencias.
                "class": "input-control",  # Clase CSS para estilo consistente.
            }
        ),
    )

    # Campo de contrasena renderizado con caracteres ocultos.
    password = forms.CharField(
        label="Contrasena",
        strip=False,  # Conserva caracteres originales sin recortar espacios.
        widget=forms.PasswordInput(
            attrs={
                "placeholder": "Tu clave",  # Texto de ayuda del campo.
                "autocomplete": "current-password",  # Sugerencia de autofill para navegador.
                "class": "input-control",  # Clase CSS para estilo consistente.
            }
        ),
    )
