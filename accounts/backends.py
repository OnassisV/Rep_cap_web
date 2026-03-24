"""Backend de autenticacion Django personalizado usando tabla legacy `usuarios`."""

# Logger estandar para trazas de exito/fallo de autenticacion.
import logging

# bcrypt se usa porque las claves legacy estan guardadas como hash bcrypt.
import bcrypt
# Settings del proyecto con mapeo de roles y valores de seguridad.
from django.conf import settings
# Utilidad para leer/actualizar la tabla local de usuarios Django.
from django.contrib.auth import get_user_model
# Clase base para backends de autenticacion personalizados.
from django.contrib.auth.backends import BaseBackend

# Utilidades locales para lectura de BD y politica de bloqueo.
from .db import LegacyDatabaseError, fetch_user_record
from .lockout import get_lockout_service


# Instancia de logger del modulo.
logger = logging.getLogger(__name__)
# Roles considerados administrativos para flag is_staff.
ADMIN_ROLES = {"Administrador"}


class LocalhostUsuariosBackend(BaseBackend):
    """Autentica contra MySQL legacy y sincroniza datos clave al usuario Django."""

    def authenticate(self, request, username=None, password=None, **kwargs):
        """Valida credenciales, aplica bloqueo y retorna usuario Django o None."""
        # Normaliza valores de entrada.
        username = str(username or "").strip()
        password = str(password or "")

        # Rechaza credenciales vacias de forma temprana.
        if not username or not password:
            self._set_error(request, "Usuario y contrasena son obligatorios.")
            return None

        # Evalua estado de bloqueo para este usuario.
        lockout_service = get_lockout_service()
        is_locked, remaining_minutes = lockout_service.is_locked(username)
        if is_locked:
            self._set_error(
                request,
                f"Cuenta bloqueada. Intenta en {remaining_minutes} minuto(s).",
            )
            return None

        # Lee registro de usuario desde BD legacy.
        try:
            user_record = fetch_user_record(username)
        except LegacyDatabaseError:
            # Si la BD cae, muestra mensaje controlado y corta el login.
            self._set_error(request, "No se pudo conectar a la base de datos.")
            return None

        # Maneja usuario inexistente.
        if not user_record:
            # Cuenta intento fallido y evalua bloqueo.
            blocked, lockout_minutes = lockout_service.register_failure(username)
            if blocked:
                self._set_error(
                    request,
                    f"Demasiados intentos fallidos. Bloqueo por {lockout_minutes} minutos.",
                )
            else:
                self._set_error(request, "Usuario o contrasena incorrectos.")
            return None

        # Lee hash bcrypt almacenado en BD.
        stored_hash = str(user_record.get("password_hash", "")).encode("utf-8")
        try:
            # Compara la clave en texto plano contra hash bcrypt.
            valid_password = bcrypt.checkpw(password.encode("utf-8"), stored_hash)
        except ValueError:
            # Hash malformado se trata como credencial invalida.
            valid_password = False

        # Maneja contrasena incorrecta.
        if not valid_password:
            # Cuenta intento fallido y evalua bloqueo.
            blocked, lockout_minutes = lockout_service.register_failure(username)
            if blocked:
                self._set_error(
                    request,
                    f"Demasiados intentos fallidos. Bloqueo por {lockout_minutes} minutos.",
                )
            else:
                self._set_error(request, "Usuario o contrasena incorrectos.")
            return None

        # Login exitoso limpia fallos y bloqueo del usuario.
        lockout_service.clear(username)

        # Construye nombre visible y rol desde campos legacy.
        display_name = str(user_record.get("especialista_cargo") or username).strip()
        role = self._map_role(user_record.get("cargo"))
        # Asegura que el usuario Django equivalente exista y quede actualizado.
        user = self._upsert_django_user(username, display_name, role)

        # Guarda datos extras de perfil en sesion para uso en UI.
        if request is not None:
            request.session["difoca_name"] = display_name
            # Rol base: conserva el maximo permiso otorgado por autenticacion.
            request.session["difoca_role_base"] = role
            # Rol efectivo: rol visual/operativo activo en esta sesion.
            request.session["difoca_role_effective"] = role
            # Alias de compatibilidad usado por vistas ya existentes.
            request.session["difoca_role"] = role

        # Registra login exitoso en logs.
        logger.info("Successful login for user %s", username)
        return user

    def get_user(self, user_id):
        """Retorna usuario Django por PK para restaurar autenticacion de sesion."""
        # Resuelve el modelo de usuario activo configurado en Django.
        user_model = get_user_model()
        try:
            # Retorna el usuario encontrado.
            return user_model.objects.get(pk=user_id)
        except user_model.DoesNotExist:
            # Retorna None si el usuario fue eliminado.
            return None

    @staticmethod
    def _map_role(cargo_value) -> str:
        """Mapea cargo legacy hacia un rol normalizado de la aplicacion."""
        # Normaliza texto de cargo para buscar en diccionario.
        cargo = str(cargo_value or "").strip().lower()
        # Retorna rol mapeado o fallback Invitado.
        return settings.ROLE_MAPPING.get(cargo, "Invitado")

    @staticmethod
    def _set_error(request, message: str) -> None:
        """Adjunta mensaje de error al request para mostrarlo en la vista."""
        if request is not None:
            setattr(request, "auth_error", message)

    @staticmethod
    def _upsert_django_user(username: str, display_name: str, role: str):
        """Crea o actualiza usuario Django local alineado con datos legacy."""
        # Resuelve el modelo de usuario activo.
        user_model = get_user_model()
        # Determina si el rol debe habilitar privilegios staff.
        is_admin = role in ADMIN_ROLES

        # Crea usuario si no existe usando datos legacy por defecto.
        user, created = user_model.objects.get_or_create(
            username=username,
            defaults={
                "first_name": display_name,
                "is_staff": is_admin,
                "is_superuser": False,
                "is_active": True,
            },
        )

        # Rastrea solo campos cambiados para guardar eficientemente.
        update_fields: list[str] = []
        if user.first_name != display_name:
            user.first_name = display_name
            update_fields.append("first_name")
        if user.is_staff != is_admin:
            user.is_staff = is_admin
            update_fields.append("is_staff")
        if user.is_superuser:
            user.is_superuser = False
            update_fields.append("is_superuser")
        if not user.is_active:
            user.is_active = True
            update_fields.append("is_active")

        # Para usuarios nuevos de Django, desactiva login por clave local.
        if created:
            user.set_unusable_password()
            update_fields.append("password")

        # Persiste solo si hay al menos un campo modificado.
        if update_fields:
            user.save(update_fields=update_fields)

        return user
