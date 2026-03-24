"""Servicio para control de intentos fallidos y bloqueo temporal de cuentas."""

# Utilidad matematica para redondear minutos restantes en mensajes UI.
import math
# Timedelta usado para calcular la expiracion del bloqueo.
from datetime import timedelta

# Valores de settings para limites de bloqueo.
from django.conf import settings
# Backend de cache para guardar fallos y tiempos de bloqueo.
from django.core.cache import cache
# Funcion now() con zona horaria.
from django.utils import timezone


class LoginLockoutService:
    """Encapsula la politica de bloqueo y su interaccion con cache."""

    def __init__(self, max_attempts: int, lockout_minutes: int, failure_ttl_minutes: int):
        # Maximo de intentos fallidos antes de bloquear cuenta.
        self.max_attempts = max_attempts
        # Duracion del bloqueo una vez alcanzado el umbral.
        self.lockout_minutes = lockout_minutes
        # TTL del contador de fallos para que los antiguos expiren solos.
        self.failure_ttl_minutes = failure_ttl_minutes

    @staticmethod
    def _failure_key(username: str) -> str:
        """Construye la clave de cache que guarda fallos de un usuario."""
        return f"auth:failures:{username}"

    @staticmethod
    def _lock_key(username: str) -> str:
        """Construye la clave de cache que guarda hasta cuando bloquea."""
        return f"auth:lock:{username}"

    def is_locked(self, username: str) -> tuple[bool, int]:
        """Verifica si un usuario esta bloqueado y retorna minutos restantes."""
        # Resuelve la clave de cache del bloqueo.
        lock_key = self._lock_key(username)
        # Lee de cache la fecha/hora de desbloqueo.
        lock_until = cache.get(lock_key)
        # Si no existe, el usuario no esta bloqueado.
        if lock_until is None:
            return False, 0

        # Compara la expiracion del bloqueo con la hora actual.
        now = timezone.now()
        # Si ya expiro, limpia la clave vieja y retorna desbloqueado.
        if now >= lock_until:
            cache.delete(lock_key)
            return False, 0

        # Calcula el tiempo restante en segundos.
        remaining_seconds = max(0, int((lock_until - now).total_seconds()))
        # Convierte a minutos redondeando hacia arriba para un mensaje mas claro.
        remaining_minutes = max(1, math.ceil(remaining_seconds / 60))
        return True, remaining_minutes

    def register_failure(self, username: str) -> tuple[bool, int]:
        """Incrementa fallos y bloquea si alcanza el umbral configurado."""
        # Resuelve la clave del contador de fallos.
        failure_key = self._failure_key(username)
        # Incrementa fallos (inicia en cero si no existe).
        attempts = int(cache.get(failure_key, 0)) + 1

        # Si llega al umbral, bloquea la cuenta.
        if attempts >= self.max_attempts:
            # Calcula fecha/hora de fin de bloqueo.
            lock_until = timezone.now() + timedelta(minutes=self.lockout_minutes)
            # Guarda la expiracion del bloqueo.
            cache.set(self._lock_key(username), lock_until, timeout=self.lockout_minutes * 60)
            # Limpia el contador de fallos al activar bloqueo.
            cache.delete(failure_key)
            return True, self.lockout_minutes

        # Si no bloquea, conserva el contador con el TTL configurado.
        cache.set(failure_key, attempts, timeout=self.failure_ttl_minutes * 60)
        return False, 0

    def clear(self, username: str) -> None:
        """Elimina claves de fallos y bloqueo tras login exitoso."""
        cache.delete(self._failure_key(username))
        cache.delete(self._lock_key(username))


def get_lockout_service() -> LoginLockoutService:
    """Crea una instancia del servicio usando valores de settings."""
    return LoginLockoutService(
        max_attempts=settings.MAX_LOGIN_ATTEMPTS,
        lockout_minutes=settings.LOGIN_LOCKOUT_MINUTES,
        failure_ttl_minutes=settings.LOCKOUT_FAILURE_TTL_MINUTES,
    )
