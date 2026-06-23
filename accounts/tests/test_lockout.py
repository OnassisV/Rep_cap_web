"""Tests para accounts/lockout.py — LoginLockoutService."""

from django.test import TestCase, override_settings
from django.core.cache import cache

from accounts.lockout import LoginLockoutService


@override_settings(
    CACHES={
        "default": {
            "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
        }
    }
)
class LoginLockoutServiceTests(TestCase):

    def setUp(self):
        cache.clear()
        self.service = LoginLockoutService(
            max_attempts=3,
            lockout_minutes=10,
            failure_ttl_minutes=30,
        )

    def tearDown(self):
        cache.clear()

    # ── is_locked ──────────────────────────────────────────────────

    def test_usuario_nuevo_no_esta_bloqueado(self):
        locked, minutes = self.service.is_locked("usuario_test")
        self.assertFalse(locked)
        self.assertEqual(minutes, 0)

    # ── register_failure ───────────────────────────────────────────

    def test_primer_fallo_no_bloquea(self):
        locked, _ = self.service.register_failure("usuario_test")
        self.assertFalse(locked)

    def test_segundo_fallo_no_bloquea(self):
        self.service.register_failure("usuario_test")
        locked, _ = self.service.register_failure("usuario_test")
        self.assertFalse(locked)

    def test_tercer_fallo_bloquea(self):
        self.service.register_failure("usuario_test")
        self.service.register_failure("usuario_test")
        locked, minutes = self.service.register_failure("usuario_test")
        self.assertTrue(locked)
        self.assertEqual(minutes, 10)

    def test_is_locked_true_despues_de_bloqueo(self):
        for _ in range(3):
            self.service.register_failure("usuario_test")
        locked, remaining = self.service.is_locked("usuario_test")
        self.assertTrue(locked)
        self.assertGreater(remaining, 0)

    def test_fallos_son_por_usuario_independiente(self):
        for _ in range(3):
            self.service.register_failure("usuario_a")
        locked_b, _ = self.service.is_locked("usuario_b")
        self.assertFalse(locked_b)

    # ── clear ──────────────────────────────────────────────────────

    def test_clear_elimina_bloqueo(self):
        for _ in range(3):
            self.service.register_failure("usuario_test")
        self.service.clear("usuario_test")
        locked, _ = self.service.is_locked("usuario_test")
        self.assertFalse(locked)

    def test_clear_elimina_contador_de_fallos(self):
        self.service.register_failure("usuario_test")
        self.service.clear("usuario_test")
        # Después de clear, un fallo más no bloquea (contador reiniciado)
        locked, _ = self.service.register_failure("usuario_test")
        self.assertFalse(locked)

    def test_clear_usuario_sin_fallos_no_falla(self):
        # No debe lanzar excepción aunque el usuario no tenga entradas en cache
        try:
            self.service.clear("usuario_inexistente")
        except Exception as exc:
            self.fail(f"clear() lanzó excepción inesperada: {exc}")
