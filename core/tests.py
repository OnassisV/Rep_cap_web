"""Pruebas para la app core.

Aqui se pueden agregar casos para:
- control de acceso a rutas protegidas
- render de plantillas con nombre/rol en sesion
"""

# Import base de pruebas Django para tests unitarios/integracion futuros.
from django.test import TestCase

from core.models import Capacitacion
from core.views import _auto_actualizar_estado, _valor_por_defecto_registro_capacitacion


class EstadoCapacitacionTests(TestCase):
    def test_valor_inicial_usa_formulada(self):
        valores = _valor_por_defecto_registro_capacitacion()
        self.assertEqual(valores["cap_estado"], "Formulada")

    def test_finalizada_incompleta_2026_vuelve_a_en_proceso(self):
        cap = Capacitacion.objects.create(
            cap_nombre="Capacitación de prueba",
            cap_anio=2026,
            cap_codigo="25017I",
            cap_estado="Finalizada",
            paso_actual=3,
        )

        _auto_actualizar_estado(cap)
        cap.refresh_from_db()

        self.assertEqual(cap.cap_estado, "En proceso")

    def test_finalizada_2026_con_paso_siete_tambien_vuelve_a_en_proceso(self):
        cap = Capacitacion.objects.create(
            cap_nombre="Gestión de Riesgos - G1",
            cap_anio=2026,
            cap_codigo="26001X",
            cap_estado="Finalizada",
            paso_actual=7,
            cap_tipo="Capacitación sincrónica",
        )

        _auto_actualizar_estado(cap)
        cap.refresh_from_db()

        self.assertEqual(cap.cap_estado, "En proceso")
