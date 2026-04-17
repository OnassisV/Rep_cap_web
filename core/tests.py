"""Pruebas para la app core.

Aqui se pueden agregar casos para:
- control de acceso a rutas protegidas
- render de plantillas con nombre/rol en sesion
"""

# Import base de pruebas Django para tests unitarios/integracion futuros.
from unittest.mock import patch

from django.test import TestCase

from core.indicadores_adapters import _compose_course_code
from core.models import Capacitacion
from core.views import _auto_actualizar_estado, _valor_por_defecto_registro_capacitacion


class EstadoCapacitacionTests(TestCase):
    def test_codigo_compuesto_para_kpi_usa_id_curso(self):
        self.assertEqual(_compose_course_code("25022I", "311"), "25022I-311")
        self.assertEqual(_compose_course_code("26001X", ""), "26001X")

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

    def test_2026_con_pasos_completos_pasa_a_por_finalizar(self):
        cap = Capacitacion.objects.create(
            cap_nombre="Gestión de Riesgos - G1",
            cap_anio=2026,
            cap_codigo="26001X",
            cap_estado="Finalizada",
            paso_actual=7,
            cap_tipo="Capacitación sincrónica",
        )

        with patch("core.views._cap_tiene_certificados", return_value=False):
            _auto_actualizar_estado(cap)
        cap.refresh_from_db()

        self.assertEqual(cap.cap_estado, "Por finalizar")

    def test_2026_con_certificados_y_pasos_completos_pasa_a_finalizada(self):
        cap = Capacitacion.objects.create(
            cap_nombre="Gestión de Riesgos - G2",
            cap_anio=2026,
            cap_codigo="26002X",
            cap_estado="En proceso",
            paso_actual=7,
            cap_tipo="Capacitación sincrónica",
        )

        with patch("core.views._cap_tiene_certificados", return_value=True):
            _auto_actualizar_estado(cap)
        cap.refresh_from_db()

        self.assertEqual(cap.cap_estado, "Finalizada")
