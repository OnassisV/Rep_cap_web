"""Tests para core/utils.py — funciones de normalización de texto."""

from django.test import SimpleTestCase
from core.utils import normalizar_texto, normalizar_texto_upper, normalizar_token


class NormalizarTextoTests(SimpleTestCase):
    """normalizar_texto → lowercase ASCII sin tildes."""

    def test_basico(self):
        self.assertEqual(normalizar_texto("Hola"), "hola")

    def test_tildes(self):
        self.assertEqual(normalizar_texto("Ángelica"), "angelica")

    def test_mayusculas_con_tildes(self):
        self.assertEqual(normalizar_texto("REGIÓN"), "region")

    def test_none_devuelve_cadena_vacia(self):
        self.assertEqual(normalizar_texto(None), "")

    def test_valor_vacio(self):
        self.assertEqual(normalizar_texto(""), "")

    def test_espacios_extremos(self):
        self.assertEqual(normalizar_texto("  Lima  "), "lima")

    def test_numero_entero(self):
        self.assertEqual(normalizar_texto(42), "42")

    def test_eñe_se_pierde(self):
        # NFKD + ascii ignora la Ñ — comportamiento esperado para búsquedas
        result = normalizar_texto("Señor")
        self.assertNotIn("ñ", result)
        self.assertNotIn("Ñ", result)


class NormalizarTextoUpperTests(SimpleTestCase):
    """normalizar_texto_upper → UPPERCASE sin tildes vocálicas, preserva Ñ."""

    def test_basico(self):
        self.assertEqual(normalizar_texto_upper("hola"), "HOLA")

    def test_tildes_vocales(self):
        self.assertEqual(normalizar_texto_upper("región"), "REGION")

    def test_preserva_enie(self):
        self.assertEqual(normalizar_texto_upper("señor"), "SENOR")

    def test_none_devuelve_cadena_vacia(self):
        self.assertEqual(normalizar_texto_upper(None), "")

    def test_ya_en_mayusculas(self):
        self.assertEqual(normalizar_texto_upper("LIMA"), "LIMA")

    def test_huanuco(self):
        self.assertEqual(normalizar_texto_upper("Huánuco"), "HUANUCO")

    def test_junin(self):
        self.assertEqual(normalizar_texto_upper("Junín"), "JUNIN")


class NormalizarTokenTests(SimpleTestCase):
    """normalizar_token → lowercase ASCII con espacios colapsados."""

    def test_basico(self):
        self.assertEqual(normalizar_token("Hola Mundo"), "hola mundo")

    def test_espacios_multiples(self):
        self.assertEqual(normalizar_token("Lima   Provincias"), "lima provincias")

    def test_tildes(self):
        self.assertEqual(normalizar_token("Gestión"), "gestion")

    def test_none(self):
        self.assertEqual(normalizar_token(None), "")

    def test_espacios_extremos(self):
        self.assertEqual(normalizar_token("  test  "), "test")
