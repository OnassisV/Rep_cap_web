"""Tests para _normalize_region_name y _normalize_iged_name en indicadores_adapters."""

import pandas as pd
from django.test import SimpleTestCase

from core.indicadores_adapters import _normalize_region_name, _normalize_iged_name


class NormalizeRegionNameTests(SimpleTestCase):
    """Verifica que las regiones con tildes, alias y tipografías diversas se unifican."""

    def _norm(self, *values):
        return list(_normalize_region_name(pd.Series(list(values))))

    def test_huanuco_con_tilde(self):
        self.assertEqual(self._norm("HUÁNUCO"), ["HUANUCO"])

    def test_junin_con_tilde(self):
        self.assertEqual(self._norm("JUNÍN"), ["JUNIN"])

    def test_san_martin_con_tilde(self):
        self.assertEqual(self._norm("SAN MARTÍN"), ["SAN MARTIN"])

    def test_madrededios_pegado(self):
        self.assertEqual(self._norm("MADREDEDIOS"), ["MADRE DE DIOS"])

    def test_lima_provincia_sin_s(self):
        self.assertEqual(self._norm("LIMA PROVINCIA"), ["LIMA PROVINCIAS"])

    def test_no_registra_se_vuelve_vacio(self):
        self.assertEqual(self._norm("No registra"), [""])

    def test_none_string_se_vuelve_vacio(self):
        self.assertEqual(self._norm("None"), [""])

    def test_valor_nulo_se_vuelve_vacio(self):
        result = _normalize_region_name(pd.Series([None]))
        self.assertEqual(list(result), [""])

    def test_region_limpia_no_se_modifica(self):
        self.assertEqual(self._norm("LIMA"), ["LIMA"])

    def test_minusculas_se_convierten(self):
        self.assertEqual(self._norm("arequipa"), ["AREQUIPA"])

    def test_espacios_extras_se_colapsan(self):
        self.assertEqual(self._norm("  CUSCO  "), ["CUSCO"])

    def test_multiples_regiones_en_serie(self):
        result = self._norm("HUÁNUCO", "JUNÍN", "LIMA", "MADREDEDIOS")
        self.assertEqual(result, ["HUANUCO", "JUNIN", "LIMA", "MADRE DE DIOS"])


class NormalizeIgedNameTests(SimpleTestCase):
    """Verifica que los nombres de IGED se normalizan correctamente."""

    def _norm(self, *values):
        return list(_normalize_iged_name(pd.Series(list(values))))

    def test_dre_con_guion_se_normaliza(self):
        self.assertEqual(self._norm("DRE-AMAZONAS"), ["DRE AMAZONAS"])

    def test_gre_con_guion_se_normaliza(self):
        self.assertEqual(self._norm("GRE-AREQUIPA"), ["GRE AREQUIPA"])

    def test_alias_ugel_paucar_de_sarasara(self):
        self.assertEqual(self._norm("UGEL Paucar de Sarasara"), ["UGEL PAUCAR DEL SARA SARA"])

    def test_alias_dre_lima_provincia(self):
        self.assertEqual(self._norm("DRE Lima Provincia"), ["DRE LIMA PROVINCIAS"])

    def test_minusculas_se_convierten(self):
        result = self._norm("ugel lima")
        self.assertEqual(result, ["UGEL LIMA"])

    def test_tilde_removida(self):
        result = self._norm("UGEL Áncash")
        self.assertNotIn("Á", result[0])
        self.assertIn("UGEL", result[0])

    def test_valor_nulo(self):
        result = _normalize_iged_name(pd.Series([None]))
        self.assertEqual(list(result), [""])

    def test_espacios_extras_colapsados(self):
        result = self._norm("UGEL   LIMA")
        self.assertEqual(result, ["UGEL LIMA"])
