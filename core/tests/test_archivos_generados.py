"""Tests de persistencia en BD de archivos generados y configuracion nominal.

Estos flujos antes escribian al disco del contenedor (efimero en Railway):
la BD es ahora la fuente de verdad y debe sobrevivir a un redeploy.
"""

import json
import tempfile
from pathlib import Path

from django.test import TestCase

from core.legacy_adapters import (
    _guardar_config_nominal,
    _leer_config_nominal,
    _persistir_plantillas_en_bd,
    eliminar_postulantes_excel,
    guardar_postulantes_excel,
    obtener_plantilla_generada_bytes,
    obtener_plantilla_generada_info,
    obtener_postulantes_excel_bytes,
    obtener_postulantes_excel_info,
)
from core.models import ArchivoGenerado, ConfigJson


class PostulantesEnBdTests(TestCase):
    CODIGO = "26001I-288"

    def test_guardar_y_leer_postulantes(self):
        contenido = b"contenido-xlsx-simulado"
        self.assertTrue(guardar_postulantes_excel(self.CODIGO, contenido))

        info = obtener_postulantes_excel_info(self.CODIGO)
        self.assertTrue(info["exists"])
        self.assertEqual(info["size_bytes"], len(contenido))
        self.assertEqual(info["file_name"], "postulantes_288.xlsx")

        self.assertEqual(obtener_postulantes_excel_bytes(self.CODIGO), contenido)

    def test_guardar_reemplaza_contenido_previo(self):
        guardar_postulantes_excel(self.CODIGO, b"version-1")
        guardar_postulantes_excel(self.CODIGO, b"version-2-mas-larga")

        self.assertEqual(
            ArchivoGenerado.objects.filter(
                codigo="288", kind=ArchivoGenerado.Kind.POSTULANTES
            ).count(),
            1,
        )
        self.assertEqual(obtener_postulantes_excel_bytes(self.CODIGO), b"version-2-mas-larga")

    def test_eliminar_postulantes(self):
        guardar_postulantes_excel(self.CODIGO, b"datos")
        self.assertTrue(eliminar_postulantes_excel(self.CODIGO))
        self.assertFalse(obtener_postulantes_excel_info(self.CODIGO)["exists"])
        self.assertEqual(obtener_postulantes_excel_bytes(self.CODIGO), b"")

    def test_codigo_vacio_no_guarda(self):
        self.assertFalse(guardar_postulantes_excel("", b"datos"))
        self.assertFalse(guardar_postulantes_excel(self.CODIGO, b""))


class ConfigNominalEnBdTests(TestCase):
    def test_round_trip_config(self):
        config = {
            "288": ["DNI", "Nombre"],
            "nominal_titles": {"288": "Reporte Nominal 288"},
            "group_names": {"288": {"1": "Grupo Inicial"}},
        }
        self.assertTrue(_guardar_config_nominal(config))
        self.assertEqual(_leer_config_nominal(), config)

    def test_guardar_actualiza_fila_unica(self):
        _guardar_config_nominal({"a": 1})
        _guardar_config_nominal({"a": 2})
        self.assertEqual(ConfigJson.objects.filter(clave="columnas_nominal_config").count(), 1)
        self.assertEqual(_leer_config_nominal(), {"a": 2})

    def test_valor_corrupto_en_bd_no_revienta(self):
        ConfigJson.objects.create(clave="columnas_nominal_config", valor="{no-es-json")
        # Con JSON corrupto en BD cae al respaldo en disco sin lanzar excepcion.
        self.assertIsInstance(_leer_config_nominal(), dict)


class PlantillaGeneradaEnBdTests(TestCase):
    CODIGO = "26001I-288"

    def _persistir_archivos_demo(self) -> dict[str, bytes]:
        contenidos = {}
        files_meta = []
        tmp_dir = Path(tempfile.mkdtemp())
        for kind in ("main", "nominal", "iged", "cumplimiento"):
            contenido = f"xlsx-{kind}".encode()
            ruta = tmp_dir / f"plantilla_{kind}.xlsx"
            ruta.write_bytes(contenido)
            contenidos[kind] = contenido
            files_meta.append(
                {
                    "kind": kind,
                    "path": str(ruta),
                    "file_name": ruta.name,
                    "size_bytes": len(contenido),
                    "exists": True,
                }
            )
        _persistir_plantillas_en_bd(self.CODIGO, 2026, files_meta)
        return contenidos

    def test_persistir_y_obtener_info(self):
        self._persistir_archivos_demo()

        info = obtener_plantilla_generada_info(self.CODIGO)
        self.assertTrue(info["exists"])
        self.assertEqual(len(info["files"]), 4)
        self.assertTrue(all(item["exists"] for item in info["files"]))
        self.assertEqual(info["file_name"], "plantilla_main.xlsx")
        self.assertTrue(info["generated_at"])

        kinds = [item["kind"] for item in info["files"]]
        self.assertEqual(kinds, ["main", "nominal", "iged", "cumplimiento"])

    def test_descarga_bytes_por_kind(self):
        contenidos = self._persistir_archivos_demo()
        for kind, esperado in contenidos.items():
            archivo = obtener_plantilla_generada_bytes(self.CODIGO, kind)
            self.assertTrue(archivo["exists"], kind)
            self.assertEqual(archivo["contenido"], esperado, kind)

    def test_regenerar_reemplaza_sin_duplicar(self):
        self._persistir_archivos_demo()
        self._persistir_archivos_demo()
        self.assertEqual(ArchivoGenerado.objects.filter(codigo="288").count(), 4)

    def test_kind_desconocido_no_existe(self):
        self._persistir_archivos_demo()
        self.assertFalse(obtener_plantilla_generada_bytes(self.CODIGO, "otro")["exists"])

    def test_sin_registros_info_vacia(self):
        info = obtener_plantilla_generada_info("26999X-999")
        self.assertFalse(info["exists"])
        self.assertEqual(info["files"], [])


class ArchivoGeneradoModeloTests(TestCase):
    def test_unicidad_codigo_kind(self):
        ArchivoGenerado.objects.create(
            codigo="288", kind="main", file_name="a.xlsx", contenido=b"x", size_bytes=1
        )
        from django.db import IntegrityError

        with self.assertRaises(IntegrityError):
            ArchivoGenerado.objects.create(
                codigo="288", kind="main", file_name="b.xlsx", contenido=b"y", size_bytes=1
            )
