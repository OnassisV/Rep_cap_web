"""Importa el catalogo oficial de caracterizacion desde el Excel 16.04.

Hoja: bbdd_x_oferta. La fila 0 contiene descripciones largas, la fila 1
contiene los nombres tecnicos por columna y desde la fila 2 vienen los datos.
Se hace match contra Capacitacion.cap_codigo.

Uso:
    python manage.py importar_caracterizacion_excel \\
        --archivo "/Users/.../caracteristicas de las capacitaciones 16.04.xlsx"
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from django.core.management.base import BaseCommand, CommandError

from core.caracterizacion_schema import (
    CAMPOS_CARACTERIZACION_BOOLEANOS,
    iterar_campos_caracterizacion,
)
from core.models import Capacitacion


# Indice de columna del Excel -> nombre del campo en el modelo Capacitacion.
# (Basado en la fila 1 del Excel "16.04", hoja bbdd_x_oferta.)
_MAPEO_COLUMNAS: dict[int, str] = {
    0: "cap_codigo",
    # 1: nombre (informativo, no se importa)
    2: "capacitacion_replicada",
    3: "capacitacion_diagnostico_previo",
    4: "capacitacion_virtual_sincronica",
    5: "autoformativo",
    6: "necesidad_acompanamiento",
    7: "capacitacion_acompanamiento",
    8: "monitores",
    9: "capacitacion_tutoria",
    10: "retroalimentacion",
    11: "acompanamiento_uo",
    12: "capacitacion_presencialidad",
    13: "acciones_sostenidas",
    14: "tipo_proceso_fortalecido",
    15: "proceso_principal_fortalecido",
    16: "subproceso_fortalecido",
    17: "rubro_tematico",
    18: "recursos_virtuales",
    19: "capacitacion_competencia_especifica",
    20: "capacitacion_aplicacion_inmediata",
    21: "organo_formulador",
    22: "especialista_cargo",
    23: "publico_objetivo_oferta",
    # 24: objetivo (mi_objetivo_capacitacion ya existe — se respeta)
    25: "evidencia_comparativa",
    26: "nivel_capacidad_fortalecida",
    27: "evaluacion_eficacia_grupo_control",
    28: "tipo_inscripcion",
    # 29: horas_certificacion (pt_horas ya existe — se respeta)
    30: "encuesta_satisfaccion",
    31: "incluido_reporte_cneb",
    32: "resultado_aprendizaje",
    33: "estrategia_formativa",
    34: "mentoria",
    35: "trazabilidad_servicio_educativo",
    36: "asesorias_personalizadas_colectivas",
}


def _to_si_no(valor: Any) -> str:
    """Normaliza booleano/0/1/Si/No al string 'Sí'/'No'/''."""
    if valor is None:
        return ""
    s = str(valor).strip().lower()
    if s in {"", "nan", "none"}:
        return ""
    if s in {"1", "1.0", "si", "sí", "true", "yes"}:
        return "Sí"
    if s in {"0", "0.0", "no", "false"}:
        return "No"
    # Algunos campos vienen como texto arbitrario: solo aplicamos a binarios.
    return ""


def _texto(valor: Any) -> str:
    if valor is None:
        return ""
    s = str(valor).strip()
    if s.lower() in {"nan", "none"}:
        return ""
    return s


# Normalizacion para organo_formulador (el Excel usa el nombre completo, pero
# el modelo guarda variantes; mapeo unico para campo libre).
_ORGANO_NORMALIZACION = {
    "DEI": "Dirección de Educación Inicial",
    "DAGED": "Dirección de Apoyo a la Gestión Descentralizada",
}


class Command(BaseCommand):
    help = "Importa caracterizacion oficial desde Excel 16.04 (hoja bbdd_x_oferta)."

    def add_arguments(self, parser):
        default_path = os.path.expanduser(
            "~/Downloads/caracteristicas de las capacitaciones 16.04.xlsx"
        )
        parser.add_argument(
            "--archivo",
            default=default_path,
            help="Ruta al archivo .xlsx (default: ~/Downloads/...)",
        )
        parser.add_argument(
            "--hoja",
            default="bbdd_x_oferta",
            help="Nombre de la hoja (default: bbdd_x_oferta)",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Solo muestra el plan sin guardar.",
        )

    def handle(self, *args, **opts):
        try:
            import pandas as pd
        except ImportError as exc:
            raise CommandError(
                "Se requiere pandas. Instala con: pip install pandas openpyxl"
            ) from exc

        archivo = Path(opts["archivo"]).expanduser()
        if not archivo.exists():
            raise CommandError(f"No se encontró el archivo: {archivo}")

        df = pd.read_excel(archivo, sheet_name=opts["hoja"], header=None)
        if df.shape[0] < 3:
            raise CommandError("El Excel no contiene filas de datos.")

        binarios = CAMPOS_CARACTERIZACION_BOOLEANOS
        actualizadas = 0
        no_encontradas = 0
        errores: list[str] = []

        # Datos a partir de la fila 2 (idx >= 2). La columna 0 es el codigo.
        for idx in range(2, df.shape[0]):
            row = df.iloc[idx]
            codigo = _texto(row.iloc[0])
            if not codigo:
                continue

            try:
                cap = Capacitacion.objects.filter(cap_codigo=codigo).first()
            except Exception as exc:
                errores.append(f"{codigo}: {exc}")
                continue

            if cap is None:
                no_encontradas += 1
                self.stdout.write(self.style.WARNING(
                    f"  ✗ {codigo}: no existe en BD (saltada)"
                ))
                continue

            cambios: list[str] = []
            for col_idx, campo_modelo in _MAPEO_COLUMNAS.items():
                if campo_modelo == "cap_codigo":
                    continue
                if not hasattr(cap, campo_modelo):
                    continue
                raw = row.iloc[col_idx] if col_idx < df.shape[1] else None
                if campo_modelo in binarios:
                    valor = _to_si_no(raw)
                else:
                    valor = _texto(raw)
                    if campo_modelo == "organo_formulador":
                        valor = _ORGANO_NORMALIZACION.get(valor, valor)

                actual = str(getattr(cap, campo_modelo, "") or "").strip()
                if valor != actual:
                    cambios.append(f"{campo_modelo}: '{actual}' → '{valor}'")
                    setattr(cap, campo_modelo, valor)

            if cambios:
                if not opts["dry_run"]:
                    cap.save()
                actualizadas += 1
                self.stdout.write(self.style.SUCCESS(
                    f"  ✓ {codigo}: {len(cambios)} cambios"
                ))
            else:
                self.stdout.write(f"  - {codigo}: sin cambios")

        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS(
            f"Resumen: {actualizadas} actualizadas, {no_encontradas} no encontradas, "
            f"{len(errores)} errores."
        ))
        if errores:
            for e in errores:
                self.stdout.write(self.style.ERROR(f"  ! {e}"))
        if opts["dry_run"]:
            self.stdout.write(self.style.WARNING("DRY-RUN: no se guardaron cambios."))
