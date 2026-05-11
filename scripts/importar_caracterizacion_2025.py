"""Importa la caracterización oficial de las capacitaciones 2025 desde Excel.

Uso:
    python scripts/importar_caracterizacion_2025.py [--apply] [--xlsx RUTA]

Sin --apply hace dry-run (no escribe en BD). Sobrescribe los valores existentes
para las capacitaciones cuyo cap_codigo aparezca en el Excel; ignora las filas
del Excel cuyo cap_codigo no exista en BD.
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import django
import pandas as pd

# Bootstrap Django.
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "app_cap_difoca.settings")
django.setup()

from core.models import Capacitacion  # noqa: E402

# Mapeo: índice de columna en Excel (fila técnica = fila 1) → campo del modelo.
# Solo se incluyen los campos que existen en el modelo y queremos cargar.
MAPEO_COLUMNAS: dict[int, str] = {
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
    25: "evidencia_comparativa",
    26: "nivel_capacidad_fortalecida",
    27: "evaluacion_eficacia_grupo_control",
    28: "tipo_inscripcion",
    30: "encuesta_satisfaccion",
    31: "incluido_reporte_cneb",
    32: "resultado_aprendizaje",
    33: "estrategia_formativa",
    34: "mentoria",
    35: "trazabilidad_servicio_educativo",
    36: "asesorias_personalizadas_colectivas",
}

# Campos binarios 0/1 → "No"/"Sí".
CAMPOS_BOOLEANOS = {
    "capacitacion_replicada",
    "capacitacion_diagnostico_previo",
    "capacitacion_virtual_sincronica",
    "autoformativo",
    "necesidad_acompanamiento",
    "capacitacion_acompanamiento",
    "monitores",
    "capacitacion_tutoria",
    "retroalimentacion",
    "acompanamiento_uo",
    "capacitacion_presencialidad",
    "acciones_sostenidas",
    "recursos_virtuales",
    "capacitacion_competencia_especifica",
    "capacitacion_aplicacion_inmediata",
    "evidencia_comparativa",
    "evaluacion_eficacia_grupo_control",
    "encuesta_satisfaccion",
    "incluido_reporte_cneb",
    "mentoria",
    "trazabilidad_servicio_educativo",
    "asesorias_personalizadas_colectivas",
}


def normaliza_bool(valor) -> str:
    """Convierte 0/1/'1'/'Sí'/'No'/'' a 'Sí' o 'No'. Vacío → ''."""
    if valor is None:
        return ""
    if isinstance(valor, float) and pd.isna(valor):
        return ""
    s = str(valor).strip().lower()
    if s in {"", "nan", "none"}:
        return ""
    if s in {"1", "1.0", "si", "sí", "true", "verdadero"}:
        return "Sí"
    if s in {"0", "0.0", "no", "false", "falso"}:
        return "No"
    return ""


def normaliza_texto(valor) -> str:
    if valor is None:
        return ""
    if isinstance(valor, float) and pd.isna(valor):
        return ""
    s = str(valor).strip()
    if s.lower() in {"nan", "none"}:
        return ""
    # Reparar encoding latin1 visible (ej. "Articulaci?n" → "Articulación")
    s = s.replace("Articulaci?n", "Articulación")
    return s


# Normaliza siglas/typos en organo_formulador al catálogo oficial.
NORMALIZA_ORGANO = {
    "DAGED": "Dirección de Apoyo a la Gestión Descentralizada",
    "DEI": "Dirección de Educación Inicial",
    "Direccion de Educación Básica Especial - DEBE": "Dirección de Educación Básica Especial - DEBE",
}


def normaliza_organo(valor: str) -> str:
    return NORMALIZA_ORGANO.get(valor, valor)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--xlsx",
        default="/Users/ovarillas/Downloads/caracteristicas de las capacitaciones 16.04.xlsx",
        help="Ruta del archivo Excel.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Escribe los cambios en la BD. Sin esta bandera solo muestra el diff.",
    )
    args = parser.parse_args()

    xlsx = Path(args.xlsx)
    if not xlsx.exists():
        print(f"ERROR: no se encontró {xlsx}", file=sys.stderr)
        return 2

    df = pd.read_excel(xlsx, sheet_name="bbdd_x_oferta", header=None)
    # Fila 0: descripciones; fila 1: nombres técnicos; fila 2+: datos.
    datos = df.iloc[2:].reset_index(drop=True)
    print(f"Filas de datos en Excel: {len(datos)}")

    matched = 0
    skipped: list[str] = []
    cambios_totales = 0
    sin_cambios = 0

    for _, fila in datos.iterrows():
        codigo_excel = normaliza_texto(fila.iloc[0])
        if not codigo_excel:
            continue
        # Excel usa "25001I-288" = cap_codigo + "-" + cap_id_curso.
        # Si no hay "-", el código completo es cap_codigo (formato "25001X").
        if "-" in codigo_excel:
            partes = [p.strip() for p in codigo_excel.split("-", 1)]
            cap_codigo = partes[0]
        else:
            cap_codigo = codigo_excel
        caps = list(Capacitacion.objects.filter(cap_codigo=cap_codigo))
        if not caps:
            skipped.append(codigo_excel)
            continue
        for cap in caps:
            matched += 1
            diffs: list[tuple[str, str, str]] = []

            for idx, campo in MAPEO_COLUMNAS.items():
                if idx >= len(fila):
                    continue
                crudo = fila.iloc[idx]
                if campo in CAMPOS_BOOLEANOS:
                    nuevo = normaliza_bool(crudo)
                else:
                    nuevo = normaliza_texto(crudo)
                    if campo == "organo_formulador":
                        nuevo = normaliza_organo(nuevo)

                if not hasattr(cap, campo):
                    continue
                actual = str(getattr(cap, campo) or "").strip()
                if not nuevo and not actual:
                    continue
                if nuevo == actual:
                    continue
                if not nuevo:
                    continue
                diffs.append((campo, actual, nuevo))

            etiqueta = cap_codigo if len(caps) == 1 else f"{cap_codigo}#pk{cap.pk}"
            if not diffs:
                sin_cambios += 1
                print(f"  = {etiqueta}: sin cambios")
                continue

            cambios_totales += len(diffs)
            print(f"  ~ {etiqueta}: {len(diffs)} cambios")
            for campo, antes, despues in diffs:
                antes_short = (antes[:60] + "…") if len(antes) > 60 else antes
                despues_short = (despues[:60] + "…") if len(despues) > 60 else despues
                print(f"      {campo}: '{antes_short}' → '{despues_short}'")

            if args.apply:
                for campo, _, despues in diffs:
                    setattr(cap, campo, despues)
                cap.save()

    print()
    print("=== RESUMEN ===")
    print(f"  Excel filas: {len(datos)}")
    print(f"  Encontradas en BD: {matched}")
    print(f"  Sin cambios: {sin_cambios}")
    print(f"  Cambios totales (campo*capacitación): {cambios_totales}")
    print(f"  Ignoradas (no existen en BD): {len(skipped)}")
    if skipped:
        print("  Códigos ignorados:")
        for c in skipped:
            print(f"    - {c}")
    if not args.apply:
        print()
        print("DRY-RUN: no se escribió nada. Re-ejecutá con --apply para aplicar.")
    else:
        print()
        print("APLICADO: cambios persistidos en la BD.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
