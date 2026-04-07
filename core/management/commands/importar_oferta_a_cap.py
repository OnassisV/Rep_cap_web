"""
Management command: importa registros de oferta_formativa_difoca a cap_capacitaciones.

Reglas:
  - El campo 'codigo' de oferta se separa por '-':
      '26001I-315' → cap_codigo='26001I', cap_id_curso='315'
      '26001I'     → cap_codigo='26001I', cap_id_curso=''  (sincrónicas)
  - condicion se mapea a cap_estado:
      Cerrado               → Finalizada
      En implementacion     → En proceso
      (otros)               → Borrador
  - Todos los importados entran con paso_actual=7 (ya pasaron el flujo completo).
  - Si ya existe un registro con el mismo cap_codigo + cap_id_curso + cap_anio, se omite.
"""

from __future__ import annotations

from django.core.management.base import BaseCommand

from accounts.db import get_connection
from core.models import Capacitacion


CONDICION_A_ESTADO = {
    "cerrado": Capacitacion.Estado.FINALIZADA,
    "en implementacion": Capacitacion.Estado.EN_PROCESO,
    "implementacion": Capacitacion.Estado.EN_PROCESO,
    "en convocatoria": Capacitacion.Estado.EN_PROCESO,
    "diseño y planificación": Capacitacion.Estado.EN_PROCESO,
    "diagnóstico y sustento": Capacitacion.Estado.EN_PROCESO,
}

COLUMNAS = [
    "codigo",
    "anio",
    "condicion",
    "tipo_proceso_formativo",
    "denominacion_proceso_formativo",
    "especialista_cargo",
    "publico_objetivo",
    "objetivo_capacitacion",
    "capacitacion_replicada",
    "capacitacion_diagnostico_previo",
    "horas_certificacion",
]


def _split_codigo(codigo_raw: str) -> tuple[str, str]:
    """Separa 'XXXX-YYY' en (cap_codigo, cap_id_curso)."""
    parts = codigo_raw.split("-", 1)
    cap_codigo = parts[0].strip()
    cap_id_curso = parts[1].strip() if len(parts) > 1 else ""
    return cap_codigo, cap_id_curso


def _norm(val: object) -> str:
    return str(val or "").strip()


def _es_si(val: object) -> str:
    v = _norm(val).lower()
    if v in ("si", "sí", "1", "true", "yes"):
        return "Si"
    return "No"


class Command(BaseCommand):
    help = "Importa oferta_formativa_difoca → cap_capacitaciones"

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Solo muestra lo que se importaría, sin crear registros.",
        )

    def handle(self, *args, **options):
        dry_run = options["dry_run"]

        # Lee filas de oferta_formativa_difoca.
        filas = self._leer_oferta()
        if not filas:
            self.stdout.write(self.style.WARNING("No se encontraron filas en oferta_formativa_difoca."))
            return

        self.stdout.write(f"Filas leídas de oferta_formativa_difoca: {len(filas)}")

        creados = 0
        omitidos = 0

        for fila in filas:
            codigo_raw = _norm(fila.get("codigo"))
            if not codigo_raw:
                omitidos += 1
                continue

            cap_codigo, cap_id_curso = _split_codigo(codigo_raw)
            anio_raw = _norm(fila.get("anio"))
            try:
                cap_anio = int(anio_raw)
            except (ValueError, TypeError):
                self.stdout.write(self.style.WARNING(f"  SKIP {codigo_raw}: año inválido '{anio_raw}'"))
                omitidos += 1
                continue

            # Verifica si ya existe.
            existe = Capacitacion.objects.filter(
                cap_codigo=cap_codigo,
                cap_id_curso=cap_id_curso,
                cap_anio=cap_anio,
            ).exists()
            if existe:
                self.stdout.write(f"  SKIP {codigo_raw} ({cap_anio}): ya existe")
                omitidos += 1
                continue

            # Mapea condicion → estado.
            condicion = _norm(fila.get("condicion")).lower()
            cap_estado = CONDICION_A_ESTADO.get(condicion, Capacitacion.Estado.BORRADOR)

            # Nombre.
            cap_nombre = _norm(fila.get("denominacion_proceso_formativo")) or f"Capacitación {codigo_raw}"

            # Tipo.
            cap_tipo = _norm(fila.get("tipo_proceso_formativo"))

            # Especialista.
            especialista = _norm(fila.get("especialista_cargo"))

            # Decisiones.
            sol_es_replica = _es_si(fila.get("capacitacion_replicada"))
            sol_tiene_diagnostico = _es_si(fila.get("capacitacion_diagnostico_previo"))

            # Público objetivo.
            pob_tipo = _norm(fila.get("publico_objetivo"))

            # Objetivo.
            mi_objetivo = _norm(fila.get("objetivo_capacitacion"))

            # Horas.
            horas_raw = _norm(fila.get("horas_certificacion"))
            pt_horas = None
            if horas_raw:
                try:
                    pt_horas = int(float(horas_raw))
                except (ValueError, TypeError):
                    pass

            if dry_run:
                self.stdout.write(
                    f"  DRY-RUN: {codigo_raw} → codigo={cap_codigo}, id_curso={cap_id_curso}, "
                    f"anio={cap_anio}, estado={cap_estado}, tipo={cap_tipo}, nombre={cap_nombre[:40]}"
                )
            else:
                Capacitacion.objects.create(
                    cap_nombre=cap_nombre,
                    cap_codigo=cap_codigo,
                    cap_id_curso=cap_id_curso,
                    cap_tipo=cap_tipo,
                    cap_anio=cap_anio,
                    cap_estado=cap_estado,
                    paso_actual=7,
                    sol_es_replica=sol_es_replica,
                    sol_tiene_diagnostico=sol_tiene_diagnostico,
                    pob_tipo=pob_tipo,
                    mi_objetivo_capacitacion=mi_objetivo,
                    pt_horas=pt_horas,
                    creado_por=especialista or "importacion",
                    creado_nombre=especialista or "Importación automática",
                )
                self.stdout.write(self.style.SUCCESS(
                    f"  CREADO: {codigo_raw} → {cap_nombre[:50]} ({cap_estado})"
                ))

            creados += 1

        self.stdout.write("")
        action = "Se importarían" if dry_run else "Importados"
        self.stdout.write(self.style.SUCCESS(f"{action}: {creados}  |  Omitidos: {omitidos}"))

    def _leer_oferta(self) -> list[dict]:
        """Lee filas de oferta_formativa_difoca usando la conexión MySQL."""
        try:
            with get_connection() as conn:
                with conn.cursor() as cursor:
                    # Detecta columnas disponibles.
                    cursor.execute("SHOW COLUMNS FROM `oferta_formativa_difoca`")
                    cols_disponibles = {
                        str(r.get("Field", "")).strip() for r in cursor.fetchall()
                    }
                    cols_validas = [c for c in COLUMNAS if c in cols_disponibles]
                    if not cols_validas:
                        return []

                    sql = "SELECT {} FROM `oferta_formativa_difoca`".format(
                        ", ".join(f"`{c}`" for c in cols_validas)
                    )
                    cursor.execute(sql)
                    return list(cursor.fetchall())
        except Exception as exc:
            self.stderr.write(f"Error leyendo oferta_formativa_difoca: {exc}")
            return []
