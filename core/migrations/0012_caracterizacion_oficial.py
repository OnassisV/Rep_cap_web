"""Caracterizacion oficial de capacitaciones (Excel 16.04).

Esta migracion:
1) Anade los 28 nuevos campos del catalogo oficial (21 binarios "Si/No" + 7
   clasificatorios) al modelo Capacitacion.
2) Hace backfill desde campos viejos equivalentes que se eliminan:
   - cap_direccion              -> organo_formulador  (con normalizacion)
   - pob_tipo                   -> publico_objetivo_oferta
   - sol_es_replica  (Si/No)    -> capacitacion_replicada (Si/No)
   - sol_tiene_diagnostico      -> capacitacion_diagnostico_previo
   - creado_nombre              -> especialista_cargo
   - cap_tipo contiene          -> capacitacion_virtual_sincronica = "Si"
     "sincronica"
3) Elimina los campos viejos redundantes.
"""

from django.db import migrations, models


# Mapa de normalizacion para organo_formulador (siglas -> nombre canonico).
_ORGANO_NORMALIZACION = {
    "DEI": "Direccion de Educacion Inicial",
    "Direccion de Educacion Inicial ": "Direccion de Educacion Inicial",
    "DAGED": "Direccion de Apoyo a la Gestion Descentralizada",
    "DIGEGED - PROGRAMA PRESUPUESTAL ": "DIGEGED - PROGRAMA PRESUPUESTAL",
}


def _normalizar_si_no(valor: str) -> str:
    v = str(valor or "").strip().lower()
    if v in {"si", "sí", "1", "true", "yes"}:
        return "Sí"
    if v in {"no", "0", "false"}:
        return "No"
    return ""


def _backfill_caracterizacion(apps, schema_editor):
    """Copia datos de campos viejos a los nuevos antes de eliminarlos."""
    Capacitacion = apps.get_model("core", "Capacitacion")
    for cap in Capacitacion.objects.all().iterator():
        # Direccion -> organo_formulador (con normalizacion).
        direccion = (getattr(cap, "cap_direccion", "") or "").strip()
        if direccion:
            cap.organo_formulador = _ORGANO_NORMALIZACION.get(direccion, direccion)

        # Publico objetivo (texto libre, antes era list_multi separado por ", ").
        pob_tipo = (getattr(cap, "pob_tipo", "") or "").strip()
        if pob_tipo:
            cap.publico_objetivo_oferta = pob_tipo

        # Decisiones binarias (Si/No).
        cap.capacitacion_replicada = _normalizar_si_no(getattr(cap, "sol_es_replica", ""))
        cap.capacitacion_diagnostico_previo = _normalizar_si_no(
            getattr(cap, "sol_tiene_diagnostico", "")
        )

        # Sincronicas: detecta por cap_tipo.
        if "sincr" in (cap.cap_tipo or "").lower():
            cap.capacitacion_virtual_sincronica = "Sí"

        # Especialista: usa creado_nombre como semilla.
        if not cap.especialista_cargo and (cap.creado_nombre or "").strip():
            cap.especialista_cargo = cap.creado_nombre.strip()

        cap.save(update_fields=[
            "organo_formulador",
            "publico_objetivo_oferta",
            "capacitacion_replicada",
            "capacitacion_diagnostico_previo",
            "capacitacion_virtual_sincronica",
            "especialista_cargo",
        ])


def _noop_reverse(apps, schema_editor):
    """Reverso vacio: los campos viejos se recrearian sin datos."""
    return None


_NUEVOS_BOOLEANOS = [
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
]


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0011_capacitacion_cert_pdf_emitido"),
    ]

    operations = (
        # 1) Nuevos campos binarios (Si/No).
        [
            migrations.AddField(
                model_name="capacitacion",
                name=name,
                field=models.CharField(blank=True, default="", max_length=4),
            )
            for name in _NUEVOS_BOOLEANOS
        ]
        + [
            # 2) Nuevos campos clasificatorios.
            migrations.AddField(
                model_name="capacitacion",
                name="organo_formulador",
                field=models.CharField(blank=True, default="", max_length=255),
            ),
            migrations.AddField(
                model_name="capacitacion",
                name="especialista_cargo",
                field=models.CharField(blank=True, default="", max_length=200),
            ),
            migrations.AddField(
                model_name="capacitacion",
                name="publico_objetivo_oferta",
                field=models.TextField(blank=True, default=""),
            ),
            migrations.AddField(
                model_name="capacitacion",
                name="tipo_proceso_fortalecido",
                field=models.CharField(blank=True, default="", max_length=80),
            ),
            migrations.AddField(
                model_name="capacitacion",
                name="proceso_principal_fortalecido",
                field=models.CharField(blank=True, default="", max_length=10),
            ),
            migrations.AddField(
                model_name="capacitacion",
                name="subproceso_fortalecido",
                field=models.CharField(blank=True, default="", max_length=255),
            ),
            migrations.AddField(
                model_name="capacitacion",
                name="rubro_tematico",
                field=models.CharField(blank=True, default="", max_length=255),
            ),
            migrations.AddField(
                model_name="capacitacion",
                name="nivel_capacidad_fortalecida",
                field=models.CharField(blank=True, default="", max_length=30),
            ),
            migrations.AddField(
                model_name="capacitacion",
                name="tipo_inscripcion",
                field=models.CharField(blank=True, default="", max_length=20),
            ),
            migrations.AddField(
                model_name="capacitacion",
                name="resultado_aprendizaje",
                field=models.CharField(blank=True, default="", max_length=80),
            ),
            migrations.AddField(
                model_name="capacitacion",
                name="estrategia_formativa",
                field=models.CharField(blank=True, default="", max_length=80),
            ),
            # 3) Backfill de viejos -> nuevos (antes de eliminar).
            migrations.RunPython(_backfill_caracterizacion, _noop_reverse),
            # 4) Eliminar campos redundantes ya migrados.
            migrations.RemoveField(model_name="capacitacion", name="cap_direccion"),
            migrations.RemoveField(model_name="capacitacion", name="pob_tipo"),
            migrations.RemoveField(model_name="capacitacion", name="pob_ambito"),
            migrations.RemoveField(model_name="capacitacion", name="sol_es_replica"),
            migrations.RemoveField(model_name="capacitacion", name="sol_tiene_diagnostico"),
        ]
    )
