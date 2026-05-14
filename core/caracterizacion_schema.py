"""Esquema declarativo de la caracterización oficial de una capacitación.

Refleja el catálogo del Excel "caracteristicas de las capacitaciones 16.04.xlsx":
- 21 indicadores binarios (Sí/No) sobre el diseño formativo.
- 7 campos clasificatorios (texto libre o desplegable controlado).
- Reutiliza los nombres de columnas usados en otras herramientas del aplicativo
  (especialista_cargo, publico_objetivo_oferta, organo_formulador, etc.) para
  mantener consistencia con `app_difoca` y `oferta_formativa_difoca`.

El renderer del Editar capacitación itera estas secciones para mostrar el form.
"""

from typing import Any


# ---------------------------------------------------------------------------
# Opciones canónicas para los desplegables.
# ---------------------------------------------------------------------------
OPCIONES_SI_NO: list[str] = ["Sí", "No"]

OPCIONES_TIPO_PROCESO_FORTALECIDO: list[str] = [
    "Proceso estratégico",
    "Proceso misional",
    "Proceso de soporte",
]

OPCIONES_PROCESO_PRINCIPAL: list[str] = ["PE1", "PE2", "PE3", "PM1", "PM3", "PO4"]

# Lista canónica de órganos formuladores (normalizada: DEI→Educación Inicial,
# DAGED→Apoyo a la Gestión Descentralizada, etc.).
OPCIONES_ORGANO_FORMULADOR: list[str] = [
    "Dirección de Fortalecimiento de Capacidades",
    "Oficina General de Transparencia Ética Pública y Anticorrupción",
    "Dirección de Educación Inicial",
    "Dirección de Educación Básica Especial - DEBE",
    "Dirección de Formación Docente en Servicio - Dirección de Educación Básica Alternativa",
    "Dirección de Servicios Educativos en el Ámbito Rural del Ministerio de Educación",
    "Dirección de Educación Intercultural Bilingüe",
    "Dirección de Educación Física y Deporte",
    "Dirección de Apoyo a la Gestión Descentralizada",
    "DIGEGED - PROGRAMA PRESUPUESTAL",
]

OPCIONES_NIVEL_CAPACIDAD: list[str] = ["Conceptual", "Procedimental", "Actitudinal"]

OPCIONES_TIPO_INSCRIPCION: list[str] = ["Cerrada", "Abierta", "Mixta"]

OPCIONES_RESULTADO_APRENDIZAJE: list[str] = [
    "Nivel 1 - Conocimientos.",
    "Nivel 2 - Comportamiento.",
    "Nivel 3 - Competencias.",
]

OPCIONES_ESTRATEGIA_FORMATIVA: list[str] = [
    "E1: Fortalecimiento de Liderazgo",
    "E2: Pertinencia Territorial",
    "E3: Articulación UO MINEDU",
]


def _campo(
    codigo: str,
    pregunta: str,
    tipo: str,
    obligatorio: bool = False,
    opciones: list[str] | None = None,
    ayuda: str = "",
) -> dict[str, Any]:
    """Crea un descriptor de campo de caracterización."""
    return {
        "codigo": codigo,
        "pregunta": pregunta,
        "tipo": tipo,
        "obligatorio": obligatorio,
        "opciones": list(opciones or []),
        "ayuda": ayuda,
    }


# ---------------------------------------------------------------------------
# Secciones agrupadas para la UI (acordeones en "Editar capacitación").
# El campo `especialista_cargo` se incluye en Identificación según pedido.
# ---------------------------------------------------------------------------
CARACTERIZACION_SECCIONES: list[dict[str, Any]] = [
    {
        "slug": "carac-identificacion",
        "titulo": "Tipo de inscripción",
        "descripcion": "Modalidad de inscripción con la que se abre la capacitación.",
        "campos": [
            _campo("tipo_inscripcion", "Tipo de inscripción", "list", True, OPCIONES_TIPO_INSCRIPCION,
                   "Cerrada (lista predefinida) / Abierta (libre) / Mixta."),
        ],
    },
    {
        "slug": "carac-clasificacion",
        "titulo": "Clasificación institucional",
        "descripcion": "Ubicación de la capacitación en el mapa de procesos del Minedu.",
        "campos": [
            _campo("tipo_proceso_fortalecido", "Tipo de proceso fortalecido", "list", False,
                   OPCIONES_TIPO_PROCESO_FORTALECIDO),
            _campo("proceso_principal_fortalecido", "Proceso principal fortalecido", "list", False,
                   OPCIONES_PROCESO_PRINCIPAL),
            _campo("subproceso_fortalecido", "Subproceso fortalecido", "text_short"),
            _campo("rubro_tematico", "Rubro temático", "text_short"),
            _campo("estrategia_formativa", "Estrategia formativa", "list", False,
                   OPCIONES_ESTRATEGIA_FORMATIVA,
                   "Estrategia a la que pertenece (E1, E2 o E3)."),
        ],
    },
    {
        "slug": "carac-diseno",
        "titulo": "Caracterización del diseño formativo",
        "descripcion": "Indicadores binarios sobre cómo se diseñó y operó la capacitación.",
        "campos": [
            # Nota: `capacitacion_replicada` se gestiona únicamente en el Paso 1
            # (Solicitud) para evitar inputs duplicados con el mismo `name` que
            # provocaban que el valor del Paso 2 sobrescribiera al guardar.
            _campo("capacitacion_diagnostico_previo", "¿Empezó con diagnóstico de necesidades?", "list", False, OPCIONES_SI_NO,
                   "Si la capa empezó con un diagnóstico de necesidades."),
            _campo("capacitacion_virtual_sincronica", "¿Fue solo virtual unas pocas horas (sincrónica)?", "list", False, OPCIONES_SI_NO,
                   "Si la capa fue solo virtual unas pocas horas."),
            _campo("autoformativo", "¿Modalidad autoformativa?", "list", False, OPCIONES_SI_NO,
                   "Si el participante debía seguir las instrucciones solo."),
            _campo("necesidad_acompanamiento", "¿Necesitó gestionar alertas?", "list", False, OPCIONES_SI_NO,
                   "Capa que necesitó gestionar alertas."),
            _campo("capacitacion_acompanamiento", "¿Tuvo profesional que avisaba y gestionaba alertas?", "list", False, OPCIONES_SI_NO,
                   "Si la capa tuvo un profesional que les avisaba y gestionaba alertas."),
            _campo("monitores", "¿Tuvo monitores que resolvían dudas generales?", "list", False, OPCIONES_SI_NO,
                   "Si la capa tuvo un profesional que resolvía dudas vinculadas al desarrollo de la capacitación en general."),
            _campo("capacitacion_tutoria", "¿Tuvo tutores que resolvían dudas técnicas en grupo?", "list", False, OPCIONES_SI_NO,
                   "Si la capa tuvo un profesional que resolvía dudas técnicas / temáticas en el grupo asignado."),
            _campo("retroalimentacion", "¿Productos con retroalimentación de DIFOCA / UO Minedu?", "list", False, OPCIONES_SI_NO,
                   "Si los productos entregados tuvieron retroalimentación de la DIFOCA o UO Minedu."),
            _campo("acompanamiento_uo", "¿Tuvo profesional UO que resolvía dudas técnicas?", "list", False, OPCIONES_SI_NO,
                   "Si la capa tuvo un profesional de la UO que resolvía dudas técnicas en general."),
            _campo("capacitacion_presencialidad", "¿Tuvo algunas clases presenciales?", "list", False, OPCIONES_SI_NO,
                   "Si la capa tuvo algunas clases presenciales."),
            _campo("acciones_sostenidas", "¿Atención permanente para asegurar desempeños aplicados?", "list", False, OPCIONES_SI_NO,
                   "Si la capa tuvo atención permanente durante todo el tiempo para asegurar desempeños aplicados."),
            _campo("recursos_virtuales", "¿Recursos virtuales a disposición (videos, lecturas)?", "list", False, OPCIONES_SI_NO,
                   "Si la capa tuvo recursos virtuales a disposición (videos, lecturas con formato)."),
            _campo("capacitacion_competencia_especifica", "¿Atiende a algo puntual / competencia específica?", "list", False, OPCIONES_SI_NO,
                   "La capacitación atiende a algo puntual."),
            _campo("capacitacion_aplicacion_inmediata", "¿Producto que evidencia lo capacitado (aplicación inmediata)?", "list", False, OPCIONES_SI_NO,
                   "Cuando la capa tiene un producto que permite evidenciar lo que se está capacitando."),
        ],
    },
    {
        "slug": "carac-evaluacion",
        "titulo": "Evaluación y resultados esperados",
        "descripcion": "Cómo se mide el resultado y a qué nivel apunta la capacidad fortalecida.",
        "campos": [
            _campo("evidencia_comparativa", "¿Tiene evidencias al inicio y al final?", "list", False, OPCIONES_SI_NO,
                   "Si la capa tiene evidencias al inicio y al final."),
            _campo("nivel_capacidad_fortalecida", "Nivel de capacidad fortalecida", "list", False, OPCIONES_NIVEL_CAPACIDAD),
            _campo("evaluacion_eficacia_grupo_control", "¿Busca contar con información posterior (grupo control)?", "list", False, OPCIONES_SI_NO,
                   "Si la capa busca contar con información posterior a la capacitación."),
            _campo("resultado_aprendizaje", "Resultado de aprendizaje", "list", False, OPCIONES_RESULTADO_APRENDIZAJE,
                   "Si la capa busca modificar conocimientos, comportamientos o competencias."),
            _campo("encuesta_satisfaccion", "¿Tiene encuesta de satisfacción?", "list", False, OPCIONES_SI_NO,
                   "Si el curso tiene encuesta de satisfacción."),
            _campo("incluido_reporte_cneb", "¿Incluida en reporte CNEB?", "list", False, OPCIONES_SI_NO,
                   "Si la capa está incluida en un reporte específico (CNEB)."),
        ],
    },
    {
        "slug": "carac-modalidades-especiales",
        "titulo": "Modalidades especiales",
        "descripcion": "Marcas adicionales de mentoría, trazabilidad y asesorías.",
        "campos": [
            _campo("mentoria", "¿Es programa de mentoría?", "list", False, OPCIONES_SI_NO,
                   "Si la capa es un programa de mentoría."),
            _campo("trazabilidad_servicio_educativo", "¿Tiene trazabilidad en el servicio educativo?", "list", False, OPCIONES_SI_NO,
                   "Si la capa tiene evidencia de nueva aplicación de un proyecto o el desempeño en el puesto."),
            _campo("asesorias_personalizadas_colectivas", "¿Asesorías personalizadas o colectivas?", "list", False, OPCIONES_SI_NO,
                   "Si la capa tuvo atención ad hoc a las necesidades de la DRE/UO."),
        ],
    },
]


def iterar_campos_caracterizacion():
    """Itera todos los campos del catálogo de caracterización."""
    for seccion in CARACTERIZACION_SECCIONES:
        for campo in seccion.get("campos", []):
            yield campo


# Conjunto de códigos de campo binarios (Sí/No) para conversiones rápidas.
CAMPOS_CARACTERIZACION_BOOLEANOS: set[str] = {
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


# Lista plana de TODOS los códigos de campo del modelo Capacitacion gestionados
# por este schema (usado por el handler POST y por _serializar_capacitacion).
CAMPOS_CARACTERIZACION_CODIGOS: list[str] = [
    campo["codigo"] for seccion in CARACTERIZACION_SECCIONES for campo in seccion["campos"]
]
