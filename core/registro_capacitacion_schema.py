"""Esquema de campos para el formulario de registro de nuevas capacitaciones.

Este modulo concentra el catalogo de secciones/campos para no saturar views.py
y facilitar mantenimiento del formulario.
"""

# Tipado explicito para estructuras de configuracion.
from typing import Any


def _campo(
    codigo: str,
    pregunta: str,
    tipo: str,
    obligatorio: bool = False,
    opciones: list[str] | None = None,
    ayuda: str = "",
    ui_zone: str = "main",
) -> dict[str, Any]:
    """Crea un descriptor de campo reutilizable para el formulario dinamico."""
    return {
        "codigo": codigo,
        "pregunta": pregunta,
        "tipo": tipo,
        "obligatorio": obligatorio,
        "opciones": list(opciones or []),
        "ayuda": ayuda,
        "ui_zone": ui_zone,
    }


# Etapas visuales del proceso de solicitud/registro para la linea de tiempo inicial.
REGISTRO_CAPACITACION_ETAPAS: list[dict[str, str]] = [
    {
        "slug": "solicitud",
        "titulo": "Solicitud",
        "descripcion": "Clasifica el origen y los insumos iniciales del pedido.",
    },
    {
        "slug": "registro-base",
        "titulo": "Registro base",
        "descripcion": "Abre la capacitacion con sus datos generales y tipologia.",
    },
    {
        "slug": "sustento",
        "titulo": "Sustento",
        "descripcion": "Desarrolla diagnostico, brechas y alineamiento tecnico.",
    },
    {
        "slug": "diseno",
        "titulo": "Diseno y alcance",
        "descripcion": "Define poblacion, contenidos, modalidad y objetivos.",
    },
    {
        "slug": "implementacion",
        "titulo": "Implementacion",
        "descripcion": "Prepara responsables, logistica y seguimiento inicial.",
    },
]


# Estructura oficial de secciones y campos compartida por vista/template.
REGISTRO_CAPACITACION_SECCIONES: list[dict[str, Any]] = [
    {
        "slug": "solicitud-inicial",
        "titulo": "Solicitud inicial",
        "descripcion": "Etapa preliminar para definir el origen y abrir el registro de la capacitacion.",
        "campos": [
            _campo("sol_origen_institucional", "Origen de la solicitud", "list", True, ["DIFOCA", "IGED", "Unidad orgánica"], "Define si la capacitacion nace internamente o llega desde una IGED/UO.", "left"),
            _campo("sol_numero_oficio", "Numero de oficio", "text_short", False, None, "Obligatorio cuando la solicitud proviene de IGED o Unidad organica.", "left"),
            _campo("sol_fecha_oficio", "Fecha de oficio", "date", False, None, "Se usa para solicitudes externas formalizadas por oficio.", "left"),
            _campo("sol_archivo_oficio", "Link de oficio", "text_short", False, None, "Pega el enlace al oficio digitalizado para conservar el sustento dentro del registro.", "left"),
            _campo("sol_region_iged", "Region", "list", False, None, "Primero elige la region para filtrar las IGED disponibles.", "left"),
            _campo("sol_iged_nombre", "IGED", "list", False, None, "La lista se actualiza segun la region seleccionada.", "left"),
            _campo("pob_tipo", "Publico objetivo", "list", True, ["Docente", "Directivo", "Especialista DRE", "Especialista UGEL", "Acompañante pedagogico", "Formador", "Otro"], "Perfil principal al que estara dirigida la intervencion.", "left"),
            _campo("pob_ambito", "Ambito territorial", "list", True, ["Nacional", "Macroregional", "Regional", "Provincial", "Local"], "Nivel geografico esperado para esta solicitud.", "left"),
            _campo("cap_nombre", "Nombre de la capacitacion", "text_short", True, None, "Titulo preliminar con el que se abrira el proceso.", "left"),
            _campo("cap_anio", "Año de la intervención", "integer", True, None, "Se propone automáticamente según el año vigente.", "left"),
            _campo("cap_tipo", "Tipo de intervención formativa", "list", True, ["Capacitación sincrónica", "Curso", "Curso-taller", "Programa de formación", "Programa de mentoría"], "Clasifica la modalidad del proceso formativo.", "left"),
            _campo("sol_responde_desempeno", "La solicitud responde a un problema de desempeno", "list", False, ["Si", "No"], "Ayuda a perfilar mejor la brecha que se atendera.", "left"),
            _campo("sol_es_replica", "La capacitacion es una replica", "list", False, ["Si", "No"], "Si marcas Si, luego podras reutilizar informacion de una experiencia previa.", "right"),
            _campo("sol_tiene_matriz", "La capacitacion contara con matriz de sustento", "list", False, ["Si", "No"], "La matriz de sustento se usa cuando se cuenta con evidencia del (de los) problema(s) de desempeño que se quiere atender en la capacitación.", "right"),
            _campo("sol_tiene_diagnostico", "La capacitacion contara con diagnostico", "list", False, ["Si", "No"], "El diagnóstico se usa cuando no existe evidencia del (de los) problema(s) de desempeño a atender y debe ser levantada mediante un instrumento.", "right"),
        ],
    },
    {
        "slug": "identificacion-general",
        "titulo": "Identificacion general",
        "descripcion": "Datos base para identificar la intervencion formativa.",
        "campos": [
            _campo("cap_codigo", "Codigo interno o sigla", "text_short"),
            _campo("cap_estrategia", "Estrategia, programa o iniciativa", "text_short"),
            _campo("cap_prioridad", "Prioridad", "list", False, ["Alta", "Media", "Baja"]),
        ],
    },
    {
        "slug": "diagnostico-paso-1",
        "titulo": "Paso 1. Registrar datos del proceso",
        "descripcion": "Datos base del diagnostico, necesidad atendida y reglas de entrada del proceso.",
        "campos": [
            _campo("diag_base_normativa", "Base normativa", "text_long", True),
            _campo("diag_servicio_territorial", "Servicio o producto que contribuye con la gestion del servicio educativo en territorio", "text_long", True),
            _campo("diag_dre_modelo", "La DRE/UGEL esta de acuerdo con el modelo de capacitacion", "list", True, ["Si", "No"]),
            _campo("diag_problemas_json", "Problemas priorizados del proceso", "hidden_json"),
        ],
    },
    {
        "slug": "diagnostico-paso-1b",
        "titulo": "Establecimiento del problema priorizado",
        "descripcion": "Prioriza los problemas registrados, vincula desempenos y justifica cada priorizacion.",
        "campos": [
            _campo("diag_priorizacion_json", "Priorizacion de problemas", "hidden_json"),
            _campo("diag_ec2_relacion_logica", "Se identifico que el problema de gestion tiene relacion razonable y logica con causas vinculadas al desempeno de los equipos tecnicos o servidores competentes", "list", False, ["Si", "No"], "Estandar de calidad 2"),
        ],
    },
    {
        "slug": "diagnostico-paso-2",
        "titulo": "Paso 2. Generar matriz de evaluacion",
        "descripcion": "Define una o mas dimensiones, sus subdimensiones y los indicadores base de la matriz de evaluacion.",
        "campos": [
            _campo("diag_matriz_json", "Matriz de evaluacion", "hidden_json"),
            _campo("diag_instrumento_json", "Configuracion estructural del instrumento", "hidden_json"),
        ],
    },
    {
        "slug": "diagnostico-paso-3",
        "titulo": "Paso 3. Elaborar instrumento(s) de evaluacion",
        "descripcion": "Configura perfil, indicador, items e instrucciones del instrumento de diagnostico.",
        "campos": [
            _campo("diag_instr_instrucciones", "Detalle de instrucciones para el instrumento", "text_long"),
            _campo("diag_instr_presentacion", "Presentacion", "text_short"),
            _campo("diag_instr_periodo", "Periodo de aplicacion", "text_short"),
            _campo("diag_instr_tiempo", "Tiempo estimado", "text_short"),
            _campo("diag_instr_confidencialidad", "Confidencialidad", "text_short"),
            _campo("diag_instr_misma_escala", "Misma escala de los perfiles anteriores", "list", False, ["Si", "No"]),
            _campo("diag_instr_puntos_escala", "Cuantos puntos tendra la escala de evaluacion", "integer"),
            _campo("diag_instr_escala_1", "Escala 1", "text_short"),
            _campo("diag_instr_escala_2", "Escala 2", "text_short"),
            _campo("diag_instr_escala_3", "Escala 3", "text_short"),
            _campo("diag_instr_preguntas_extra", "Preguntas adicionales", "text_long"),
        ],
    },
    {
        "slug": "diagnostico-paso-4",
        "titulo": "Paso 4. Generar instrumento(s) de evaluacion",
        "descripcion": "Deja preparado el perfil final y la salida operativa para generar el instrumento.",
        "campos": [
            _campo("diag_generacion_json", "Configuracion de generacion del instrumento", "hidden_json"),
            _campo("diag_instr_perfil_final", "Perfil final para generar instrumento", "text_short"),
            _campo("diag_instr_previsualizacion", "Resumen o previsualizacion del instrumento", "text_long"),
            _campo("diag_instr_link_kr20", "Observacion sobre KR-20 o validacion tecnica", "text_long"),
        ],
    },
    {
        "slug": "diagnostico-paso-5",
        "titulo": "Paso 5. Resultados e informe",
        "descripcion": "Procesa resultados por dimension/subdimension y deja listo el informe del diagnostico.",
        "campos": [
            _campo("diag_resultados_json", "Procesamiento consolidado de resultados", "hidden_json"),
            _campo("diag_result_proc_dimension", "Procesamiento de datos por dimension", "text_long"),
            _campo("diag_result_proc_subdimension", "Procesamiento de datos por subdimension", "text_long"),
            _campo("diag_result_analisis", "Analisis e interpretacion de resultados", "text_long"),
            _campo("diag_result_informe", "Informe preliminar del diagnostico", "text_long"),
            _campo("diag_evidencia", "Resumen de evidencia diagnostica", "text_long"),
            _campo("diag_linea_base_existe", "Existe linea base", "list", False, ["Si", "No"]),
            _campo("diag_linea_base_valor", "Valor de linea base", "decimal"),
            _campo("diag_normativa", "Normativa que sustenta la capacitacion", "text_long"),
            _campo("diag_alineamiento", "Alineamiento estrategico", "text_long"),
            _campo("diag_just_tecnica", "Justificacion tecnica", "text_long"),
            _campo("diag_just_operativa", "Justificacion operativa", "text_long"),
            _campo("diag_just_normativa", "Justificacion normativa", "text_long"),
        ],
    },
    # ── 4to pop-up: Diseno de la Matriz Instruccional (+alcance) ──
    {
        "slug": "mi-diseno-contenido",
        "titulo": "Diseno y contenido instruccional",
        "descripcion": "Objetivo, competencias, desempenos y malla curricular del diseno formativo.",
        "campos": [
            _campo("mi_objetivo_capacitacion", "Objetivo de la capacitacion", "text_long", True),
            _campo("mi_competencias_json", "Competencias a fortalecer / desarrollar", "hidden_json"),
            _campo("mi_desempenos_json", "Desempeno esperado", "hidden_json"),
            _campo("mi_malla_json", "Malla curricular", "hidden_json"),
        ],
    },
    {
        "slug": "mi-criterios-evaluacion",
        "titulo": "Criterios y formula de evaluacion",
        "descripcion": "Criterios, formula de evaluacion e indicadores de calidad.",
        "campos": [
            _campo("mi_criterios_formula", "Criterios y formula de evaluacion", "text_long"),
            _campo("mi_formula_json", "Formula de evaluacion generada", "hidden_json"),
            _campo("mi_ind3_obj_consistente", "El objetivo de los proyectos formativos es consistente con las causas y el problema de gestion identificado y priorizado", "list", False, ["Si", "No"], "Indicador 3"),
            _campo("mi_ec4_desempenos_consistentes", "Los desempenos a fortalecer en los proyectos formativos son consistentes con sus objetivos", "list", False, ["Si", "No"], "Estandar de calidad 4"),
        ],
    },
    # ── 5to pop-up: Plan de Trabajo ──
    {
        "slug": "pt-resumen",
        "titulo": "Resumen",
        "descripcion": "Datos generales, modalidad y condiciones operativas.",
        "campos": [
            _campo("pt_fecha_desarrollo", "Fecha de desarrollo", "date"),
            _campo("pt_horas", "Horas de la capacitacion", "integer"),
            _campo("pt_modalidad", "Modalidad", "list", True, ["Presencial", "Virtual sincronica", "Virtual asincronica", "Semipresencial", "Mixta"]),
            _campo("pt_tipo_convocatoria", "Tipo de convocatoria", "list", True, ["Abierta", "Cerrada"]),
            _campo("pt_acciones_seguimiento", "Acciones de seguimiento al cumplimiento de las actividades de aprendizaje", "text_long"),
        ],
    },
    {
        "slug": "pt-sustento",
        "titulo": "Sustento",
        "descripcion": "Justificacion, base legal, objetivos e implementacion.",
        "campos": [
            _campo("pt_justificacion", "Justificacion", "text_long"),
            _campo("pt_base_legal", "Base legal", "text_long"),
            _campo("pt_obj_alcances", "Objetivo y alcances del programa", "text_long"),
            _campo("pt_convocatoria", "Convocatoria", "text_long"),
            _campo("pt_inscripcion", "Inscripcion de participantes", "text_long"),
            _campo("pt_desarrollo", "Desarrollo de la capacitacion", "text_long"),
        ],
    },
    {
        "slug": "pt-evaluacion",
        "titulo": "Evaluacion",
        "descripcion": "Requisitos, calificacion, responsables y productos.",
        "campos": [
            _campo("pt_requisitos", "Requisitos", "text_long"),
            _campo("pt_calculo_calificacion", "Calculo de calificacion", "text_long"),
            _campo("pt_actores_responsables", "Actores, roles y responsables", "text_long"),
            _campo("pt_productos_indicadores_json", "Productos e indicadores", "hidden_json"),
        ],
    },
    # ── 6to pop-up: Generacion de recursos ──
    {
        "slug": "gr-guia-participante",
        "titulo": "Elaboracion de Guia del Participante",
        "descripcion": "Generacion o carga de la guia del participante.",
        "campos": [
            _campo("gr_guia_link", "Link de Guia subida en plataforma", "text_short"),
            _campo("gr_guia_generada", "Se genero la Guia del Participante", "list", False, ["Si", "No"]),
        ],
    },
    {
        "slug": "gr-cuestionario-inicio",
        "titulo": "Elaboracion de Cuestionario de inicio",
        "descripcion": "Formato, carga y validacion del cuestionario de inicio.",
        "campos": [
            _campo("gr_cuestionario_link", "Link para aplicacion del cuestionario de inicio", "text_short"),
            _campo("gr_cuestionario_validado", "Cuestionario validado por 2 especialistas", "list", False, ["Si", "No"]),
        ],
    },
    {
        "slug": "gr-cronograma",
        "titulo": "Elaboracion de Cronograma",
        "descripcion": "Seleccion de campos, generacion y fecha de pre matricula.",
        "campos": [
            _campo("gr_cronograma_campos_json", "Campos seleccionados para el cronograma", "hidden_json"),
            _campo("gr_cronograma_link", "Link de Cronograma subido en plataforma", "text_short"),
            _campo("gr_fecha_prematricula", "Fecha de pre matricula", "date", True),
        ],
    },
    {
        "slug": "gr-plataforma",
        "titulo": "Linea grafica y plataforma",
        "descripcion": "Estado de la linea grafica, plataforma y recursos.",
        "campos": [
            _campo("gr_linea_grafica", "Linea grafica solicitada", "list", False, ["Si", "No"]),
            _campo("gr_plataforma_habilitada", "Plataforma habilitada", "list", False, ["Si", "No"]),
            _campo("gr_recursos_cargados", "Recursos de aprendizaje cargados en Plataforma", "list", False, ["Si", "No"]),
            _campo("gr_solicitudes_atendidas", "Solicitudes a plataforma atendidas en los plazos que corresponden", "list", False, ["Si", "No"], "Indicador 4"),
        ],
    },
    {
        "slug": "gr-indicadores",
        "titulo": "Indicadores y estandares de calidad",
        "descripcion": "Verificacion de indicadores y estandares de calidad de la etapa de recursos.",
        "campos": [
            _campo("gr_ind6_recursos", "Los recursos de aprendizaje cuentan con metodologias y actividades basadas en retos, problemas, casos que conducen eficazmente al logro de los aprendizajes propuestos y cuentan con bases normativas, academicas y cientificas", "list", False, ["Si", "No"], "Indicador 6"),
            _campo("gr_ec15_actividades", "Las actividades de aprendizaje y evaluacion son pertinentes y dan cuenta de la progresion de aprendizaje de los participantes durante el desarrollo de los proyectos formativos", "list", False, ["Si", "No"], "Estandar de calidad 15"),
            _campo("gr_ec13_tutores_docs", "De contar con tutores/monitores se generaron los documentos e instructivos necesarios para el desarrollo de su labor e indicadores en coordinacion con la unidad organica proponente y corresponsable", "list", False, ["Si", "No"], "Estandar de calidad 13"),
            _campo("gr_ec14_tutores_espacios", "De contar con tutores/monitores se generaron espacios de induccion y homologacion de criterios para el desarrollo de sus labores, reportes y linea de supervision en coordinacion con la unidad organica proponente y corresponsable", "list", False, ["Si", "No"], "Estandar de calidad 14"),
        ],
    },
    # ── 7mo pop-up: Implementacion y seguimiento ──
    {
        "slug": "is-convocatoria",
        "titulo": "Oficio de convocatoria",
        "descripcion": "Datos del oficio formal de convocatoria.",
        "campos": [
            _campo("is_conv_nro_oficio", "Numero de oficio", "text_short", True),
            _campo("is_conv_fecha_oficio", "Fecha de oficio", "date", True),
        ],
    },
    {
        "slug": "is-confirmacion",
        "titulo": "Oficio de confirmacion de participantes",
        "descripcion": "Datos del oficio de confirmacion y compromiso de participantes.",
        "campos": [
            _campo("is_conf_nro_oficio", "Numero de oficio", "text_short", True),
            _campo("is_conf_fecha_oficio", "Fecha de oficio", "date", True),
            _campo("is_conf_fecha_compromiso", "Fecha de compromiso", "date", True),
        ],
    },
    {
        "slug": "is-seguimiento",
        "titulo": "Seguimiento a los proyectos formativos",
        "descripcion": "Verificacion de alertas, comunicados y seguimiento.",
        "campos": [
            _campo("is_seg_alertas_cumplimiento", "Se remitio alertas y comunicados a los puntos focales de las Unidades Organicas respecto al cumplimiento de actividades de aprendizaje de los proyectos formativos", "list", False, ["Si", "No"], "Estandar de calidad 22"),
            _campo("is_seg_alertas_progreso", "Se remitio alertas y comunicados a los participantes respecto a su progreso de aprendizajes", "list", False, ["Si", "No"]),
        ],
    },
    # ── 8vo pop-up: Evaluacion y documentacion ──
    {
        "slug": "ed-reportes",
        "titulo": "Reportes y documentacion final",
        "descripcion": "Generacion de reportes, certificados y oficios de cierre.",
        "campos": [
            _campo("ed_nro_oficio", "Nro de oficio", "text_short", True),
            _campo("ed_fecha_oficio", "Fecha de oficio", "date", True),
        ],
    },
    {
        "slug": "ed-cierre",
        "titulo": "Cierre de capacitacion",
        "descripcion": "Ultima verificacion antes del cierre formal.",
        "campos": [
            _campo("ed_cierre_confirmado", "Se confirma el cierre de la capacitacion", "list", False, ["Si", "No"]),
        ],
    },
]


def iterar_campos_registro_capacitacion(
    secciones_filtro: set[str] | None = None,
) -> list[dict[str, Any]]:
    """Retorna lista plana de campos para validacion/procesamiento.

    Si *secciones_filtro* se indica, solo incluye campos de esas secciones.
    Cada campo incluye la clave ``seccion_slug`` con el slug de su seccion.
    """
    campos: list[dict[str, Any]] = []
    for seccion in REGISTRO_CAPACITACION_SECCIONES:
        slug = str(seccion.get("slug", ""))
        if secciones_filtro is not None and slug not in secciones_filtro:
            continue
        for campo in seccion.get("campos", []):
            campos.append({**campo, "seccion_slug": slug})
    return campos
