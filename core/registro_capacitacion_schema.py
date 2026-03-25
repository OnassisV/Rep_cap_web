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
            _campo("sol_archivo_oficio", "Archivo de oficio", "file", False, None, "Adjunta el oficio en PDF, DOC o DOCX para conservar el sustento dentro del registro.", "left"),
            _campo("sol_region_iged", "Region", "list", False, None, "Primero elige la region para filtrar las IGED disponibles.", "left"),
            _campo("sol_iged_nombre", "IGED", "list", False, None, "La lista se actualiza segun la region seleccionada.", "left"),
            _campo("pob_tipo", "Publico objetivo", "list", True, ["Docente", "Directivo", "Especialista DRE", "Especialista UGEL", "Acompañante pedagogico", "Formador", "Otro"], "Perfil principal al que estara dirigida la intervencion.", "left"),
            _campo("pob_ambito", "Ambito territorial", "list", True, ["Nacional", "Macroregional", "Regional", "Provincial", "Local"], "Nivel geografico esperado para esta solicitud.", "left"),
            _campo("cap_nombre", "Nombre de la capacitacion", "text_short", True, None, "Titulo preliminar con el que se abrira el proceso.", "left"),
            _campo("cap_anio", "Año de la intervención", "integer", True, None, "Se propone automáticamente según el año vigente.", "left"),
            _campo("sol_es_replica", "La capacitacion es una replica", "list", False, ["Si", "No"], "Si marcas Si, luego podras reutilizar informacion de una experiencia previa.", "right"),
            _campo("sol_tiene_matriz", "La capacitacion contara con matriz de sustento", "list", False, ["Si", "No"], "Esta decision prepara el siguiente paso de sustento tecnico.", "right"),
            _campo("sol_tiene_diagnostico", "La capacitacion contara con diagnostico", "list", False, ["Si", "No"], "Si marcas Si, el flujo posterior priorizara el bloque diagnostico.", "right"),
            _campo("sol_responde_desempeno", "La solicitud responde a un problema de desempeno", "list", False, ["Si", "No"], "Ayuda a perfilar mejor la brecha que se atendera.", "right"),
        ],
    },
    {
        "slug": "identificacion-general",
        "titulo": "Identificacion general",
        "descripcion": "Datos base para identificar la intervencion formativa.",
        "campos": [
            _campo("cap_codigo", "Codigo interno o sigla", "text_short"),
            _campo(
                "cap_tipo",
                "Tipo de intervencion formativa",
                "list",
                True,
                [
                    "Capacitación sincrónica",
                    "Curso",
                    "Curso-taller",
                    "Programa de formación",
                    "Programa de mentoría",
                ],
            ),
            _campo("cap_estrategia", "Estrategia, programa o iniciativa", "text_short"),
            _campo("cap_prioridad", "Prioridad", "list", False, ["Alta", "Media", "Baja"]),
        ],
    },
    {
        "slug": "diagnostico-paso-1",
        "titulo": "Paso 1. Registrar datos del proceso",
        "descripcion": "Datos base del diagnostico, necesidad atendida y reglas de entrada del proceso.",
        "campos": [
            _campo("diag_base_normativa", "Base normativa", "text_long"),
            _campo("diag_ndc_normada", "La NDC esta normada", "list", False, ["Si", "No"]),
            _campo("diag_servicio_territorial", "Servicio o producto que contribuye con la gestion del servicio educativo en territorio", "text_long"),
            _campo("diag_dos_fuentes", "El diagnostico cuenta con 2 o mas fuentes de informacion", "list", False, ["Si", "No"]),
            _campo("diag_dre_modelo", "La DRE/UGEL esta de acuerdo con el modelo de capacitacion", "list", False, ["Si", "No"]),
            _campo("diag_problemas_json", "Problemas priorizados del proceso", "hidden_json"),
            _campo("diag_brecha", "Brecha de capacidades identificada", "text_long"),
            _campo(
                "diag_fuente",
                "Fuente principal del diagnostico",
                "list",
                False,
                [
                    "Reporte de estandares",
                    "Encuesta",
                    "Evaluacion diagnostica",
                    "Supervision",
                    "Monitoreo",
                    "Norma",
                    "Solicitud territorial",
                    "Otro",
                ],
            ),
            _campo("diag_fuente_det", "Documento o reporte fuente", "text_short"),
        ],
    },
    {
        "slug": "diagnostico-paso-2",
        "titulo": "Paso 2. Generar matriz de evaluacion",
        "descripcion": "Define la dimension, subdimension e indicadores base de la matriz de evaluacion.",
        "campos": [
            _campo("diag_matriz_json", "Matriz de evaluacion", "hidden_json"),
        ],
    },
    {
        "slug": "diagnostico-paso-3",
        "titulo": "Paso 3. Elaborar instrumento(s) de evaluacion",
        "descripcion": "Configura perfil, indicador, items e instrucciones del instrumento de diagnostico.",
        "campos": [
            _campo("diag_instrumento_json", "Configuracion estructural del instrumento", "hidden_json"),
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
    {
        "slug": "oferta-formativa",
        "titulo": "Oferta formativa",
        "descripcion": "Datos de catalogo y vigencia de la oferta institucional.",
        "campos": [
            _campo("oferta_en_catalogo", "Existe en la oferta formativa o catalogo", "list", True, ["Si", "No"]),
            _campo("oferta_codigo", "Codigo de oferta formativa", "text_short"),
            _campo("oferta_linea", "Linea de oferta formativa", "text_short"),
            _campo("oferta_sublinea", "Sublinea o familia formativa", "text_short"),
            _campo(
                "oferta_nivel",
                "Nivel de oferta",
                "list",
                False,
                ["Inicial", "Intermedio", "Avanzado", "Especializado", "No aplica"],
            ),
            _campo("oferta_vigencia", "Vigencia de la oferta", "list", False, ["Si", "No"]),
            _campo(
                "oferta_modalidad",
                "Modalidad registrada en catalogo",
                "list",
                False,
                ["Presencial", "Virtual sincronica", "Virtual asincronica", "Semipresencial", "Mixta"],
            ),
            _campo("oferta_horas", "Horas registradas en catalogo", "decimal"),
        ],
    },
    {
        "slug": "poblacion-objetivo",
        "titulo": "Poblacion objetivo",
        "descripcion": "Segmentacion, focalizacion y cobertura proyectada.",
        "campos": [
            _campo("pob_cargo", "Cargo o perfil especifico", "text_short"),
            _campo(
                "pob_nivel_edu",
                "Nivel educativo",
                "list",
                False,
                ["Inicial", "Primaria", "Secundaria", "Basica alternativa", "Basica especial", "No aplica"],
            ),
            _campo("pob_servicio", "Modalidad o servicio educativo asociado", "text_short"),
            _campo("pob_instancia", "Instancia de procedencia", "list", False, ["MINEDU", "DRE/GRE", "UGEL", "IE", "Otro"]),
            _campo("pob_region", "Region o regiones participantes", "text_short"),
            _campo("pob_focalizacion", "Criterio de focalizacion o priorizacion", "text_long"),
            _campo("pob_total", "Numero total de participantes proyectados", "integer", True),
            _campo("pob_cohortes", "Se trabajara por cohortes o grupos", "list", False, ["Si", "No"]),
            _campo("pob_n_cohortes", "Numero de cohortes o grupos", "integer"),
            _campo("pob_cupo", "Cupo maximo por grupo", "integer"),
        ],
    },
    {
        "slug": "objetivos-resultados",
        "titulo": "Objetivos y resultados",
        "descripcion": "Definicion de objetivos, resultados e indicadores.",
        "campos": [
            _campo("obj_general", "Objetivo general", "text_long", True),
            _campo("obj_especificos", "Objetivos especificos", "text_long"),
            _campo("res_producto", "Producto esperado", "text_long"),
            _campo("res_resultado", "Resultado esperado", "text_long"),
            _campo("res_impacto", "Impacto esperado", "text_long"),
            _campo("ind_principal", "Indicador principal", "text_short"),
            _campo("ind_formula", "Formula del indicador", "text_short"),
            _campo("ind_meta", "Meta del indicador", "decimal"),
            _campo("ind_fuente_verif", "Fuente de verificacion", "text_short"),
            _campo("ind_frecuencia", "Frecuencia de medicion", "list", False, ["Unica", "Semanal", "Quincenal", "Mensual", "Por hito"]),
        ],
    },
    {
        "slug": "estandares-alcance",
        "titulo": "Estandares y alcance",
        "descripcion": "Relacion de estandares, desempenos y hallazgos asociados.",
        "campos": [
            _campo("est_codigo", "Codigo de estandar", "text_short"),
            _campo("est_competencia", "Competencia asociada", "text_short"),
            _campo("est_desempeno", "Desempeno asociado", "text_long"),
            _campo("est_brecha", "Brecha asociada al estandar", "text_long"),
            _campo("est_nivel_logro", "Nivel de logro identificado", "text_short"),
            _campo("est_hallazgo", "Hallazgo principal del estandar", "text_long"),
            _campo("est_fuente_medicion", "Fuente de medicion del estandar", "text_short"),
        ],
    },
    {
        "slug": "diseno-formativo-base",
        "titulo": "Diseno formativo base",
        "descripcion": "Parametros base de modalidad, duracion y certificacion.",
        "campos": [
            _campo(
                "dis_modalidad",
                "Modalidad de la capacitacion",
                "list",
                True,
                ["Presencial", "Virtual sincronica", "Virtual asincronica", "Semipresencial", "Mixta"],
            ),
            _campo(
                "dis_metodologia",
                "Metodologia general",
                "list",
                False,
                [
                    "Taller",
                    "Curso autoformativo",
                    "Curso tutorizado",
                    "Blended",
                    "Acompanamiento",
                    "Comunidad de practica",
                    "Otro",
                ],
            ),
            _campo("dis_horas_total", "Duracion total en horas", "decimal", True),
            _campo("dis_horas_sinc", "Horas sincronicas", "decimal"),
            _campo("dis_horas_asinc", "Horas asincronicas", "decimal"),
            _campo("dis_modulos", "Numero de modulos o unidades", "integer"),
            _campo("dis_sesiones", "Numero de sesiones", "integer"),
            _campo("dis_certifica", "Tipo de certificacion", "list", False, ["Certificado", "Constancia", "No aplica"]),
            _campo("dis_req_cert", "Requisitos preliminares de certificacion", "text_long"),
        ],
    },
    {
        "slug": "contenido-preliminar",
        "titulo": "Contenido preliminar",
        "descripcion": "Definicion inicial de contenidos, recursos y evidencias.",
        "campos": [
            _campo("cont_competencias", "Competencias a desarrollar", "text_long"),
            _campo("cont_obj_aprend", "Objetivos de aprendizaje preliminares", "text_long"),
            _campo("cont_temas", "Temas o contenidos centrales", "text_long"),
            _campo("cont_producto_part", "Producto o evidencia principal del participante", "text_long"),
            _campo("cont_recursos", "Recursos o materiales previstos", "text_long"),
            _campo("cont_estrategia", "Estrategia metodologica general", "text_long"),
        ],
    },
    {
        "slug": "evaluacion-preliminar",
        "titulo": "Evaluacion preliminar",
        "descripcion": "Esquema preliminar de evaluacion y criterios.",
        "campos": [
            _campo("eval_tipo", "Tipo de evaluacion", "list", False, ["Diagnostica", "Formativa", "Sumativa", "Mixta"]),
            _campo(
                "eval_instr",
                "Instrumento de evaluacion previsto",
                "list",
                False,
                ["Cuestionario", "Rubrica", "Lista de cotejo", "Producto", "Otro"],
            ),
            _campo("eval_criterios", "Criterios generales de evaluacion", "text_long"),
            _campo("eval_ponderacion", "Ponderacion general", "text_long"),
            _campo("eval_nota_min", "Nota minima de aprobacion", "decimal"),
            _campo("eval_asistencia_min", "Asistencia minima requerida", "decimal"),
        ],
    },
    {
        "slug": "implementacion-base",
        "titulo": "Implementacion base",
        "descripcion": "Fechas, mecanismos y responsables para la puesta en marcha.",
        "campos": [
            _campo("imp_fecha_ini", "Fecha tentativa de inicio", "date"),
            _campo("imp_fecha_fin", "Fecha tentativa de fin", "date"),
            _campo("imp_convocatoria", "Mecanismo de convocatoria", "text_long"),
            _campo("imp_inscripcion", "Mecanismo de inscripcion o matricula", "text_long"),
            _campo("imp_sede", "Plataforma o sede principal", "text_short"),
            _campo("imp_resp_conv", "Responsable de convocatoria", "text_short"),
            _campo("imp_resp_impl", "Responsable de implementacion", "text_short"),
        ],
    },
    {
        "slug": "logistica-recursos",
        "titulo": "Logistica y recursos",
        "descripcion": "Base presupuestal y requerimientos operativos.",
        "campos": [
            _campo("log_tiene_ppto", "Tiene presupuesto identificado", "list", False, ["Si", "No"]),
            _campo("log_monto", "Monto estimado", "currency"),
            _campo("log_componentes", "Componentes logisticos requeridos", "text_long"),
            _campo("log_req_tec", "Requerimientos tecnologicos", "text_long"),
            _campo("log_req_op", "Requerimientos operativos", "text_long"),
            _campo("log_req_adm", "Requerimientos administrativos", "text_long"),
        ],
    },
    {
        "slug": "responsables",
        "titulo": "Responsables",
        "descripcion": "Responsables de la conduccion y datos de contacto.",
        "campos": [
            _campo("resp_unidad", "Unidad responsable", "text_short", True),
            _campo("resp_coord", "Coordinador o coordinadora de la capacitacion", "text_short"),
            _campo("resp_cargo", "Cargo del responsable", "text_short"),
            _campo("resp_correo", "Correo de contacto", "text_short"),
            _campo("resp_telefono", "Telefono de contacto", "text_short"),
            _campo("resp_otros", "Otros roles asociados", "text_long"),
        ],
    },
    {
        "slug": "seguimiento-inicial",
        "titulo": "Seguimiento inicial",
        "descripcion": "Riesgos, supuestos e indicadores iniciales de monitoreo.",
        "campos": [
            _campo("seg_tablero", "Requiere tablero o reporte de seguimiento", "list", False, ["Si", "No"]),
            _campo("seg_ind_sec", "Indicadores secundarios de seguimiento", "text_long"),
            _campo("seg_riesgos", "Riesgos identificados", "text_long"),
            _campo("seg_supuestos", "Supuestos criticos", "text_long"),
            _campo("seg_obs", "Observaciones iniciales", "text_long"),
        ],
    },
]


def iterar_campos_registro_capacitacion() -> list[dict[str, Any]]:
    """Retorna lista plana de campos para validacion/procesamiento."""
    campos: list[dict[str, Any]] = []
    for seccion in REGISTRO_CAPACITACION_SECCIONES:
        for campo in seccion.get("campos", []):
            campos.append(campo)
    return campos
