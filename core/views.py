"""Vistas protegidas del modulo core para pantalla inicial y secciones."""

# Tipado para mantener estructura de datos del menu mas clara.
from typing import Any
from datetime import datetime, date as _date_type
from urllib.parse import urlencode

# Error HTTP para retornar 404 cuando una seccion no exista.
from django.http import Http404, HttpResponse
# Decorador que exige sesion autenticada activa.
from django.contrib.auth.decorators import login_required
# Framework de mensajes para feedback de operaciones CRUD.
from django.contrib import messages
# Decorador para limitar endpoint de cambio de rol a metodo POST.
from django.views.decorators.http import require_POST
# Atajos para renderizar y redirigir.
from django.shortcuts import redirect, render
# Utilidad para validar redireccion segura.
from django.utils.http import url_has_allowed_host_and_scheme

# Adaptadores de lectura legacy para submenus migrados desde Streamlit.
from .legacy_adapters import (
    agregar_retiros_manual,
    agregar_actividad_estructura,
    aplicar_filtro_anio,
    actualizar_actividad_estructura,
    construir_alertas_seguimiento,
    construir_resumen_estandares_por_capacitacion,
    crear_registro_capacitacion,
    eliminar_postulantes_excel,
    eliminar_retiro_manual,
    eliminar_actividad_estructura,
    eliminar_formula_promedio,
    exportar_certificados_csv,
    exportar_confiabilidad_csv,
    exportar_resumen_certificados_csv,
    extraer_id_capacitacion,
    filtrar_capacitaciones_para_usuario,
    guardar_config_nominal_reporte,
    guardar_excel_actividad_fuera,
    guardar_formula_promedio,
    guardar_postulantes_excel,
    guardar_rutas_plantilla,
    generar_plantilla_seguimiento,
    interpretar_kr20,
    leer_retiros_manual,
    limpiar_retiros_manual,
    analizar_confiabilidad_por_codigo,
    eliminar_excel_actividad_fuera,
    obtener_catalogo_iged_por_region,
    obtener_estructura_por_codigo,
    obtener_actividades_plantilla,
    obtener_filas_oferta_formativa,
    obtener_formulas_promedio,
    obtener_metricas_seguimiento,
    obtener_alertas_actividades_plataforma,
    obtener_config_nominal_reporte,
    obtener_postulantes_excel_info,
    obtener_plantilla_generada_info,
    obtener_excel_actividad_fuera_info,
    obtener_certificados_detalle,
    obtener_participantes_retiro_manual_por_codigo,
    obtener_resumen_satisfaccion,
    obtener_resumen_estandares,
    obtener_rutas_plantilla,
    resumir_certificados_por_region,
    _normalizar_texto,
)

# Esquema declarativo del formulario de registro de nuevas capacitaciones.
from .registro_capacitacion_schema import (
    REGISTRO_CAPACITACION_ETAPAS,
    REGISTRO_CAPACITACION_SECCIONES,
    iterar_campos_registro_capacitacion,
)
from .indicadores_adapters import build_indicadores_dashboard_context, build_indicadores_download
from .sync_runtime import build_sync_status_context


# Estructura central del GeoMenu.
# Cada bloque define:
# - slug: identificador URL
# - titulo/subtitulo/descripcion: textos visibles
# - imagen: ruta relativa dentro de static
# - modulos: lista de herramientas legacy agrupadas
# - submenus: acciones internas (opcional por seccion)
MENU_GEOMETRICO: list[dict[str, Any]] = [
    {
        "slug": "gestion-capacitacion",
        "titulo": "Gestión de la Capacitación",
        "subtitulo": "Planifica, estructura y ejecuta",
        "descripcion": (
            "Control integral de oferta, estándares y plantillas para gestionar"
            " ciclos formativos de extremo a extremo."
        ),
        "imagen": "images/menu/gestion_capacitacion.svg",
        "modulos": [
            "oferta_formativa.py",
            "estandares_calidad.py",
            "plantillas.py",
        ],
        "submenus": [
            {
                "slug": "registrar-nueva-capacitacion",
                "titulo": "Registrar nueva capacitación",
                "descripcion": "Alta inicial de nuevos procesos formativos.",
                "adapter": "registro_capacitacion",
                "legacy_module": "core/registro_capacitacion_schema.py + MySQL Railway",
            },
            {
                "slug": "editar-capacitacion",
                "titulo": "Editar capacitación",
                "descripcion": "Continúa el registro o actualiza datos de una capacitación existente.",
                "adapter": "editar_capacitacion",
                "legacy_module": "core/models.py (ORM)",
            },
            {
                "slug": "productos-capacitacion",
                "titulo": "Productos de la capacitación",
                "descripcion": "Gestión de entregables y evidencias por proceso.",
                "adapter": "placeholder",
                "legacy_module": "N/A (nuevo flujo web)",
            },
            {
                "slug": "seguimiento-capacitaciones",
                "titulo": "Seguimiento de capacitaciones",
                "descripcion": "Control operativo del avance, alertas y resultados por proceso.",
                "adapter": "seguimiento",
                "legacy_module": "core/legacy_adapters.py + templates/core/submenu_detail.html",
            },
            {
                "slug": "oferta-formativa",
                "titulo": "Oferta formativa",
                "descripcion": "Listado operativo de procesos disponibles.",
                "adapter": "oferta",
                "legacy_module": "core/legacy_adapters.py + templates/core/submenu_detail.html",
            },
            {
                "slug": "estandares-calidad",
                "titulo": "Estándares de calidad",
                "descripcion": "Matriz de estándares y niveles de cumplimiento por capacitación.",
                "adapter": "estandares",
                "legacy_module": "core/legacy_adapters.py + templates/core/submenu_detail.html",
            },
        ],
    },
    {
        "slug": "reporte-indicadores",
        "titulo": "Reporte de Indicadores",
        "subtitulo": "Visualiza desempeño en tiempo real",
        "descripcion": (
            "Consolida KPIs, ámbitos y satisfacción para seguimiento ejecutivo,"
            " comparativos y toma de decisiones."
        ),
        "imagen": "images/menu/reporte_indicadores.svg",
        "modulos": [
            "dashboard_kpi.py",
            "reporte_ambitos.py",
            "satisfaccion/procesamiento_satisfaccion.py",
        ],
        "submenus": [
            {
                "slug": "dashboard-kpi",
                "titulo": "Dashboard KPI",
                "descripcion": "Resumen ejecutivo de KPIs por capacitacion, region, IGED y participante.",
                "adapter": "indicadores",
                "legacy_module": "app_difoca/modules/dashboard_kpi.py",
            },
        ],
    },
    {
        "slug": "laboratorio-datos",
        "titulo": "Laboratorio de Datos",
        "subtitulo": "Transforma, cruza y valida",
        "descripcion": (
            "Espacio técnico para análisis avanzado y procesamiento de bases,"
            " con enfoque en calidad y trazabilidad."
        ),
        "imagen": "images/menu/laboratorio_datos.svg",
        "modulos": [
            "analisis.py",
            "procesamiento_datos.py",
        ],
    },
    {
        "slug": "operaciones-plataforma",
        "titulo": "Operaciones de Plataforma",
        "subtitulo": "Conecta sistemas y exporta",
        "descripcion": (
            "Gestión operativa de consultas, exportaciones y flujos desde SIDI"
            " y Aula Virtual."
        ),
        "imagen": "images/menu/operaciones_plataforma.svg",
        "modulos": [
            "plataforma.py",
            "list_matricula.py",
            "detalle_cuestionario.py",
        ],
    },
    {
        "slug": "sincronicas-evidencias",
        "titulo": "Sincrónicas y Evidencias",
        "subtitulo": "Entrada, salida y trazabilidad",
        "descripcion": (
            "Gestiona insumos de sesiones sincrónicas y su manifiesto para mantener"
            " consistencia de evidencia y auditoría."
        ),
        "imagen": "images/menu/sincronicas_evidencias.svg",
        "modulos": [
            "plantillas_sincronicas_streamlit.py",
        ],
    },
    {
        "slug": "administracion-seguridad",
        "titulo": "Administración y Seguridad",
        "subtitulo": "Usuarios, accesos y control",
        "descripcion": (
            "Gestión de cuentas, permisos y auditoría para mantener gobernanza"
            " del sistema."
        ),
        "imagen": "images/menu/administracion_seguridad.svg",
        "modulos": [
            "gestion_usuarios.py",
        ],
    },
]

# Indice rapido para resolver una seccion por slug.
MENU_POR_SLUG = {seccion["slug"]: seccion for seccion in MENU_GEOMETRICO}
def _buscar_submenu(section_slug: str, submenu_slug: str) -> tuple[dict[str, Any], dict[str, Any]]:
    """Retorna seccion + submenu solicitado o lanza 404 logico en el caller."""
    # Busca seccion principal por slug.
    section = MENU_POR_SLUG.get(section_slug)
    if section is None:
        raise Http404("Seccion no encontrada.")

    # Busca submenu dentro de la seccion.
    for submenu in section.get("submenus", []):
        if submenu.get("slug") == submenu_slug:
            return section, submenu

    # Si no existe el submenu, retorna error 404.
    raise Http404("Submenu no encontrado.")


def _parse_datetime_local(value: str) -> str | None:
    """Convierte valor datetime-local del formulario a formato SQL."""
    value = str(value or "").strip()
    if not value:
        return None
    try:
        # Soporta formato HTML5: YYYY-MM-DDTHH:MM
        dt = datetime.fromisoformat(value)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def _format_datetime_local(value: Any) -> str:
    """Convierte datetime/str de BD a formato datetime-local para inputs HTML."""
    if value is None:
        return ""
    if hasattr(value, "strftime"):
        try:
            return value.strftime("%Y-%m-%dT%H:%M")
        except Exception:
            return ""

    text = str(value).strip()
    if not text:
        return ""

    # Normaliza representaciones tipicas de MySQL: "YYYY-MM-DD HH:MM:SS".
    text = text.replace(" ", "T")
    try:
        dt = datetime.fromisoformat(text)
        return dt.strftime("%Y-%m-%dT%H:%M")
    except Exception:
        return ""


def _build_submenu_url(section_slug: str, submenu_slug: str, params: dict[str, Any]) -> str:
    """Construye URL con query params para redireccion consistente."""
    query = urlencode({k: v for k, v in params.items() if str(v or "").strip()})
    base = f"/app/seccion/{section_slug}/submenu/{submenu_slug}/"
    return f"{base}?{query}" if query else base


def _valor_por_defecto_registro_capacitacion() -> dict[str, str]:
    """Retorna valores iniciales del formulario de registro de capacitacion."""
    return {
        # Se sugiere año actual para agilizar carga inicial.
        "cap_anio": str(datetime.now().year),
        # Flujo arranca en borrador por defecto.
        "cap_estado": "Borrador",
    }


def _coercer_valor_registro_capacitacion(tipo: str, valor_raw: str) -> Any:
    """Convierte valor de formulario a tipo base para serializacion JSON."""
    valor = str(valor_raw or "").strip()
    if not valor:
        return None

    # Convierte enteros.
    if tipo == "integer":
        try:
            return int(valor)
        except Exception:
            return None

    # Convierte decimales/moneda aceptando coma o punto.
    if tipo in {"decimal", "currency"}:
        valor_norm = valor.replace(",", ".")
        try:
            return float(valor_norm)
        except Exception:
            return None

    # Fecha/lista/texto se conservan como string limpio.
    return valor


def _contar_estado_bloque_registro(campos: list[dict[str, Any]]) -> dict[str, int | bool]:
    """Resume avance de un bloque segun campos llenos y obligatorios completos."""
    required_count = 0
    required_done = 0
    filled_count = 0

    for campo in campos:
        valor = str(campo.get("valor", "") or "").strip()
        if valor:
            filled_count += 1

        if bool(campo.get("obligatorio")):
            required_count += 1
            if valor:
                required_done += 1

    # Un bloque se considera completo cuando cumple todos sus obligatorios
    # o, si no tiene obligatorios, cuando al menos ya contiene informacion.
    is_complete = (
        (required_count > 0 and required_done >= required_count)
        or (required_count == 0 and filled_count > 0)
    )
    is_started = filled_count > 0

    return {
        "required_count": required_count,
        "required_done": required_done,
        "filled_count": filled_count,
        "is_started": is_started,
        "is_complete": is_complete,
    }


def _enriquecer_campo_registro(campo: dict[str, Any], valores_form: dict[str, str]) -> dict[str, Any]:
    """Agrega metadatos de interfaz para renderizar la ficha de registro."""
    codigo = str(campo.get("codigo", "")).strip()

    # Campos visibles solo cuando la solicitud proviene de una entidad externa.
    campos_externos = {
        "sol_numero_oficio",
        "sol_fecha_oficio",
        "sol_archivo_oficio",
    }

    # Campos visibles solo para solicitudes IGED.
    campos_iged = {
        "sol_region_iged",
        "sol_iged_nombre",
    }

    # Decisiones binarias mostradas como chips Si/No.
    campos_decision = {
        "sol_es_replica",
        "sol_tiene_matriz",
        "sol_tiene_diagnostico",
        "sol_responde_desempeno",
    }

    return {
        **campo,
        "valor": str(valores_form.get(codigo, "")).strip(),
        "is_origin_selector": codigo == "sol_origen_institucional",
        "is_decision": codigo in campos_decision,
        "is_external_only": codigo in campos_externos,
        "is_iged_only": codigo in campos_iged,
    }


def _diagnostico_habilitado(valores_form: dict[str, str]) -> bool:
    """Determina si el flujo de diagnostico debe activarse desde la fase preliminar."""
    decision_diagnostico = str(valores_form.get("sol_tiene_diagnostico", "")).strip().lower()
    decision_matriz = str(valores_form.get("sol_tiene_matriz", "")).strip().lower()
    return decision_diagnostico in {"si", "sí"} or decision_matriz in {"si", "sí"}


def _matriz_habilitada(valores_form: dict[str, str]) -> bool:
    """Indica si la matriz de sustento debe mostrarse como flujo disponible."""
    return str(valores_form.get("sol_tiene_matriz", "")).strip().lower() in {"si", "sí"}


def _detalle_diagnostico_habilitado(valores_form: dict[str, str]) -> bool:
    """Indica si el modal específico de diagnostico debe habilitarse."""
    return str(valores_form.get("sol_tiene_diagnostico", "")).strip().lower() in {"si", "sí"}


def _filtrar_campos_registro(
    campos: list[dict[str, Any]],
    *,
    include: set[str] | None = None,
    exclude: set[str] | None = None,
) -> list[dict[str, Any]]:
    """Devuelve una copia filtrada de campos del bloque de registro."""
    include = include or set()
    exclude = exclude or set()
    campos_filtrados: list[dict[str, Any]] = []
    for campo in campos:
        codigo = str(campo.get("codigo", "")).strip()
        if include and codigo not in include:
            continue
        if codigo in exclude:
            continue
        campos_filtrados.append({**campo})
    return campos_filtrados


def _construir_pasarela_sustento(
    matriz_flujo: dict[str, Any] | None,
    diagnostico_flujo: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """Construye la etapa intermedia con dos accesos paralelos via modales."""
    if not matriz_flujo and not diagnostico_flujo:
        return None

    return {
        "slug": "etapa-sustento",
        "titulo": "Sustento técnico previo",
        "descripcion": "Despues de la fase preliminar puedes desarrollar la matriz de sustento y el diagnostico en paralelo, cada uno desde su propio modal guiado.",
        "is_enabled": bool((matriz_flujo and matriz_flujo.get("is_enabled")) or (diagnostico_flujo and diagnostico_flujo.get("is_enabled"))),
        "matriz": matriz_flujo,
        "diagnostico": diagnostico_flujo,
    }


def _construir_timeline_registro(
    secciones_render: list[dict[str, Any]],
    valores_form: dict[str, str],
) -> list[dict[str, Any]]:
    """Construye una linea de tiempo moderna a partir del avance de las secciones."""
    estado_por_slug = {str(item.get("slug", "")): item for item in secciones_render}
    diagnostico_activo = _diagnostico_habilitado(valores_form)
    mapa_etapas = {
        "solicitud": ["solicitud-inicial"],
        "registro-base": ["identificacion-general"],
        "sustento": (
            [
                "diagnostico-paso-1",
                "diagnostico-paso-2",
                "diagnostico-paso-3",
                "diagnostico-paso-4",
                "diagnostico-paso-5",
            ]
            if diagnostico_activo
            else []
        ),
        "diseno": [
            "mi-diseno-contenido",
            "mi-criterios-evaluacion",
            "pt-resumen",
            "pt-sustento",
            "pt-evaluacion",
        ],
        "implementacion": [
            "gr-guia-participante",
            "gr-cuestionario-inicio",
            "gr-cronograma",
            "gr-plataforma",
            "gr-indicadores",
            "is-convocatoria",
            "is-confirmacion",
            "is-seguimiento",
            "ed-reportes",
            "ed-cierre",
        ],
    }

    timeline: list[dict[str, Any]] = []
    current_index = 0

    for index, etapa in enumerate(REGISTRO_CAPACITACION_ETAPAS):
        secciones_etapa = [
            estado_por_slug.get(slug)
            for slug in mapa_etapas.get(str(etapa.get("slug", "")), [])
            if estado_por_slug.get(slug) is not None
        ]
        total_filled = sum(int(item.get("filled_count", 0) or 0) for item in secciones_etapa)
        total_required = sum(int(item.get("required_count", 0) or 0) for item in secciones_etapa)
        total_required_done = sum(int(item.get("required_done", 0) or 0) for item in secciones_etapa)

        is_skipped = not bool(secciones_etapa)
        is_complete = bool(secciones_etapa) and all(bool(item.get("is_complete")) for item in secciones_etapa)
        is_started = total_filled > 0

        timeline.append(
            {
                **etapa,
                "index": index + 1,
                "filled_count": total_filled,
                "required_count": total_required,
                "required_done": total_required_done,
                "is_complete": is_complete,
                "is_started": is_started,
                "is_skipped": is_skipped,
            }
        )

    for index, etapa in enumerate(timeline):
        if bool(etapa.get("is_skipped")):
            continue
        if not bool(etapa.get("is_complete")):
            current_index = index
            break
        current_index = index

    for index, etapa in enumerate(timeline):
        etapa["is_current"] = index == current_index

    return timeline


def _construir_flujo_diagnostico(
    secciones_render: list[dict[str, Any]],
    valores_form: dict[str, str],
) -> dict[str, Any] | None:
    """Construye el modal guiado especifico de diagnostico."""
    diagnostico_activo = _detalle_diagnostico_habilitado(valores_form)

    pasos_base = {
        str(item.get("slug", "")).strip(): item
        for item in secciones_render
    }
    definiciones = [
        {
            "slug": "diagnostico-modal-paso-1",
            "source_slug": "diagnostico-paso-1",
            "titulo": "Paso 1. Registrar datos del proceso",
            "descripcion": "Completa la base normativa, el contexto y las validaciones iniciales del diagnostico.",
        },
        {
            "slug": "diagnostico-modal-paso-2",
            "source_slug": "diagnostico-paso-2",
            "titulo": "Paso 2. Generar matriz de evaluacion",
            "descripcion": "Organiza dimensiones, subdimensiones e indicadores que serviran como base del instrumento.",
        },
        {
            "slug": "diagnostico-modal-paso-3",
            "source_slug": "diagnostico-paso-3",
            "titulo": "Paso 3. Elaborar instrumento(s) de evaluacion",
            "descripcion": "Configura perfiles, selecciona indicadores registrados y redacta los items del instrumento diagnostico.",
        },
        {
            "slug": "diagnostico-modal-paso-4",
            "source_slug": "diagnostico-paso-4",
            "titulo": "Paso 4. Generar instrumento(s) de evaluacion",
            "descripcion": "Deja preparada la salida operativa del instrumento para su uso.",
        },
        {
            "slug": "diagnostico-modal-paso-5",
            "source_slug": "diagnostico-paso-5",
            "titulo": "Paso 5. Resultados e informe",
            "descripcion": "Consolida analisis, evidencias, linea base y justificaciones finales.",
        },
    ]

    pasos: list[dict[str, Any]] = []
    for definicion in definiciones:
        bloque_base = pasos_base.get(str(definicion.get("source_slug", "")))
        if bloque_base is None:
            continue
        campos = _filtrar_campos_registro(
            list(bloque_base.get("campos", [])),
            exclude=set(definicion.get("exclude_fields", set())),
        )
        estado = _contar_estado_bloque_registro(campos)
        pasos.append(
            {
                **bloque_base,
                **estado,
                "slug": definicion["slug"],
                "titulo": definicion["titulo"],
                "descripcion": definicion["descripcion"],
                "campos": campos,
            }
        )

    if not pasos:
        return None

    current_index = 0
    for index, paso in enumerate(pasos):
        if not bool(paso.get("is_complete")):
            current_index = index
            break
        current_index = index

    for index, paso in enumerate(pasos):
        paso["step_index"] = index + 1
        paso["is_current_step"] = index == current_index

    return {
        "slug": "diagnostico-modal",
        "titulo": "Diagnostico",
        "descripcion": "Modal guiado de cinco pasos para registrar el proceso, estructurar la evaluacion y consolidar el informe diagnostico.",
        "is_enabled": diagnostico_activo,
        "steps": pasos,
        "cap_nombre": str(valores_form.get("cap_nombre", "")).strip(),
        "cap_codigo": str(valores_form.get("cap_codigo", "")).strip(),
        "cap_anio": str(valores_form.get("cap_anio", "")).strip(),
    }


def _construir_flujo_matriz_sustento(
    secciones_render: list[dict[str, Any]],
    valores_form: dict[str, str],
) -> dict[str, Any] | None:
    """Construye el modal de matriz de sustento inspirado en el flujo de tres pasos."""
    matriz_activa = _matriz_habilitada(valores_form)

    pasos_base = {
        str(item.get("slug", "")).strip(): item
        for item in secciones_render
    }
    definiciones = [
        {
            "slug": "matriz-modal-paso-1",
            "source_slug": "diagnostico-paso-1",
            "titulo": "Paso 1. Registrar datos del proceso",
            "descripcion": "Completa la base normativa, el contexto y el listado de problemas que alimentaran la matriz.",
        },
        {
            "slug": "matriz-modal-paso-2",
            "source_slug": "diagnostico-paso-2",
            "titulo": "Paso 2. Registrar indicadores de gestion",
            "descripcion": "Para cada problema registra indicadores y competencias vinculadas, siguiendo la secuencia de la matriz de sustento.",
        },
        {
            "slug": "matriz-modal-paso-3",
            "source_slug": "diagnostico-paso-5",
            "titulo": "Paso 3. Registrar proyeccion de resultados",
            "descripcion": "Formula expectativas de cambio y resultados esperados por cada problema priorizado.",
            "include_fields": {"diag_resultados_json"},
        },
    ]

    pasos: list[dict[str, Any]] = []
    for definicion in definiciones:
        bloque_base = pasos_base.get(str(definicion.get("source_slug", "")))
        if bloque_base is None:
            continue
        campos = _filtrar_campos_registro(
            list(bloque_base.get("campos", [])),
            include=set(definicion.get("include_fields", set())),
        )
        if not definicion.get("include_fields"):
            campos = _filtrar_campos_registro(list(bloque_base.get("campos", [])))
        estado = _contar_estado_bloque_registro(campos)
        pasos.append(
            {
                **bloque_base,
                **estado,
                "slug": definicion["slug"],
                "titulo": definicion["titulo"],
                "descripcion": definicion["descripcion"],
                "campos": campos,
            }
        )

    if not pasos:
        return None

    current_index = 0
    for index, paso in enumerate(pasos):
        if not bool(paso.get("is_complete")):
            current_index = index
            break
        current_index = index

    for index, paso in enumerate(pasos):
        paso["step_index"] = index + 1
        paso["is_current_step"] = index == current_index

    return {
        "slug": "matriz-sustento-modal",
        "titulo": "Matriz de sustento",
        "descripcion": "Modal guiado para documentar problemas, indicadores y proyeccion de resultados antes del diseno formativo.",
        "is_enabled": matriz_activa,
        "steps": pasos,
        "cap_nombre": str(valores_form.get("cap_nombre", "")).strip(),
        "cap_codigo": str(valores_form.get("cap_codigo", "")).strip(),
        "cap_anio": str(valores_form.get("cap_anio", "")).strip(),
    }


def _construir_flujo_expediente(
    secciones_render: list[dict[str, Any]],
    valores_form: dict[str, str],
) -> dict[str, Any] | None:
    """Agrupa el expediente en una secuencia comun posterior al sustento inicial."""
    bloques_por_slug = {
        str(item.get("slug", "")).strip(): item
        for item in secciones_render
    }

    definiciones = [
        {
            "slug": "expediente-diseno-matriz",
            "titulo": "Diseno de la Matriz Instruccional (+ alcance)",
            "descripcion": "Objetivo, competencias, desempenos, malla curricular y criterios de evaluacion del diseno formativo.",
            "block_slugs": [
                "mi-diseno-contenido",
                "mi-criterios-evaluacion",
            ],
        },
        {
            "slug": "expediente-plan-trabajo",
            "titulo": "Plan de Trabajo",
            "descripcion": "Consolida la ficha operativa, sustento, evaluacion y programacion para la ejecucion.",
            "block_slugs": [
                "pt-resumen",
                "pt-sustento",
                "pt-evaluacion",
            ],
        },
        {
            "slug": "expediente-generacion-recursos",
            "titulo": "Generacion de recursos",
            "descripcion": "Guia del participante, cuestionario de inicio, cronograma, plataforma e indicadores de calidad.",
            "block_slugs": [
                "gr-guia-participante",
                "gr-cuestionario-inicio",
                "gr-cronograma",
                "gr-plataforma",
                "gr-indicadores",
            ],
        },
        {
            "slug": "expediente-implementacion",
            "titulo": "Implementacion y seguimiento",
            "descripcion": "Oficios de convocatoria y confirmacion, seguimiento a los proyectos formativos.",
            "block_slugs": [
                "is-convocatoria",
                "is-confirmacion",
                "is-seguimiento",
            ],
        },
        {
            "slug": "expediente-evaluacion",
            "titulo": "Evaluacion y documentacion",
            "descripcion": "Reportes finales, certificados, oficios de resultados y cierre de la capacitacion.",
            "block_slugs": [
                "ed-reportes",
                "ed-cierre",
            ],
        },
    ]

    pasos: list[dict[str, Any]] = []
    for definicion in definiciones:
        bloques = [
            {**bloques_por_slug[slug]}
            for slug in definicion.get("block_slugs", [])
            if slug in bloques_por_slug
        ]

        for bloque in bloques:
            if bloque is None:
                continue
            bloque["modal_id"] = f"expediente-modal-{definicion['slug']}-{bloque['slug']}"

        required_count = sum(int(item.get("required_count", 0) or 0) for item in bloques)
        required_done = sum(int(item.get("required_done", 0) or 0) for item in bloques)
        filled_count = sum(int(item.get("filled_count", 0) or 0) for item in bloques)
        is_complete = bool(bloques) and all(bool(item.get("is_complete")) for item in bloques)
        is_started = filled_count > 0

        paso = {
            **definicion,
            "blocks": bloques,
            "required_count": required_count,
            "required_done": required_done,
            "filled_count": filled_count,
            "is_complete": is_complete,
            "is_started": is_started,
        }

        if definicion["slug"] == "expediente-plan-trabajo":
            paso["summary_cards"] = [
                {
                    "label": "Capacitacion",
                    "value": str(valores_form.get("cap_nombre", "")).strip() or "Pendiente",
                },
                {
                    "label": "Origen",
                    "value": str(valores_form.get("sol_origen_institucional", "")).strip() or "Pendiente",
                },
                {
                    "label": "Publico",
                    "value": str(valores_form.get("pob_tipo", "")).strip() or "Pendiente",
                },
                {
                    "label": "Modalidad",
                    "value": str(valores_form.get("pt_modalidad", "")).strip() or "Pendiente",
                },
            ]

        pasos.append(paso)

    if not pasos:
        return None

    current_index = 0
    for index, paso in enumerate(pasos):
        if not bool(paso.get("is_complete")):
            current_index = index
            break
        current_index = index

    for index, paso in enumerate(pasos):
        paso["step_index"] = index + 1
        paso["is_current_step"] = index == current_index

    return {
        "slug": "expediente-comun",
        "titulo": "Ruta comun posterior",
        "descripcion": "Luego de la matriz de sustento o del diagnostico, ambas rutas desembocan en esta secuencia comun del expediente.",
        "steps": pasos,
    }


def _construir_flujo_unificado(
    secciones_render: list[dict[str, Any]],
    valores_form: dict[str, str],
    solicitud_bloque: dict[str, Any] | None,
    sustento_etapa: dict[str, Any] | None,
    expediente_flujo: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    """Construye una linea de tiempo unica con todos los pasos del registro."""
    pasos: list[dict[str, Any]] = []

    # Paso 1: Solicitud
    sol_req = sol_done = sol_filled = 0
    if solicitud_bloque:
        sol_req = int(solicitud_bloque.get("required_count", 0) or 0)
        sol_done = int(solicitud_bloque.get("required_done", 0) or 0)
        sol_filled = int(solicitud_bloque.get("filled_count", 0) or 0)
    pasos.append({
        "slug": "paso-solicitud",
        "titulo": "Solicitud",
        "descripcion": "Clasifica el origen y los insumos iniciales del pedido.",
        "panel_type": "solicitud",
        "required_count": sol_req,
        "required_done": sol_done,
        "filled_count": sol_filled,
        "is_complete": sol_req > 0 and sol_done >= sol_req,
        "is_started": sol_filled > 0,
    })

    # Paso 2: Sustento tecnico
    pasos.append({
        "slug": "paso-sustento",
        "titulo": "Sustento tecnico",
        "descripcion": "Desarrolla diagnostico, brechas y alineamiento tecnico.",
        "panel_type": "sustento",
        "required_count": 0,
        "required_done": 0,
        "filled_count": 0,
        "is_complete": False,
        "is_started": bool(sustento_etapa and sustento_etapa.get("is_enabled")),
    })

    # Pasos 3-7: Expediente
    if expediente_flujo:
        for exp_paso in expediente_flujo.get("steps", []):
            pasos.append({
                **exp_paso,
                "panel_type": "expediente",
            })

    current_index = 0
    for index, paso in enumerate(pasos):
        if not bool(paso.get("is_complete")):
            current_index = index
            break
        current_index = index

    for index, paso in enumerate(pasos):
        paso["step_index"] = index + 1
        paso["is_current_step"] = index == current_index

    return pasos


# Secciones que se validan al crear (solo paso 1 – solicitud).
SECCIONES_PASO_1 = {"solicitud-inicial"}


def _validar_registro_capacitacion(
    payload_raw: dict[str, str],
    secciones_validas: set[str] | None = None,
 ) -> tuple[list[str], dict[str, Any]]:
    """Valida campos del registro de capacitacion y retorna payload tipado.

    Si *secciones_validas* se indica, solo se verifican los campos obligatorios
    de esas secciones (util para guardar solo el paso 1).
    """
    errores: list[str] = []
    payload_tipado: dict[str, Any] = {}
    origen_solicitud = str(payload_raw.get("sol_origen_institucional", "")).strip()
    campos_omitidos_condicionales = {
        "sol_region_iged",
        "sol_iged_nombre",
    }

    for campo in iterar_campos_registro_capacitacion(secciones_filtro=secciones_validas):
        codigo = str(campo.get("codigo", "")).strip()
        etiqueta = str(campo.get("pregunta", codigo)).strip()
        tipo = str(campo.get("tipo", "text_short")).strip()
        obligatorio = bool(campo.get("obligatorio"))
        opciones = [str(op).strip() for op in list(campo.get("opciones", []))]

        valor_raw = str(payload_raw.get(codigo, "")).strip()

        # Los campos condicionales se validan luego segun el origen seleccionado.
        if codigo in campos_omitidos_condicionales:
            payload_tipado[codigo] = _coercer_valor_registro_capacitacion(tipo, valor_raw)
            continue

        # Regla de obligatoriedad inicial.
        if obligatorio and not valor_raw:
            errores.append(f"Falta campo obligatorio: {etiqueta}.")
            payload_tipado[codigo] = None
            continue

        # Regla de opciones validas para listas desplegables.
        if valor_raw and opciones and valor_raw not in opciones:
            errores.append(f"Valor no valido en: {etiqueta}.")

        valor_tipado = _coercer_valor_registro_capacitacion(tipo, valor_raw)

        # Regla de formato numerico para enteros/decimales.
        if valor_raw and valor_tipado is None and tipo in {"integer", "decimal", "currency"}:
            errores.append(f"Formato numerico invalido en: {etiqueta}.")

        payload_tipado[codigo] = valor_tipado

    # Regla condicional: si el origen es externo, se requiere oficio y unidad solicitante.
    if origen_solicitud in {"IGED", "Unidad orgánica"}:
        campos_condicionales = [
            ("sol_numero_oficio", "Numero de oficio"),
            ("sol_fecha_oficio", "Fecha de oficio"),
        ]
        for codigo, etiqueta in campos_condicionales:
            if not str(payload_raw.get(codigo, "")).strip():
                errores.append(
                    f"Falta campo obligatorio para solicitudes externas: {etiqueta}."
                )
                payload_tipado[codigo] = None

    if origen_solicitud == "IGED":
        campos_iged = [
            ("sol_region_iged", "Region"),
            ("sol_iged_nombre", "IGED"),
        ]
        for codigo, etiqueta in campos_iged:
            if not str(payload_raw.get(codigo, "")).strip():
                errores.append(
                    f"Falta campo obligatorio para solicitudes IGED: {etiqueta}."
                )
                payload_tipado[codigo] = None

    return errores, payload_tipado


def _construir_estado_plantilla(
    postulantes_info: dict[str, Any],
    actividades_plantilla: dict[str, list[dict[str, Any]]],
    nominal_config: dict[str, Any],
) -> dict[str, Any]:
    """Calcula checklist de completitud para habilitar generacion de plantilla."""
    # Evalua carga de postulantes.
    # Nota operativa: este bloque NO es requisito para generar plantilla;
    # solo se usa para cierre administrativo de la capacitacion.
    post_ok = bool(postulantes_info.get("exists"))
    post_size = int(postulantes_info.get("size_bytes") or 0)
    post_resumen = (
        f"Archivo cargado ({post_size} bytes)"
        if post_ok
        else "Opcional para generar (requerido para cierre)"
    )

    # Evalua estructura de actividades dentro de plataforma.
    actividades_plataforma = list(actividades_plantilla.get("plataforma", []))
    total_plataforma = len(actividades_plataforma)
    plataforma_ok = total_plataforma > 0
    plataforma_resumen = (
        f"{total_plataforma} actividad(es) detectadas"
        if plataforma_ok
        else "No hay actividades de origen Plataforma"
    )

    # Evalua actividades fuera y sus archivos.
    actividades_fuera = list(actividades_plantilla.get("fuera", []))
    total_fuera = len(actividades_fuera)
    total_fuera_archivos = sum(1 for item in actividades_fuera if bool(item.get("archivo_existe")))
    fuera_ok = total_fuera == 0 or total_fuera_archivos == total_fuera
    if total_fuera == 0:
        fuera_resumen = "Sin actividades fuera por cargar"
    else:
        fuera_resumen = f"{total_fuera_archivos}/{total_fuera} archivo(s) cargados"

    # Evalua configuracion nominal (titulo, columnas y nombres de grupo).
    titulo_nominal = str(nominal_config.get("titulo_nominal", "")).strip()
    columnas_nominal = list(nominal_config.get("columnas_seleccionadas", []))
    grupos_detalle = list(nominal_config.get("grupos_detalle", []))
    grupos_ok = all(str(item.get("nombre", "")).strip() for item in grupos_detalle) if grupos_detalle else True
    nominal_ok = bool(titulo_nominal) and len(columnas_nominal) > 0 and grupos_ok
    nominal_resumen = (
        f"{len(columnas_nominal)} columna(s) seleccionadas"
        if nominal_ok
        else "Completa titulo, columnas y grupos"
    )

    checkpoints = [
        {
            "slug": "plataforma",
            "titulo": "Actividades dentro de plataforma",
            "icono": "🧩",
            "resumen": plataforma_resumen,
            "ok": plataforma_ok,
            "anchor": "sec-plataforma",
        },
        {
            "slug": "fuera",
            "titulo": "Actividades fuera de plataforma",
            "icono": "📁",
            "resumen": fuera_resumen,
            "ok": fuera_ok,
            "anchor": "sec-fuera",
        },
        {
            "slug": "nominal",
            "titulo": "Configuración de reporte nominal",
            "icono": "⚙️",
            "resumen": nominal_resumen,
            "ok": nominal_ok,
            "anchor": "sec-nominal",
        },
    ]

    # Estado general para habilitar boton de generar plantilla.
    # Solo se consideran requeridos: plataforma, fuera y nominal.
    slugs_requeridos_generacion = {"plataforma", "fuera", "nominal"}
    pendientes = [
        item["titulo"]
        for item in checkpoints
        if item.get("slug") in slugs_requeridos_generacion and not bool(item.get("ok"))
    ]
    ready = len(pendientes) == 0
    completados = sum(
        1
        for item in checkpoints
        if item.get("slug") in slugs_requeridos_generacion and bool(item.get("ok"))
    )
    total = len(slugs_requeridos_generacion)

    # Pendientes operativos para cierre, no bloquean generacion.
    pendientes_cierre = []
    if not post_ok:
        pendientes_cierre.append("Carga de Excel de postulantes")

    return {
        "checkpoints": checkpoints,
        "ready": ready,
        "pendientes": pendientes,
        "completados": completados,
        "total": total,
        "pendientes_cierre": pendientes_cierre,
    }


def _allowed_roles_for_base(base_role: str) -> list[str]:
    """Retorna roles permitidos segun rol base del usuario."""
    # Normaliza texto para comparaciones tolerantes.
    normalized_base = str(base_role or "").strip().lower()

    # Solo el rol base Administrador puede alternar a modo estandar.
    if normalized_base == "administrador":
        return ["Administrador", "Usuario estandar"]

    # Para cualquier otro rol no se permite alternancia de modo.
    return [base_role]


def _resolve_roles(request) -> tuple[str, str, list[str]]:
    """Resuelve rol base, rol efectivo y lista de roles permitidos."""
    # Rol actual heredado del backend al autenticar.
    current_role = request.session.get("difoca_role", "Invitado")

    # Rol base permanente de la sesion (fuente de permisos maximos).
    base_role = request.session.get("difoca_role_base")
    if not base_role:
        base_role = current_role
        request.session["difoca_role_base"] = base_role

    # Roles permitidos segun rol base.
    allowed_roles = _allowed_roles_for_base(base_role)

    # Rol efectivo para visualizacion/filtrado.
    effective_role = request.session.get("difoca_role_effective")
    if not effective_role:
        effective_role = current_role

    # Si el rol efectivo no es valido, se corrige al rol base.
    if effective_role not in allowed_roles:
        effective_role = allowed_roles[0]

    # Sincroniza valores de sesion para evitar estados inconsistentes.
    request.session["difoca_role_effective"] = effective_role
    request.session["difoca_role"] = effective_role

    return base_role, effective_role, allowed_roles


def _build_user_context(request) -> dict[str, Any]:
    """Construye contexto base de identidad y rol para vistas protegidas."""
    # Prioriza nombre guardado en sesion por backend personalizado.
    # Si no existe, usa first_name y finalmente username de Django.
    display_name = (
        request.session.get("difoca_name")
        or request.user.first_name
        or request.user.username
    )

    # Resuelve roles base/efectivo y opciones permitidas.
    base_role, effective_role, allowed_roles = _resolve_roles(request)

    return {
        "display_name": display_name,
        "role": effective_role,  # Compatibilidad con templates existentes.
        "role_base": base_role,
        "role_effective": effective_role,
        "role_options": allowed_roles,
        "can_switch_role": len(allowed_roles) > 1,
        "sync_status_bar": build_sync_status_context(),
        # Ruta actual para volver a la misma pantalla tras cambiar rol.
        "current_path": request.get_full_path(),
    }


@login_required
def home_view(request):
    """Renderiza la pantalla inicial con GeoMenu de modulos."""
    # Construye contexto de identidad.
    user_context = _build_user_context(request)
    # Calcula cantidad total de modulos legacy agrupados.
    total_modulos = sum(len(seccion["modulos"]) for seccion in MENU_GEOMETRICO)

    # Renderiza pagina inicial con menu de secciones.
    return render(
        request,
        "core/home.html",
        {
            **user_context,
            "menu_sections": MENU_GEOMETRICO,
            "total_sections": len(MENU_GEOMETRICO),
            "total_modulos": total_modulos,
        },
    )


@login_required
def section_detail_view(request, section_slug: str):
    """Renderiza vista detalle de una seccion del GeoMenu."""
    # Busca la seccion por slug.
    section = MENU_POR_SLUG.get(section_slug)
    # Si no existe, retorna 404.
    if section is None:
        raise Http404("Seccion no encontrada.")

    section_submenus = section.get("submenus", [])
    if section_slug == "reporte-indicadores" and len(section_submenus) == 1:
        return redirect("core:submenu_detail", section_slug, str(section_submenus[0].get("slug", "dashboard-kpi")))

    # Construye contexto de identidad.
    user_context = _build_user_context(request)

    # Renderiza pagina detalle de la seccion seleccionada.
    return render(
        request,
        "core/section_detail.html",
        {
            **user_context,
            "section": section,
            # Secciones completas para renderizar barra de pestanas de navegacion.
            "menu_sections": MENU_GEOMETRICO,
            # Submenus de la seccion activa (si aplica).
            "section_submenus": section_submenus if len(section_submenus) > 1 else [],
        },
    )


@login_required
def submenu_detail_view(request, section_slug: str, submenu_slug: str):
    """Renderiza una pantalla de submenu interno dentro de una seccion."""
    # Resuelve seccion y submenu solicitado.
    section, submenu = _buscar_submenu(section_slug, submenu_slug)

    # Construye contexto comun de identidad.
    user_context = _build_user_context(request)

    # Lee parametro de anio para filtros de datos legacy.
    anio_param = str(request.GET.get("anio", "")).strip()

    # Contexto base comun para cualquier submenu.
    context: dict[str, Any] = {
        **user_context,
        "section": section,
        "submenu": submenu,
        "menu_sections": MENU_GEOMETRICO,
        "section_submenus": section.get("submenus", []) if len(section.get("submenus", [])) > 1 else [],
        "adapter_kind": submenu.get("adapter", "placeholder"),
        # Permite ocultar filtro de año en submenus donde no aporta al flujo.
        "mostrar_filtro_anio": submenu_slug not in {"registrar-nueva-capacitacion", "editar-capacitacion"},
        "anios_disponibles": [],
        "anio_seleccionado": None,
        "capacitaciones": [],
    }

    # Procesa creacion de "Registrar nueva capacitacion" en MySQL Railway.
    if (
        request.method == "POST"
        and section_slug == "gestion-capacitacion"
        and submenu_slug == "registrar-nueva-capacitacion"
    ):
        action = str(request.POST.get("action", "")).strip()
        draft_key = "registro_capacitacion_form_draft"

        # Ruta de retorno a la misma pantalla (sin parametros de año).
        redirect_url = _build_submenu_url(section_slug, submenu_slug, {})

        try:
            if action == "create_capacitacion":
                # Lee solo los campos de la seccion de solicitud (Paso 1).
                payload_raw: dict[str, str] = {}
                for campo in iterar_campos_registro_capacitacion(secciones_filtro=SECCIONES_PASO_1):
                    codigo = str(campo.get("codigo", "")).strip()
                    payload_raw[codigo] = str(request.POST.get(codigo, "")).strip()

            # Campos extra que pueden venir pre-llenados desde oferta_formativa.
            for extra in ("cap_codigo", "cap_tipo"):
                val = str(request.POST.get(extra, "")).strip()
                if val:
                    payload_raw[extra] = val

            # Valida solo campos obligatorios del paso 1.
            errores, payload_tipado = _validar_registro_capacitacion(
                payload_raw, secciones_validas=SECCIONES_PASO_1,
            )
            payload_tipado["cap_estado"] = "Borrador"

            if errores:
                # Conserva borrador para no perder avance al corregir campos.
                request.session[draft_key] = payload_raw
                for mensaje in errores[:8]:
                    messages.error(request, mensaje)
                if len(errores) > 8:
                    messages.error(
                        request,
                        f"Se detectaron {len(errores)} observaciones. Corrige los campos marcados como obligatorios.",
                    )
            else:
                # Crea la capacitacion en la BD (solo datos de solicitud).
                resultado = crear_registro_capacitacion(
                    payload=payload_tipado,
                    creado_por=str(request.user.username),
                    creado_nombre=str(user_context.get("display_name", "")),
                )
                if resultado.get("ok"):
                    request.session.pop(draft_key, None)
                    messages.success(
                        request,
                        "Capacitacion registrada como borrador. "
                        "Puedes continuar la edicion desde el modulo Editar capacitacion.",
                    )
                else:
                    request.session[draft_key] = payload_raw
                    messages.error(
                        request,
                        "No se pudo registrar la capacitacion en la base de datos: "
                        f"{resultado.get('error', 'Error desconocido')}",
                    )
        except Exception as exc:
            import logging
            logging.getLogger("core.views").exception("Error en create_capacitacion POST")
            messages.error(request, f"Error inesperado al registrar: {exc}")

        return redirect(redirect_url)

    # Procesa edicion de capacitacion existente (guardar cambios por paso).
    if (
        request.method == "POST"
        and section_slug == "gestion-capacitacion"
        and submenu_slug == "editar-capacitacion"
    ):
        from core.models import Capacitacion

        action = str(request.POST.get("action", "")).strip()
        cap_id = str(request.POST.get("cap_id", "")).strip()

        redirect_params: dict[str, str] = {}
        if cap_id:
            redirect_params["id"] = cap_id

        if action == "save_id_plataforma" and cap_id:
            username = str(request.user.username)
            role_eff = str(user_context.get("role_effective", ""))
            is_admin = _normalizar_texto(role_eff) in {
                "administrador", "admin", "superusuario",
            }
            try:
                qs = Capacitacion.objects.all()
                if not is_admin:
                    qs = qs.filter(creado_por=username)
                cap_obj = qs.get(pk=int(cap_id))
                cap_obj.cap_codigo = str(request.POST.get("cap_codigo", "")).strip()
                cap_obj.cap_id_curso = str(request.POST.get("cap_id_curso", "")).strip()
                cap_obj.save(update_fields=["cap_codigo", "cap_id_curso", "actualizado_en"])
                messages.success(request, "Identificadores de plataforma actualizados.")
            except Capacitacion.DoesNotExist:
                messages.error(request, "No se encontró la capacitación o no tienes permisos.")
            except Exception as exc:
                messages.error(request, f"Error al guardar identificadores: {exc}")
            return redirect(_build_submenu_url(section_slug, submenu_slug, {}))

        if action == "save_capacitacion" and cap_id:
            username = str(request.user.username)
            role_eff = str(user_context.get("role_effective", ""))
            is_admin = _normalizar_texto(role_eff) in {
                "administrador", "admin", "superusuario",
            }

            try:
                qs = Capacitacion.objects.all()
                if not is_admin:
                    qs = qs.filter(creado_por=username)
                cap_obj = qs.get(pk=int(cap_id))

                # Lee TODOS los campos del formulario (sin filtro de seccion).
                for campo in iterar_campos_registro_capacitacion():
                    codigo = str(campo.get("codigo", "")).strip()
                    tipo = str(campo.get("tipo", "")).strip()
                    if tipo == "hidden_json":
                        continue
                    raw_val = str(request.POST.get(codigo, "")).strip()
                    if not hasattr(cap_obj, codigo):
                        continue
                    coerced = _coercer_valor_registro_capacitacion(tipo, raw_val)
                    # Los campos de fecha/entero/decimal aceptan null; usar None en vez de "".
                    if coerced is None:
                        field_obj = cap_obj._meta.get_field(codigo)
                        if field_obj.null:
                            setattr(cap_obj, codigo, None)
                        else:
                            setattr(cap_obj, codigo, "")
                    else:
                        setattr(cap_obj, codigo, coerced)

                # Actualiza paso_actual si se envio.
                paso_post = request.POST.get("paso_actual", "")
                if paso_post:
                    try:
                        cap_obj.paso_actual = max(1, int(paso_post))
                    except (ValueError, TypeError):
                        pass

                cap_obj.save()
                messages.success(request, "Capacitacion actualizada correctamente.")
            except Capacitacion.DoesNotExist:
                messages.error(request, "No se encontro la capacitacion o no tienes permisos para editarla.")
            except Exception as exc:
                messages.error(request, f"Error al guardar: {exc}")

        return redirect(_build_submenu_url(section_slug, submenu_slug, redirect_params))

    # Procesa acciones POST para seguimiento (CRUD sin salir de la vista).
    if (
        request.method == "POST"
        and section_slug == "gestion-capacitacion"
        and submenu_slug == "seguimiento-capacitaciones"
    ):
        # Clave de sesion para conservar borrador del formulario de estructura.
        draft_key = "seguimiento_estructura_form_draft"
        action = str(request.POST.get("action", "")).strip()
        post_anio = str(request.POST.get("anio", "")).strip()
        post_codigo = str(request.POST.get("codigo", "")).strip()
        post_tab = str(request.POST.get("tab", "alertas")).strip() or "alertas"

        # Params de retorno para mantener contexto visual tras una operacion.
        redirect_params = {"anio": post_anio, "codigo": post_codigo, "tab": post_tab}

        # Normaliza datos comunes del formulario de estructura.
        estructura_data_raw = {
            "actividad": str(request.POST.get("actividad", "")).strip(),
            "codigo_actividad": str(request.POST.get("codigo_actividad", "")).strip(),
            "cumplimiento_nota": str(request.POST.get("cumplimiento_nota", "Cumplimiento")).strip(),
            "origen": str(request.POST.get("origen", "Plataforma")).strip(),
            "inicio": str(request.POST.get("inicio", "")).strip(),
            "fin": str(request.POST.get("fin", "")).strip(),
            "escala": str(request.POST.get("escala", "Vigesimal")).strip(),
            "tipo": str(request.POST.get("tipo", "Ejercicio")).strip(),
            "obligatoria": str(request.POST.get("obligatoria", "")).strip().lower() in {"1", "true", "on", "si", "sí"},
            "enlace": str(request.POST.get("enlace", "")).strip().lower() in {"1", "true", "on", "si", "sí"},
            "observaciones": str(request.POST.get("observaciones", "")).strip(),
            "aplica_a": str(request.POST.get("aplica_a", "Ambos")).strip(),
        }
        estructura_data = {
            "actividad": estructura_data_raw["actividad"],
            "codigo_actividad": estructura_data_raw["codigo_actividad"],
            "cumplimiento_nota": estructura_data_raw["cumplimiento_nota"],
            "origen": estructura_data_raw["origen"],
            "inicio": _parse_datetime_local(estructura_data_raw["inicio"]),
            "fin": _parse_datetime_local(estructura_data_raw["fin"]),
            "escala": estructura_data_raw["escala"],
            "tipo": estructura_data_raw["tipo"],
            "obligatoria": estructura_data_raw["obligatoria"],
            "enlace": estructura_data_raw["enlace"],
            "observaciones": estructura_data_raw["observaciones"],
            "aplica_a": estructura_data_raw["aplica_a"],
        }

        # Reglas de campos obligatorios para evitar perder avance del usuario.
        required_fields = [
            ("actividad", "Actividad"),
            ("codigo_actividad", "Código de actividad"),
            ("tipo", "Tipo"),
            ("cumplimiento_nota", "Cumplimiento/Nota"),
            ("origen", "Origen"),
            ("escala", "Escala"),
            ("aplica_a", "Aplica a"),
            ("inicio", "Inicio"),
            ("fin", "Fin"),
        ]

        if action == "add_estructura":
            faltantes = [label for key, label in required_fields if not str(estructura_data_raw.get(key, "")).strip()]
            if faltantes:
                messages.error(request, f"Faltan datos obligatorios: {', '.join(faltantes)}.")
                request.session[draft_key] = {
                    "codigo": post_codigo,
                    "edit_id": 0,
                    "data": estructura_data_raw,
                }
                redirect_params["modal"] = "1"
            elif agregar_actividad_estructura(post_codigo, estructura_data):
                messages.success(request, "Actividad registrada correctamente en estructura.")
                request.session.pop(draft_key, None)
            else:
                messages.error(request, "No se pudo registrar la actividad en estructura.")
                request.session[draft_key] = {
                    "codigo": post_codigo,
                    "edit_id": 0,
                    "data": estructura_data_raw,
                }
                redirect_params["modal"] = "1"

        elif action == "update_estructura":
            row_id = int(request.POST.get("id_estructura", "0") or 0)
            faltantes = [label for key, label in required_fields if not str(estructura_data_raw.get(key, "")).strip()]
            if faltantes:
                messages.error(request, f"Faltan datos obligatorios: {', '.join(faltantes)}.")
                request.session[draft_key] = {
                    "codigo": post_codigo,
                    "edit_id": row_id,
                    "data": estructura_data_raw,
                }
                redirect_params["edit"] = row_id
                redirect_params["modal"] = "1"
            elif row_id and actualizar_actividad_estructura(row_id, estructura_data):
                messages.success(request, "Actividad actualizada correctamente.")
                request.session.pop(draft_key, None)
            else:
                messages.error(request, "No se pudo actualizar la actividad.")
                request.session[draft_key] = {
                    "codigo": post_codigo,
                    "edit_id": row_id,
                    "data": estructura_data_raw,
                }
                redirect_params["edit"] = row_id
                redirect_params["modal"] = "1"

        elif action == "delete_estructura":
            row_id = int(request.POST.get("id_estructura", "0") or 0)
            if row_id and eliminar_actividad_estructura(row_id):
                messages.success(request, "Actividad eliminada de estructura.")
            else:
                messages.error(request, "No se pudo eliminar la actividad seleccionada.")

        elif action == "save_formula":
            aplica_a = str(request.POST.get("aplica_a", "Ambos")).strip()
            formula = str(request.POST.get("formula", "")).strip()
            if guardar_formula_promedio(post_codigo, aplica_a, formula):
                messages.success(request, "Formula de promedio guardada correctamente.")
            else:
                messages.error(request, "No se pudo guardar la formula de promedio.")

        elif action == "delete_formula":
            formula_id = int(request.POST.get("id_formula", "0") or 0)
            if formula_id and eliminar_formula_promedio(formula_id):
                messages.success(request, "Formula eliminada correctamente.")
            else:
                messages.error(request, "No se pudo eliminar la formula.")

        elif action == "save_rutas_plantilla":
            excel_path = str(request.POST.get("excel_path", "")).strip()
            py_path = str(request.POST.get("py_path", "")).strip()
            if guardar_rutas_plantilla(post_codigo, excel_path, py_path):
                messages.success(request, "Rutas de plantilla guardadas correctamente.")
            else:
                messages.error(request, "No se pudo guardar las rutas de plantilla.")

        elif action == "upload_postulantes_excel":
            archivo = request.FILES.get("archivo_postulantes")
            if archivo is None:
                messages.error(request, "No se envio archivo de postulantes.")
            elif not str(archivo.name).lower().endswith(".xlsx"):
                messages.error(request, "El archivo debe tener extension .xlsx.")
            elif guardar_postulantes_excel(post_codigo, archivo.read()):
                messages.success(request, "Excel de postulantes cargado correctamente.")
            else:
                messages.error(request, "No se pudo guardar el Excel de postulantes.")

        elif action == "upload_actividad_fuera_excel":
            archivo = request.FILES.get("archivo_actividad_fuera")
            row_id = int(request.POST.get("id_estructura", "0") or 0)
            if archivo is None:
                messages.error(request, "No se envio archivo para la actividad seleccionada.")
            elif not str(archivo.name).lower().endswith(".xlsx"):
                messages.error(request, "El archivo de actividad debe tener extension .xlsx.")
            elif not row_id:
                messages.error(request, "No se pudo identificar la actividad de origen Fuera.")
            elif guardar_excel_actividad_fuera(post_codigo, row_id, archivo.read()):
                messages.success(request, "Archivo de actividad fuera cargado correctamente.")
            else:
                messages.error(request, "No se pudo guardar el archivo de actividad fuera.")

        elif action == "delete_actividad_fuera_excel":
            row_id = int(request.POST.get("id_estructura", "0") or 0)
            if row_id and eliminar_excel_actividad_fuera(post_codigo, row_id):
                messages.success(request, "Archivo de actividad fuera eliminado.")
            else:
                messages.error(request, "No se pudo eliminar el archivo de actividad fuera.")

        elif action == "delete_postulantes_excel":
            if eliminar_postulantes_excel(post_codigo):
                messages.success(request, "Archivo de postulantes eliminado.")
            else:
                messages.error(request, "No se pudo eliminar el archivo de postulantes.")

        elif action == "save_config_nominal":
            titulo_nominal = str(request.POST.get("titulo_nominal", "")).strip()
            columnas_seleccionadas = request.POST.getlist("columnas_nominal")

            # Recupera estructura para validar columnas y grupos antes de guardar.
            estructura_post = obtener_estructura_por_codigo(post_codigo) if post_codigo else []
            nominal_actual = obtener_config_nominal_reporte(post_codigo, estructura_post) if post_codigo else {
                "columnas_disponibles": [],
                "grupos": [],
            }

            columnas_disponibles = list(nominal_actual.get("columnas_disponibles", []))
            grupos = list(nominal_actual.get("grupos", []))

            nombres_grupo: dict[str, str] = {}
            for grupo in grupos:
                key = f"grupo_nombre_{grupo}"
                nombres_grupo[str(grupo)] = str(request.POST.get(key, "")).strip()

            if guardar_config_nominal_reporte(
                post_codigo,
                columnas_disponibles,
                columnas_seleccionadas,
                titulo_nominal,
                nombres_grupo,
            ):
                messages.success(request, "Configuración nominal guardada correctamente.")
            else:
                messages.error(request, "No se pudo guardar la configuración nominal.")

        elif action == "generate_plantilla":
            # Recalcula checklist antes de permitir generacion.
            estructura_post = obtener_estructura_por_codigo(post_codigo) if post_codigo else []
            postulantes_post = obtener_postulantes_excel_info(post_codigo) if post_codigo else {"exists": False, "size_bytes": 0}
            actividades_post = obtener_actividades_plantilla(post_codigo, estructura_post) if post_codigo else {"plataforma": [], "fuera": []}
            nominal_post = obtener_config_nominal_reporte(post_codigo, estructura_post) if post_codigo else {"titulo_nominal": "", "columnas_seleccionadas": [], "grupos_detalle": []}
            estado_post = _construir_estado_plantilla(postulantes_post, actividades_post, nominal_post)

            if not estado_post.get("ready"):
                faltantes = ", ".join(estado_post.get("pendientes", []))
                messages.error(
                    request,
                    f"No se puede generar plantilla aún. Falta completar: {faltantes}.",
                )
            else:
                # Ejecuta generacion real de archivo XLSX dentro de app_cap_difoca.
                try:
                    anio_gen = int(post_anio) if str(post_anio).isdigit() else None
                except Exception:
                    anio_gen = None
                resultado = generar_plantilla_seguimiento(post_codigo, anio=anio_gen)
                if resultado.get("ok"):
                    total_archivos = sum(
                        1 for item in list(resultado.get("files", []))
                        if bool(item.get("exists"))
                    )
                    faltantes_archivos = [
                        str(item.get("label", "archivo")).strip()
                        for item in list(resultado.get("files", []))
                        if not bool(item.get("exists"))
                    ]
                    messages.success(
                        request,
                        (
                            "Plantillas generadas correctamente: "
                            f"{resultado.get('file_name', '')} "
                            f"({resultado.get('total_participantes', 0)} participantes, "
                            f"{total_archivos} archivo(s))."
                        ),
                    )
                    if faltantes_archivos:
                        messages.warning(
                            request,
                            (
                                "No se pudo generar todas las salidas del flujo: "
                                f"{', '.join(faltantes_archivos)}."
                            ),
                        )
                else:
                    messages.error(
                        request,
                        f"No se pudo generar la plantilla. {resultado.get('error', 'Revisa configuración y datos base.')}",
                    )

        elif action == "add_retiro_manual":
            texto_dni = str(request.POST.get("dni_bulk", "")).strip()
            dnis_raw = [
                item.strip()
                for bloque in texto_dni.splitlines()
                for item in bloque.split(",")
            ]
            ok, nuevos = agregar_retiros_manual(post_codigo, dnis_raw)
            if ok:
                messages.success(request, f"Retiros manuales actualizados. Nuevos DNIs agregados: {nuevos}.")
            else:
                messages.error(request, "No se pudo guardar los retiros manuales.")

        elif action == "delete_retiro_manual":
            dni = str(request.POST.get("dni", "")).strip()
            if eliminar_retiro_manual(post_codigo, dni):
                messages.success(request, f"DNI {dni} retirado del listado manual.")
            else:
                messages.error(request, "No se pudo eliminar el DNI del listado manual.")

        elif action == "clear_retiros_manual":
            if limpiar_retiros_manual(post_codigo):
                messages.success(request, "Listado manual de retiros limpiado.")
            else:
                messages.error(request, "No se pudo limpiar el listado manual de retiros.")

        # Regresa a la misma pantalla con filtros preservados.
        return redirect(_build_submenu_url(section_slug, submenu_slug, redirect_params))

    # Aplica adaptaciones de datos solo para submenús de Gestión de la Capacitación.
    # Nota: este bloque reutiliza el comportamiento base de filtros visto en Streamlit.
    if section_slug == "gestion-capacitacion":
        # Obtiene oferta base desde tabla legacy local.
        oferta_bruta = obtener_filas_oferta_formativa()

        # Filtra por rol efectivo y estado permitido.
        oferta_filtrada = filtrar_capacitaciones_para_usuario(
            filas=oferta_bruta,
            role_effective=str(user_context.get("role_effective", "")),
            display_name=str(user_context.get("display_name", "")),
            username=str(request.user.username),
            excluir_sincronicas=True,
        )

        # Aplica selector de anio como en los modulos legacy.
        anios, anio_sel, oferta_anio = aplicar_filtro_anio(oferta_filtrada, anio_param)

        # Expone datos filtrados al template de submenu.
        context["anios_disponibles"] = anios
        context["anio_seleccionado"] = anio_sel
        context["capacitaciones"] = oferta_anio

        # Adaptacion del submenu "Registrar nueva capacitacion" (alta inicial).
        if submenu_slug == "registrar-nueva-capacitacion":
            # Recupera borrador cuando hubo error de validacion.
            draft_key = "registro_capacitacion_form_draft"
            draft_payload = request.session.get(draft_key, {})

            # Valores base por defecto + sobreescritura con borrador.
            valores_form = _valor_por_defecto_registro_capacitacion()
            if isinstance(draft_payload, dict):
                for campo in iterar_campos_registro_capacitacion():
                    codigo = str(campo.get("codigo", "")).strip()
                    if codigo in draft_payload:
                        valores_form[codigo] = str(draft_payload.get(codigo, "")).strip()

            # Prepara secciones con valor inyectado para render dinamico simple.
            secciones_render: list[dict[str, Any]] = []
            campos_totales = 0
            campos_obligatorios = 0
            for seccion in REGISTRO_CAPACITACION_SECCIONES:
                campos_render: list[dict[str, Any]] = []
                for campo in seccion.get("campos", []):
                    campos_totales += 1
                    if bool(campo.get("obligatorio")):
                        campos_obligatorios += 1
                    campos_render.append(
                        _enriquecer_campo_registro(campo, valores_form)
                    )

                secciones_render.append(
                    {
                        **seccion,
                        "campos": campos_render,
                    }
                )

            # Enriquecimiento del primer bloque para el layout de solicitud inicial.
            catalogo_iged = obtener_catalogo_iged_por_region()
            regiones_iged = sorted(catalogo_iged.keys())
            for seccion in secciones_render:
                estado_bloque = _contar_estado_bloque_registro(
                    list(seccion.get("campos", []))
                )
                seccion.update(estado_bloque)
                if str(seccion.get("slug", "")) == "solicitud-inicial":
                    for campo in seccion.get("campos", []):
                        codigo = str(campo.get("codigo", "")).strip()
                        if codigo == "sol_region_iged":
                            campo["opciones"] = regiones_iged
                        elif codigo == "sol_iged_nombre":
                            region_sel = str(valores_form.get("sol_region_iged", "")).strip()
                            campo["opciones"] = [
                                str(item.get("nombre", "")).strip()
                                for item in catalogo_iged.get(region_sel, [])
                                if str(item.get("nombre", "")).strip()
                            ]

                    seccion["special_layout"] = "solicitud"
                    seccion["campos_left"] = [
                        campo
                        for campo in seccion.get("campos", [])
                        if str(campo.get("ui_zone", "main")) == "left"
                    ]
                    seccion["campos_right"] = [
                        campo
                        for campo in seccion.get("campos", [])
                        if str(campo.get("ui_zone", "main")) == "right"
                    ]

            timeline_registro = _construir_timeline_registro(secciones_render, valores_form)
            flujo_matriz = _construir_flujo_matriz_sustento(secciones_render, valores_form)
            flujo_diagnostico = _construir_flujo_diagnostico(secciones_render, valores_form)
            etapa_sustento = _construir_pasarela_sustento(flujo_matriz, flujo_diagnostico)
            flujo_expediente = _construir_flujo_expediente(secciones_render, valores_form)
            solicitud_inicial = next(
                (
                    item
                    for item in secciones_render
                    if str(item.get("slug", "")) == "solicitud-inicial"
                ),
                None,
            )
            flujo_unificado = _construir_flujo_unificado(
                secciones_render, valores_form, solicitud_inicial, etapa_sustento, flujo_expediente,
            )
            slugs_diagnostico = {
                "diagnostico-paso-1",
                "diagnostico-paso-2",
                "diagnostico-paso-3",
                "diagnostico-paso-4",
                "diagnostico-paso-5",
            }
            secciones_posteriores = [
                item
                for item in secciones_render
                if str(item.get("slug", "")) != "solicitud-inicial"
                and str(item.get("slug", "")) not in slugs_diagnostico
            ]
            origen_externo = str(valores_form.get("sol_origen_institucional", "")).strip() in {
                "IGED",
                "Unidad orgánica",
            }
            origen_actual = str(valores_form.get("sol_origen_institucional", "")).strip()

            context.update(
                {
                    "valores_form": valores_form,
                    "registro_form_sections": secciones_render,
                    "registro_form_sections_rest": secciones_posteriores,
                    "registro_form_sections_restantes": secciones_posteriores,
                    "registro_timeline": timeline_registro,
                    "registro_solicitud": solicitud_inicial,
                    "registro_matriz_flujo": flujo_matriz,
                    "registro_diagnostico_flujo": flujo_diagnostico,
                    "registro_sustento_etapa": etapa_sustento,
                    "registro_expediente_flujo": flujo_expediente,
                    "registro_flujo_unificado": flujo_unificado,
                    "registro_iged_catalogo": catalogo_iged,
                    "registro_iged_regiones": regiones_iged,
                    "registro_origen_actual": origen_actual,
                    "registro_origen_externo": origen_externo,
                    "registro_campos_total": campos_totales,
                    "registro_campos_obligatorios": campos_obligatorios,
                    "registro_oferta_catalogo": [
                        {
                            "codigo": str(f.get("codigo", "")).strip(),
                            "denominacion": str(f.get("denominacion_proceso_formativo", "")).strip(),
                            "tipo": str(f.get("tipo_proceso_formativo", "")).strip(),
                            "anio": str(f.get("anio", "")).strip(),
                        }
                        for f in oferta_anio
                        if str(f.get("codigo", "")).strip()
                    ],
                }
            )

        # Adaptacion del submenu "Editar capacitacion" (lista + edicion ORM).
        if submenu_slug == "editar-capacitacion":
            from core.models import Capacitacion

            username = str(request.user.username)
            role_eff = str(user_context.get("role_effective", ""))
            is_admin = _normalizar_texto(role_eff) in {
                "administrador", "admin", "superusuario",
            }

            # Obtiene lista de capacitaciones del usuario (o todas si admin).
            qs = Capacitacion.objects.all().order_by("-creado_en")
            if not is_admin:
                qs = qs.filter(creado_por=username)

            editar_lista = list(
                qs.values(
                    "id", "cap_nombre", "cap_codigo", "cap_id_curso", "cap_anio",
                    "cap_estado", "paso_actual", "creado_nombre", "creado_en",
                )[:200]
            )

            # Si se recibe ?id=X, carga la capacitacion para edicion.
            cap_id_param = str(request.GET.get("id", "")).strip()
            editar_cap = None
            editar_valores = {}
            if cap_id_param:
                try:
                    cap_obj = qs.get(pk=int(cap_id_param))
                    editar_cap = cap_obj
                    # Serializa campos escalares del modelo al dict de valores_form.
                    for field in cap_obj._meta.get_fields():
                        if not hasattr(field, "column"):
                            continue
                        val = getattr(cap_obj, field.name, None)
                        if val is None:
                            editar_valores[field.name] = ""
                        elif isinstance(val, datetime):
                            editar_valores[field.name] = val.strftime("%Y-%m-%d %H:%M")
                        elif isinstance(val, _date_type):
                            editar_valores[field.name] = val.strftime("%Y-%m-%d")
                        else:
                            editar_valores[field.name] = str(val)
                except (Capacitacion.DoesNotExist, ValueError):
                    pass

            # Si estamos en modo edicion, construir secciones para el formulario.
            editar_secciones_render = []
            editar_flujo_unificado = []
            if editar_cap:
                # Reutiliza misma logica de secciones que en registro.
                for seccion in REGISTRO_CAPACITACION_SECCIONES:
                    campos_render_ed = []
                    for campo in seccion.get("campos", []):
                        campos_render_ed.append(
                            _enriquecer_campo_registro(campo, editar_valores)
                        )
                    sec_copy = {**seccion, "campos": campos_render_ed}
                    estado_bloque = _contar_estado_bloque_registro(campos_render_ed)
                    sec_copy.update(estado_bloque)

                    if str(sec_copy.get("slug", "")) == "solicitud-inicial":
                        catalogo_iged_ed = obtener_catalogo_iged_por_region()
                        regiones_iged_ed = sorted(catalogo_iged_ed.keys())
                        for campo in sec_copy.get("campos", []):
                            codigo = str(campo.get("codigo", "")).strip()
                            if codigo == "sol_region_iged":
                                campo["opciones"] = regiones_iged_ed
                            elif codigo == "sol_iged_nombre":
                                region_sel = str(editar_valores.get("sol_region_iged", "")).strip()
                                campo["opciones"] = [
                                    str(item.get("nombre", "")).strip()
                                    for item in catalogo_iged_ed.get(region_sel, [])
                                    if str(item.get("nombre", "")).strip()
                                ]
                        sec_copy["special_layout"] = "solicitud"
                        sec_copy["campos_left"] = [
                            c for c in sec_copy.get("campos", [])
                            if str(c.get("ui_zone", "main")) == "left"
                        ]
                        sec_copy["campos_right"] = [
                            c for c in sec_copy.get("campos", [])
                            if str(c.get("ui_zone", "main")) == "right"
                        ]

                    editar_secciones_render.append(sec_copy)

                solicitud_ed = next(
                    (s for s in editar_secciones_render if str(s.get("slug", "")) == "solicitud-inicial"),
                    None,
                )
                flujo_matriz_ed = _construir_flujo_matriz_sustento(editar_secciones_render, editar_valores)
                flujo_diag_ed = _construir_flujo_diagnostico(editar_secciones_render, editar_valores)
                etapa_sustento_ed = _construir_pasarela_sustento(flujo_matriz_ed, flujo_diag_ed)
                flujo_exp_ed = _construir_flujo_expediente(editar_secciones_render, editar_valores)
                editar_flujo_unificado = _construir_flujo_unificado(
                    editar_secciones_render, editar_valores, solicitud_ed, etapa_sustento_ed, flujo_exp_ed,
                )

                context.update({
                    "editar_cap": {
                        "id": editar_cap.pk,
                        "cap_nombre": editar_cap.cap_nombre,
                        "cap_estado": editar_cap.cap_estado,
                        "paso_actual": editar_cap.paso_actual,
                    },
                    "editar_valores": editar_valores,
                    "editar_secciones_render": editar_secciones_render,
                    "editar_flujo_unificado": editar_flujo_unificado,
                    "editar_solicitud": solicitud_ed,
                    "editar_matriz_flujo": flujo_matriz_ed,
                    "editar_diagnostico_flujo": flujo_diag_ed,
                    "editar_sustento_etapa": etapa_sustento_ed,
                    "registro_iged_catalogo": catalogo_iged_ed if solicitud_ed else {},
                })

            context["editar_lista"] = editar_lista

        # Adaptacion del submenu "Seguimiento de capacitaciones" (plantillas.py).
        if submenu_slug == "seguimiento-capacitaciones":
            from core.models import Capacitacion as CapModel

            # ── Construye lista de capacitaciones desde ORM (cap_capacitaciones) ──
            seg_username = str(request.user.username)
            seg_role = str(user_context.get("role_effective", ""))
            seg_display = str(user_context.get("display_name", ""))
            seg_is_admin = _normalizar_texto(seg_role) not in {"usuario estandar"}

            seg_qs_base = CapModel.objects.filter(cap_codigo__gt="").order_by("cap_codigo")
            if not seg_is_admin:
                seg_qs_base = seg_qs_base.filter(creado_por__in=[seg_username, seg_display])

            # Calcula años disponibles desde ORM y sobreescribe contexto global.
            seg_anios = sorted(
                seg_qs_base.values_list("cap_anio", flat=True).distinct(),
                reverse=True,
            )
            seg_anios = [a for a in seg_anios if a]  # descarta vacíos

            # Resuelve año seleccionado (anio_param es string, seg_anios son ints).
            try:
                _anio_solicitado = int(anio_param) if anio_param else None
            except (ValueError, TypeError):
                _anio_solicitado = None
            if _anio_solicitado not in seg_anios:
                from datetime import datetime as _dt
                _anio_actual = _dt.now().year
                _anio_solicitado = _anio_actual if _anio_actual in seg_anios else (seg_anios[0] if seg_anios else None)
            seg_anio_sel = _anio_solicitado
            context["anios_disponibles"] = seg_anios
            context["anio_seleccionado"] = seg_anio_sel

            # Aplica filtro de año.
            seg_qs = seg_qs_base
            if seg_anio_sel:
                seg_qs = seg_qs.filter(cap_anio=seg_anio_sel)

            # Construye lista compatible con el formato que espera el template.
            seg_filas: list[dict[str, Any]] = []
            for cap in seg_qs:
                # Reconstruye codigo completo: "XXXX-YYY" o solo "XXXX" si no hay id_curso.
                codigo_completo = cap.cap_codigo
                if cap.cap_id_curso:
                    codigo_completo = f"{cap.cap_codigo}-{cap.cap_id_curso}"
                seg_filas.append({
                    "codigo": codigo_completo,
                    "cap_codigo": cap.cap_codigo,
                    "cap_id_curso": cap.cap_id_curso,
                    "anio": cap.cap_anio,
                    "condicion": cap.cap_estado,
                    "tipo_proceso_formativo": cap.cap_tipo,
                    "denominacion_proceso_formativo": cap.cap_nombre,
                    "especialista_cargo": cap.creado_nombre or cap.creado_por,
                    "cap_id": cap.pk,
                })

            # Define pestañas funcionales heredadas del módulo legacy.
            seguimiento_tabs = [
                {"slug": "alertas", "titulo": "Alertas"},
                {"slug": "estructura", "titulo": "Gestión de estructura"},
                {"slug": "formula", "titulo": "Fórmula de promedio"},
                {"slug": "retiros", "titulo": "Retiros"},
                {"slug": "plantilla", "titulo": "Generación de plantilla"},
                {"slug": "confiabilidad", "titulo": "Confiabilidad"},
                {"slug": "certificados", "titulo": "Certificados"},
                {"slug": "satisfaccion", "titulo": "Satisfacción"},
            ]
            tabs_validos = {item["slug"] for item in seguimiento_tabs}

            # Lee codigo seleccionado desde query y lo valida contra capacitaciones ORM.
            codigo_param = str(request.GET.get("codigo", "")).strip()
            codigos_visibles = [str(fila.get("codigo", "")).strip() for fila in seg_filas]
            codigo_sel = codigo_param if codigo_param in codigos_visibles else (codigos_visibles[0] if codigos_visibles else "")

            # Resuelve tab activo de trabajo.
            tab_param = str(request.GET.get("tab", "alertas")).strip().lower()
            tab_activo = tab_param if tab_param in tabs_validos else "alertas"

            # Descargas CSV bajo demanda para seguimiento (confiabilidad/certificados).
            download_kind = str(request.GET.get("download", "")).strip().lower()
            if download_kind and codigo_sel:
                if download_kind == "actividad_fuera_excel":
                    row_id_param = str(request.GET.get("id_estructura", "")).strip()
                    row_id = int(row_id_param) if row_id_param.isdigit() else 0
                    if row_id:
                        actividad_fuera_info = obtener_excel_actividad_fuera_info(codigo_sel, row_id)
                        if actividad_fuera_info.get("exists"):
                            with open(str(actividad_fuera_info.get("path", "")), "rb") as file_fuera:
                                content = file_fuera.read()
                            file_name = str(actividad_fuera_info.get("file_name", "")).strip() or "actividad_fuera.xlsx"
                            response = HttpResponse(
                                content,
                                content_type=(
                                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                                ),
                            )
                            response["Content-Disposition"] = f'attachment; filename="{file_name}"'
                            return response

                if download_kind == "kr20_csv":
                    analisis = analizar_confiabilidad_por_codigo(codigo_sel)
                    csv_data = exportar_confiabilidad_csv(analisis)
                    response = HttpResponse(csv_data, content_type="text/csv; charset=utf-8")
                    response["Content-Disposition"] = (
                        f'attachment; filename="confiabilidad_kr20_{codigo_sel}.csv"'
                    )
                    return response

                if download_kind == "certificados_csv":
                    cert_rows = obtener_certificados_detalle(codigo_sel)
                    csv_data = exportar_certificados_csv(cert_rows)
                    response = HttpResponse(csv_data, content_type="text/csv; charset=utf-8")
                    response["Content-Disposition"] = (
                        f'attachment; filename="certificados_detalle_{codigo_sel}.csv"'
                    )
                    return response

                if download_kind == "certificados_resumen_csv":
                    cert_rows = obtener_certificados_detalle(codigo_sel)
                    resumen_rows = resumir_certificados_por_region(cert_rows)
                    csv_data = exportar_resumen_certificados_csv(resumen_rows)
                    response = HttpResponse(csv_data, content_type="text/csv; charset=utf-8")
                    response["Content-Disposition"] = (
                        f'attachment; filename="certificados_resumen_region_{codigo_sel}.csv"'
                    )
                    return response

                if download_kind == "postulantes_excel":
                    post_info = obtener_postulantes_excel_info(codigo_sel)
                    if post_info.get("exists"):
                        with open(str(post_info.get("path", "")), "rb") as file_post:
                            content = file_post.read()
                        id_simple = extraer_id_capacitacion(codigo_sel)
                        response = HttpResponse(
                            content,
                            content_type=(
                                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                            ),
                        )
                        response["Content-Disposition"] = (
                            f'attachment; filename="postulantes_{id_simple}.xlsx"'
                        )
                        return response

                if download_kind in {"plantilla_generada", "plantilla_generada_nominal", "plantilla_generada_iged"}:
                    plantilla_info = obtener_plantilla_generada_info(codigo_sel)
                    target_kind = "main"
                    if download_kind == "plantilla_generada_nominal":
                        target_kind = "nominal"
                    if download_kind == "plantilla_generada_iged":
                        target_kind = "iged"

                    files = list(plantilla_info.get("files", []))
                    target_file = next(
                        (item for item in files if str(item.get("kind")) == target_kind and bool(item.get("exists"))),
                        None,
                    )
                    if target_file is None and target_kind == "main" and plantilla_info.get("exists"):
                        # Compatibilidad con metadata legacy de un solo archivo.
                        target_file = {
                            "path": plantilla_info.get("path", ""),
                            "file_name": plantilla_info.get("file_name", ""),
                            "exists": True,
                        }

                    if target_file and target_file.get("exists"):
                        with open(str(target_file.get("path", "")), "rb") as file_gen:
                            content = file_gen.read()
                        file_name = str(target_file.get("file_name", "")).strip() or "plantilla_generada.xlsx"
                        response = HttpResponse(
                            content,
                            content_type=(
                                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                            ),
                        )
                        response["Content-Disposition"] = f'attachment; filename="{file_name}"'
                        return response

            # Busca detalle de la capacitacion actualmente seleccionada.
            cap_sel = next(
                (fila for fila in seg_filas if str(fila.get("codigo", "")).strip() == codigo_sel),
                {},
            )
            tipo_proceso_sel = str(cap_sel.get("tipo_proceso_formativo", "")).strip().lower()
            es_sincronica = "sincron" in tipo_proceso_sel

            # Lee datasets principales de seguimiento desde tablas legacy.
            metricas = obtener_metricas_seguimiento(codigo_sel) if codigo_sel else {}
            alertas = construir_alertas_seguimiento(metricas) if codigo_sel else []
            alertas_plataforma = (
                obtener_alertas_actividades_plataforma(codigo_sel)
                if codigo_sel
                else {"curso_id": "", "pendientes": {}, "total": 0, "error": ""}
            )
            alertas_plataforma_rows: list[dict[str, Any]] = []
            for tipo in ["Ejercicio", "Tarea", "Encuesta"]:
                for item in alertas_plataforma.get("pendientes", {}).get(tipo, []):
                    alertas_plataforma_rows.append(
                        {
                            "tipo": tipo,
                            "id": item.get("id", ""),
                            "nombre": item.get("nombre", ""),
                        }
                    )
            estructura = obtener_estructura_por_codigo(codigo_sel) if codigo_sel else []
            formulas = obtener_formulas_promedio(codigo_sel) if codigo_sel else []
            # En la tabla de retiros solo se muestran DNIs registrados manualmente.
            retiros = obtener_participantes_retiro_manual_por_codigo(codigo_sel) if codigo_sel else []
            retiros_manual = leer_retiros_manual(codigo_sel) if codigo_sel else []
            satisfaccion = obtener_resumen_satisfaccion(codigo_sel) if codigo_sel else {}
            rutas_plantilla = obtener_rutas_plantilla(codigo_sel) if codigo_sel else {}
            postulantes_info = obtener_postulantes_excel_info(codigo_sel) if codigo_sel else {
                "path": "",
                "file_name": "",
                "exists": False,
                "size_bytes": 0,
            }
            actividades_plantilla = obtener_actividades_plantilla(codigo_sel, estructura) if codigo_sel else {
                "plataforma": [],
                "fuera": [],
            }
            nominal_config = obtener_config_nominal_reporte(codigo_sel, estructura) if codigo_sel else {
                "titulo_nominal": "Reporte Nominal",
                "columnas_disponibles": [],
                "columnas_seleccionadas": [],
                "grupos": [],
                "grupos_detalle": [],
                "nombres_grupo": {},
            }
            plantilla_generada = obtener_plantilla_generada_info(codigo_sel) if codigo_sel else {
                "exists": False,
                "path": "",
                "file_name": "",
                "size_bytes": 0,
                "generated_at": "",
            }
            estado_plantilla = _construir_estado_plantilla(
                postulantes_info,
                actividades_plantilla,
                nominal_config,
            )
            plantilla_ok_map = {
                str(item.get("slug")): bool(item.get("ok"))
                for item in estado_plantilla.get("checkpoints", [])
            }
            confiabilidad = analizar_confiabilidad_por_codigo(codigo_sel) if codigo_sel else {"ok": False, "items": []}
            if confiabilidad.get("ok"):
                confiabilidad["interpretacion"] = interpretar_kr20(float(confiabilidad.get("kr20", 0)))
            else:
                confiabilidad["interpretacion"] = "Sin datos"
            certificados_detalle = obtener_certificados_detalle(codigo_sel) if codigo_sel else []
            certificados_resumen_region = resumir_certificados_por_region(certificados_detalle)
            total_certificados = len(certificados_detalle)
            promedio_nota_cert = round(
                (
                    sum(float(row.get("promedio_final_general") or 0) for row in certificados_detalle)
                    / total_certificados
                ),
                2,
            ) if total_certificados > 0 else 0.0

            # Variables sugeridas para formula (actividades registradas en estructura).
            # Se conserva orden de registro y se evita duplicar nombres en el selector.
            formula_variables: list[dict[str, Any]] = []
            variables_vistas: set[tuple[str, str]] = set()
            for row in estructura:
                actividad = str(row.get("actividad", "")).strip()
                aplica_a_var = str(row.get("aplica_a", "Ambos")).strip() or "Ambos"
                if not actividad:
                    continue
                key = (actividad, aplica_a_var)
                if key in variables_vistas:
                    continue
                variables_vistas.add(key)
                formula_variables.append(
                    {
                        "actividad": actividad,
                        "aplica_a": aplica_a_var,
                    }
                )

            # Resuelve fila en edicion para formulario de estructura.
            draft_key = "seguimiento_estructura_form_draft"
            draft_payload = request.session.get(draft_key, {})
            edit_param = str(request.GET.get("edit", "")).strip()
            edit_id = int(edit_param) if edit_param.isdigit() else 0
            if not edit_id:
                draft_edit = draft_payload.get("edit_id")
                try:
                    draft_edit_int = int(draft_edit or 0)
                except Exception:
                    draft_edit_int = 0
                if (
                    draft_edit_int > 0
                    and str(draft_payload.get("codigo", "")).strip() == codigo_sel
                ):
                    edit_id = draft_edit_int
            estructura_edit = next(
                (fila for fila in estructura if int(fila.get("id_estructura") or 0) == edit_id),
                {},
            )

            # Construye formulario base (edicion o alta) y aplica borrador si existe.
            if estructura_edit:
                estructura_form = {
                    "actividad": str(estructura_edit.get("actividad", "") or "").strip(),
                    "codigo_actividad": str(estructura_edit.get("codigo_actividad", "") or "").strip(),
                    "tipo": str(estructura_edit.get("tipo", "Ejercicio") or "Ejercicio").strip(),
                    "cumplimiento_nota": str(estructura_edit.get("cumplimiento_nota", "Cumplimiento") or "Cumplimiento").strip(),
                    "origen": str(estructura_edit.get("origen", "Plataforma") or "Plataforma").strip(),
                    "escala": str(estructura_edit.get("escala", "Vigesimal") or "Vigesimal").strip(),
                    "aplica_a": str(estructura_edit.get("aplica_a", "Ambos") or "Ambos").strip(),
                    "inicio": _format_datetime_local(estructura_edit.get("inicio")),
                    "fin": _format_datetime_local(estructura_edit.get("fin")),
                    "observaciones": str(estructura_edit.get("observaciones", "") or "").strip(),
                    "obligatoria": bool(estructura_edit.get("obligatoria")),
                    "enlace": bool(estructura_edit.get("enlace")),
                }
            else:
                estructura_form = {
                    "actividad": "",
                    "codigo_actividad": "",
                    "tipo": "Ejercicio",
                    "cumplimiento_nota": "Cumplimiento",
                    "origen": "Plataforma",
                    "escala": "Vigesimal",
                    "aplica_a": "Ambos",
                    "inicio": "",
                    "fin": "",
                    "observaciones": "",
                    "obligatoria": False,
                    "enlace": False,
                }

            draft_data = {}
            if (
                isinstance(draft_payload, dict)
                and str(draft_payload.get("codigo", "")).strip() == codigo_sel
            ):
                draft_data = draft_payload.get("data", {}) or {}

            if isinstance(draft_data, dict) and draft_data:
                for key in [
                    "actividad",
                    "codigo_actividad",
                    "tipo",
                    "cumplimiento_nota",
                    "origen",
                    "escala",
                    "aplica_a",
                    "inicio",
                    "fin",
                    "observaciones",
                ]:
                    if key in draft_data:
                        estructura_form[key] = str(draft_data.get(key, "") or "").strip()
                for key in ["obligatoria", "enlace"]:
                    if key in draft_data:
                        estructura_form[key] = bool(draft_data.get(key))

            # Controla autoapertura del modal para edicion o tras validacion fallida.
            modal_param = str(request.GET.get("modal", "")).strip()
            modal_open = bool(estructura_edit) or (modal_param == "1")

            # Construye links de cada card de capacitacion para navegacion rapida.
            cards: list[dict[str, Any]] = []
            actividades_por_codigo: dict[str, int] = {}
            for fila in seg_filas:
                codigo_card = str(fila.get("codigo", "")).strip()
                if codigo_card not in actividades_por_codigo:
                    actividades_por_codigo[codigo_card] = len(obtener_estructura_por_codigo(codigo_card))
                card_params = {"anio": anio_sel, "codigo": codigo_card, "tab": tab_activo}
                cards.append(
                    {
                        **fila,
                        "url": _build_submenu_url(section_slug, submenu_slug, card_params),
                        "is_active": codigo_card == codigo_sel,
                        "id_simple": extraer_id_capacitacion(codigo_card),
                        "total_actividades": actividades_por_codigo.get(codigo_card, 0),
                    }
                )
            cards_con_estructura = [
                card for card in cards if int(card.get("total_actividades", 0)) > 0
            ]

            # Expone todo el contexto de seguimiento portado a la plantilla.
            context.update(
                {
                    "seguimiento_tabs": seguimiento_tabs,
                    "seguimiento_tab_activo": tab_activo,
                    "seguimiento_codigo_sel": codigo_sel,
                    "seguimiento_cap_sel": cap_sel,
                    "seguimiento_es_sincronica": es_sincronica,
                    "seguimiento_metricas": metricas,
                    "seguimiento_alertas": alertas,
                    "seguimiento_alertas_plataforma": alertas_plataforma,
                    "seguimiento_alertas_plataforma_rows": alertas_plataforma_rows,
                    "seguimiento_estructura": estructura,
                    "seguimiento_edit": estructura_edit,
                    "seguimiento_estructura_form": estructura_form,
                    "seguimiento_modal_open": modal_open,
                    "seguimiento_formulas": formulas,
                    "seguimiento_retiros": retiros,
                    "seguimiento_retiros_manual": retiros_manual,
                    "seguimiento_satisfaccion": satisfaccion,
                    "seguimiento_plantilla": rutas_plantilla,
                    "seguimiento_postulantes": postulantes_info,
                    "seguimiento_actividades_plantilla_plataforma": actividades_plantilla.get("plataforma", []),
                    "seguimiento_actividades_plantilla_fuera": actividades_plantilla.get("fuera", []),
                    "seguimiento_nominal_config": nominal_config,
                    "seguimiento_plantilla_generada": plantilla_generada,
                    "seguimiento_plantilla_estado": estado_plantilla,
                    "seguimiento_plantilla_ok_map": plantilla_ok_map,
                    "seguimiento_confiabilidad": confiabilidad,
                    "seguimiento_certificados": certificados_detalle,
                    "seguimiento_certificados_region": certificados_resumen_region,
                    "seguimiento_certificados_total": total_certificados,
                    "seguimiento_certificados_promedio_nota": promedio_nota_cert,
                    "seguimiento_cards": cards,
                    "seguimiento_cards_con_estructura": cards_con_estructura,
                    "seguimiento_total_estructura": len(estructura),
                    "seguimiento_formula_variables": formula_variables,
                }
            )

        # Adaptacion del submenu "Estandares de calidad" (estandares_calidad.py).
        if submenu_slug == "estandares-calidad":
            # Recopila codigos visibles en el anio filtrado.
            codigos_visibles = [
                str(fila.get("codigo", "")).strip()
                for fila in oferta_anio
                if str(fila.get("codigo", "")).strip()
            ]
            # Consulta tabla estandares_calidad y calcula completitud por codigo.
            resumen_estandares = obtener_resumen_estandares(codigos_visibles)
            filas_estandares = construir_resumen_estandares_por_capacitacion(
                capacitaciones=oferta_anio,
                resumen_estandares=resumen_estandares,
                total_capitulos=4,  # Capitulo A/B/C/D del esquema vigente.
            )
            context["filas_estandares"] = filas_estandares
            context["total_con_estandares"] = sum(
                1 for fila in filas_estandares if int(fila.get("respuestas_totales", 0)) > 0
            )
            context["total_sin_estandares"] = sum(
                1 for fila in filas_estandares if int(fila.get("respuestas_totales", 0)) == 0
            )

    if section_slug == "reporte-indicadores" and submenu_slug == "dashboard-kpi":
        download_kind = str(request.GET.get("download", "")).strip().lower()
        download_format = str(request.GET.get("format", "xlsx")).strip().lower()
        if download_kind:
            download_payload = build_indicadores_download(request.GET, download_kind, download_format)
            if download_payload is not None:
                response = HttpResponse(
                    download_payload.get("content", b""),
                    content_type=str(download_payload.get("content_type", "application/octet-stream")),
                )
                response["Content-Disposition"] = f'attachment; filename="{download_payload.get("filename", "indicadores_export")}"'
                return response

        indicadores_context = build_indicadores_dashboard_context(request.GET)
        base_url = f"/app/seccion/{section_slug}/submenu/{submenu_slug}/"
        tabs_with_urls: list[dict[str, Any]] = []
        for tab in indicadores_context.get("indicadores_tabs", []):
            params = request.GET.copy()
            params["vista"] = str(tab.get("slug", ""))
            url = f"{base_url}?{params.urlencode()}" if params else base_url
            tabs_with_urls.append({**tab, "url": url})

        indicadores_context["indicadores_tabs"] = tabs_with_urls
        indicadores_context["indicadores_download_base_url"] = f"{base_url}?{request.GET.urlencode()}" if request.GET else base_url
        context["mostrar_filtro_anio"] = False
        context.update(indicadores_context)

    # Renderiza vista de submenu con adaptacion correspondiente.
    return render(request, "core/submenu_detail.html", context)


@login_required
@require_POST
def switch_role_view(request):
    """Cambia rol efectivo de la sesion dentro de los limites del rol base."""
    # Resuelve estado actual de roles para validar cambios.
    _, _, allowed_roles = _resolve_roles(request)

    # Lee rol solicitado desde formulario.
    requested_role = str(request.POST.get("role", "")).strip()

    # Aplica cambio solo si el rol solicitado esta permitido.
    if requested_role in allowed_roles:
        request.session["difoca_role_effective"] = requested_role
        request.session["difoca_role"] = requested_role

    # Lee URL destino para volver a la pantalla anterior.
    next_url = request.POST.get("next", "")
    # Valida que la URL sea segura y pertenezca al host actual.
    if next_url and url_has_allowed_host_and_scheme(
        url=next_url,
        allowed_hosts={request.get_host()},
        require_https=request.is_secure(),
    ):
        return redirect(next_url)

    # Fallback al inicio si next no es valido.
    return redirect("core:home")
