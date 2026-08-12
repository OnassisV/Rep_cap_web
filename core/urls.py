"""Definicion de rutas para la app core."""

# Utilidad Django para declarar rutas.
from django.urls import path

# Vistas protegidas de core.
from .views import (
    cert_descargar_lista_excel_view,
    home_view,
    section_detail_view,
    submenu_detail_view,
    switch_role_view,
    api_caracterizacion_replica_view,
    api_recalcular_estado_view,
    cargar_satisfaccion_view,
    cargar_satisfaccion_aula_virtual_view,
    portal_reportes_view,
    portal_reporte_descargar_view,
    portal_casos_view,
    caso_detalle_view,
    casuistica_evidencia_ver_view,
    casuisticas_exportar_excel_view,
    casuisticas_accion_crear_view,
)


# Namespace para resolver URLs como `core:home`.
app_name = "core"

# Patrones de rutas locales de esta app.
urlpatterns = [
    path("", home_view, name="home"),  # Pagina de inicio protegida.
    path(
        "seccion/<slug:section_slug>/",
        section_detail_view,
        name="section_detail",
    ),  # Vista detalle por bloque del GeoMenu.
    path(
        "seccion/<slug:section_slug>/submenu/<slug:submenu_slug>/",
        submenu_detail_view,
        name="submenu_detail",
    ),  # Vista detalle para submenus internos de una seccion.
    path(
        "rol/cambiar/",
        switch_role_view,
        name="switch_role",
    ),  # Endpoint para cambiar modo de rol en sesion.
    path(
        "certificados/lista-excel/<str:codigo>/",
        cert_descargar_lista_excel_view,
        name="cert_descargar_lista_excel",
    ),  # Descarga Excel con lista nominal de participantes certificados.
    path(
        "api/caracterizacion-replica/<int:cap_id>/",
        api_caracterizacion_replica_view,
        name="api_caracterizacion_replica",
    ),  # Devuelve los campos de caracterización de una capacitación fuente.
    path(
        "api/recalcular-estado/<int:cap_id>/",
        api_recalcular_estado_view,
        name="api_recalcular_estado",
    ),  # Fuerza re-evaluación del estado (útil cuando certificados se agregan externamente).
    path(
        "cargar-satisfaccion/",
        cargar_satisfaccion_view,
        name="cargar_satisfaccion",
    ),  # Carga datos de satisfacción desde Excel.
    path(
        "cargar-satisfaccion-aula-virtual/",
        cargar_satisfaccion_aula_virtual_view,
        name="cargar_satisfaccion_aula_virtual",
    ),  # Carga datos de satisfacción desde Aula Virtual (Chamilo).
    path(
        "portal-reportes/",
        portal_reportes_view,
        name="portal_reportes",
    ),  # Portal externo (rol Visor): cursos autorizados y sus reportes.
    path(
        "portal-reportes/descargar/<int:cap_id>/",
        portal_reporte_descargar_view,
        name="portal_reporte_descargar",
    ),  # Descarga del reporte de cumplimiento de un curso autorizado.
    path(
        "portal-reportes/<int:cap_id>/casos/",
        portal_casos_view,
        name="portal_casos",
    ),  # Portal externo (rol Visor): casuisticas reportadas sobre un curso.
    path(
        "casos/<int:caso_id>/",
        caso_detalle_view,
        name="caso_detalle",
    ),  # Hilo de conversacion de una casuistica (Visor o personal interno).
    path(
        "casos/evidencia/<int:evidencia_id>/",
        casuistica_evidencia_ver_view,
        name="caso_evidencia",
    ),  # Sirve una evidencia (foto/PDF) proxied desde Google Drive.
    path(
        "casos/exportar-excel/",
        casuisticas_exportar_excel_view,
        name="casuisticas_exportar_excel",
    ),  # Excel "para Plataforma" de casos En plataforma. Solo Administrador.
    path(
        "casos/acciones/crear/",
        casuisticas_accion_crear_view,
        name="casuisticas_accion_crear",
    ),  # AJAX: agrega una accion nueva al catalogo del desplegable.
]
