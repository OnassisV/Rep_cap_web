"""Modelos ORM para la gestion de capacitaciones.

Reemplaza el almacenamiento JSON-blob anterior por una estructura relacional
normalizada que permite queries directas, integridad referencial y extensiones
sin romper datos existentes.
"""

from django.db import models


# ---------------------------------------------------------------------------
# Tabla principal: cada fila es una capacitacion
# ---------------------------------------------------------------------------
class Capacitacion(models.Model):
    """Registro maestro de una capacitacion."""

    class Estado(models.TextChoices):
        BORRADOR = "Borrador"
        EN_PROCESO = "En proceso"
        FINALIZADA = "Finalizada"
        CANCELADA = "Cancelada"

    # -- Datos de solicitud (Paso 1) -----------------------------------------
    sol_origen_institucional = models.CharField(max_length=60, blank=True, default="")
    sol_numero_oficio = models.CharField(max_length=120, blank=True, default="")
    sol_fecha_oficio = models.DateField(null=True, blank=True)
    sol_archivo_oficio = models.CharField(max_length=500, blank=True, default="")
    sol_region_iged = models.CharField(max_length=120, blank=True, default="")
    sol_iged_nombre = models.CharField(max_length=250, blank=True, default="")
    sol_es_replica = models.CharField(max_length=10, blank=True, default="")
    sol_tiene_matriz = models.CharField(max_length=10, blank=True, default="")
    sol_tiene_diagnostico = models.CharField(max_length=10, blank=True, default="")
    sol_responde_desempeno = models.CharField(max_length=10, blank=True, default="")

    # -- Identificacion general ----------------------------------------------
    cap_nombre = models.CharField(max_length=255)
    cap_codigo = models.CharField(max_length=120, blank=True, default="")
    cap_id_curso = models.CharField(max_length=120, blank=True, default="")
    cap_tipo = models.CharField(max_length=100, blank=True, default="")
    cap_estrategia = models.CharField(max_length=255, blank=True, default="")
    cap_prioridad = models.CharField(max_length=30, blank=True, default="")
    cap_anio = models.IntegerField()
    cap_direccion = models.CharField(max_length=120, blank=True, default="")
    pob_tipo = models.CharField(max_length=120, blank=True, default="")
    pob_ambito = models.CharField(max_length=120, blank=True, default="")

    # -- Estado y control del flujo ------------------------------------------
    cap_estado = models.CharField(
        max_length=30,
        choices=Estado.choices,
        default=Estado.BORRADOR,
    )
    paso_actual = models.PositiveSmallIntegerField(default=1)

    # -- Diagnostico (Paso 2 – modales) -------------------------------------
    diag_base_normativa = models.TextField(blank=True, default="")
    diag_servicio_territorial = models.TextField(blank=True, default="")
    diag_dre_modelo = models.CharField(max_length=10, blank=True, default="")
    diag_instr_instrucciones = models.TextField(blank=True, default="")
    diag_instr_presentacion = models.CharField(max_length=255, blank=True, default="")
    diag_instr_periodo = models.CharField(max_length=120, blank=True, default="")
    diag_instr_tiempo = models.CharField(max_length=120, blank=True, default="")
    diag_instr_confidencialidad = models.CharField(max_length=255, blank=True, default="")
    diag_instr_misma_escala = models.CharField(max_length=10, blank=True, default="")
    diag_instr_puntos_escala = models.IntegerField(null=True, blank=True)
    diag_instr_escala_1 = models.CharField(max_length=120, blank=True, default="")
    diag_instr_escala_2 = models.CharField(max_length=120, blank=True, default="")
    diag_instr_escala_3 = models.CharField(max_length=120, blank=True, default="")
    diag_instr_preguntas_extra = models.TextField(blank=True, default="")
    diag_instr_perfil_final = models.CharField(max_length=255, blank=True, default="")
    diag_instr_previsualizacion = models.TextField(blank=True, default="")
    diag_instr_link_kr20 = models.TextField(blank=True, default="")
    diag_result_proc_dimension = models.TextField(blank=True, default="")
    diag_result_proc_subdimension = models.TextField(blank=True, default="")
    diag_result_analisis = models.TextField(blank=True, default="")
    diag_result_informe = models.TextField(blank=True, default="")
    diag_evidencia = models.TextField(blank=True, default="")
    diag_linea_base_existe = models.CharField(max_length=10, blank=True, default="")
    diag_linea_base_valor = models.DecimalField(
        max_digits=12, decimal_places=4, null=True, blank=True,
    )
    diag_normativa = models.TextField(blank=True, default="")
    diag_alineamiento = models.TextField(blank=True, default="")
    diag_just_tecnica = models.TextField(blank=True, default="")
    diag_just_operativa = models.TextField(blank=True, default="")
    diag_just_normativa = models.TextField(blank=True, default="")

    # -- Diseno instruccional (Paso 3) ---------------------------------------
    mi_objetivo_capacitacion = models.TextField(blank=True, default="")
    mi_criterios_formula = models.TextField(blank=True, default="")
    mi_ind3_obj_consistente = models.CharField(max_length=10, blank=True, default="")
    mi_ec4_desempenos_consistentes = models.CharField(max_length=10, blank=True, default="")

    # -- Plan de trabajo (Paso 4) --------------------------------------------
    pt_fecha_desarrollo = models.DateField(null=True, blank=True)
    pt_horas = models.IntegerField(null=True, blank=True)
    pt_modalidad = models.CharField(max_length=80, blank=True, default="")
    pt_tipo_convocatoria = models.CharField(max_length=60, blank=True, default="")
    pt_acciones_seguimiento = models.TextField(blank=True, default="")
    pt_justificacion = models.TextField(blank=True, default="")
    pt_base_legal = models.TextField(blank=True, default="")
    pt_obj_alcances = models.TextField(blank=True, default="")
    pt_convocatoria = models.TextField(blank=True, default="")
    pt_inscripcion = models.TextField(blank=True, default="")
    pt_desarrollo = models.TextField(blank=True, default="")
    pt_requisitos = models.TextField(blank=True, default="")
    pt_calculo_calificacion = models.TextField(blank=True, default="")
    pt_actores_responsables = models.TextField(blank=True, default="")

    # -- Generacion de recursos (Paso 5) ------------------------------------
    gr_guia_link = models.CharField(max_length=500, blank=True, default="")
    gr_guia_generada = models.CharField(max_length=10, blank=True, default="")
    gr_cuestionario_link = models.CharField(max_length=500, blank=True, default="")
    gr_cuestionario_validado = models.CharField(max_length=10, blank=True, default="")
    gr_cronograma_link = models.CharField(max_length=500, blank=True, default="")
    gr_fecha_prematricula = models.DateField(null=True, blank=True)
    gr_linea_grafica = models.CharField(max_length=10, blank=True, default="")
    gr_plataforma_habilitada = models.CharField(max_length=10, blank=True, default="")
    gr_recursos_cargados = models.CharField(max_length=10, blank=True, default="")
    gr_solicitudes_atendidas = models.CharField(max_length=10, blank=True, default="")
    gr_ind6_recursos = models.CharField(max_length=10, blank=True, default="")
    gr_ec15_actividades = models.CharField(max_length=10, blank=True, default="")
    gr_ec13_tutores_docs = models.CharField(max_length=10, blank=True, default="")
    gr_ec14_tutores_espacios = models.CharField(max_length=10, blank=True, default="")

    # -- Implementacion y seguimiento (Paso 6) -------------------------------
    is_conv_nro_oficio = models.CharField(max_length=120, blank=True, default="")
    is_conv_fecha_oficio = models.DateField(null=True, blank=True)
    is_conf_nro_oficio = models.CharField(max_length=120, blank=True, default="")
    is_conf_fecha_oficio = models.DateField(null=True, blank=True)
    is_conf_fecha_compromiso = models.DateField(null=True, blank=True)
    is_seg_alertas_cumplimiento = models.CharField(max_length=10, blank=True, default="")
    is_seg_alertas_progreso = models.CharField(max_length=10, blank=True, default="")

    # -- Evaluacion y cierre (Paso 7) ----------------------------------------
    ed_nro_oficio = models.CharField(max_length=120, blank=True, default="")
    ed_fecha_oficio = models.DateField(null=True, blank=True)
    ed_cierre_confirmado = models.CharField(max_length=10, blank=True, default="")

    # -- Metadata ------------------------------------------------------------
    creado_por = models.CharField(max_length=150)
    creado_nombre = models.CharField(max_length=200, blank=True, default="")
    creado_en = models.DateTimeField(auto_now_add=True)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "cap_capacitaciones"
        ordering = ["-creado_en"]
        indexes = [
            models.Index(fields=["cap_anio"], name="idx_cap_anio"),
            models.Index(fields=["cap_estado"], name="idx_cap_estado"),
            models.Index(fields=["cap_codigo"], name="idx_cap_codigo"),
            models.Index(fields=["creado_por"], name="idx_cap_creador"),
        ]

    def __str__(self) -> str:
        return f"{self.cap_nombre} ({self.cap_anio})"


# ---------------------------------------------------------------------------
# Diagnostico: problemas priorizados (antes diag_problemas_json)
# ---------------------------------------------------------------------------
class CapProblema(models.Model):
    """Problema priorizado vinculado al diagnostico de una capacitacion."""

    capacitacion = models.ForeignKey(
        Capacitacion, on_delete=models.CASCADE, related_name="problemas",
    )
    orden = models.PositiveSmallIntegerField(default=0)
    problema = models.TextField(blank=True, default="")
    evidencia = models.TextField(blank=True, default="")
    fuente = models.CharField(max_length=255, blank=True, default="")
    enlace_fuente = models.URLField(max_length=500, blank=True, default="")

    class Meta:
        db_table = "cap_problemas"
        ordering = ["capacitacion", "orden"]

    def __str__(self) -> str:
        return f"Problema #{self.orden} – {self.capacitacion_id}"


# ---------------------------------------------------------------------------
# Diagnostico: dimensiones → subdimensiones → indicadores (antes diag_matriz_json)
# ---------------------------------------------------------------------------
class CapDimension(models.Model):
    """Dimension de la matriz de evaluacion del diagnostico."""

    capacitacion = models.ForeignKey(
        Capacitacion, on_delete=models.CASCADE, related_name="dimensiones",
    )
    orden = models.PositiveSmallIntegerField(default=0)
    nombre = models.CharField(max_length=300)

    class Meta:
        db_table = "cap_dimensiones"
        ordering = ["capacitacion", "orden"]

    def __str__(self) -> str:
        return self.nombre


class CapSubdimension(models.Model):
    """Subdimension vinculada a una dimension."""

    dimension = models.ForeignKey(
        CapDimension, on_delete=models.CASCADE, related_name="subdimensiones",
    )
    orden = models.PositiveSmallIntegerField(default=0)
    nombre = models.TextField(blank=True, default="")

    class Meta:
        db_table = "cap_subdimensiones"
        ordering = ["dimension", "orden"]

    def __str__(self) -> str:
        return self.nombre[:80]


class CapIndicador(models.Model):
    """Indicador vinculado a una subdimension."""

    subdimension = models.ForeignKey(
        CapSubdimension, on_delete=models.CASCADE, related_name="indicadores",
    )
    orden = models.PositiveSmallIntegerField(default=0)
    descripcion = models.TextField(blank=True, default="")

    class Meta:
        db_table = "cap_indicadores"
        ordering = ["subdimension", "orden"]

    def __str__(self) -> str:
        return self.descripcion[:80]


# ---------------------------------------------------------------------------
# Diagnostico: instrumento de evaluacion (antes diag_instrumento_json)
# ---------------------------------------------------------------------------
class CapInstrumentoItem(models.Model):
    """Item del instrumento de evaluacion del diagnostico."""

    capacitacion = models.ForeignKey(
        Capacitacion, on_delete=models.CASCADE, related_name="instrumento_items",
    )
    orden = models.PositiveSmallIntegerField(default=0)
    perfil = models.CharField(max_length=255, blank=True, default="")
    dimension = models.CharField(max_length=300, blank=True, default="")
    subdimension = models.CharField(max_length=300, blank=True, default="")
    indicador_desempenio = models.TextField(blank=True, default="")
    item = models.TextField(blank=True, default="")
    preguntas_extra = models.TextField(blank=True, default="")

    class Meta:
        db_table = "cap_instrumento_items"
        ordering = ["capacitacion", "orden"]

    def __str__(self) -> str:
        return f"Item #{self.orden} – {self.capacitacion_id}"


# ---------------------------------------------------------------------------
# Diagnostico: generacion operativa (antes diag_generacion_json)
# ---------------------------------------------------------------------------
class CapGeneracionOperativa(models.Model):
    """Salida operativa para la generacion del instrumento."""

    capacitacion = models.ForeignKey(
        Capacitacion, on_delete=models.CASCADE, related_name="generaciones",
    )
    orden = models.PositiveSmallIntegerField(default=0)
    perfil = models.CharField(max_length=255, blank=True, default="")
    canal = models.CharField(max_length=255, blank=True, default="")
    fecha = models.CharField(max_length=120, blank=True, default="")
    observaciones = models.TextField(blank=True, default="")

    class Meta:
        db_table = "cap_generacion_operativa"
        ordering = ["capacitacion", "orden"]


# ---------------------------------------------------------------------------
# Diagnostico: resultados esperados (antes diag_resultados_json)
# ---------------------------------------------------------------------------
class CapResultadoEsperado(models.Model):
    """Proyeccion de resultados vinculada a un problema."""

    capacitacion = models.ForeignKey(
        Capacitacion, on_delete=models.CASCADE, related_name="resultados_esperados",
    )
    orden = models.PositiveSmallIntegerField(default=0)
    problema = models.TextField(blank=True, default="")
    cambio_desempenio = models.TextField(blank=True, default="")
    cambio_proceso = models.TextField(blank=True, default="")
    cambios_colaterales = models.TextField(blank=True, default="")

    class Meta:
        db_table = "cap_resultados_esperados"
        ordering = ["capacitacion", "orden"]


# ---------------------------------------------------------------------------
# Matriz de sustento: indicadores de gestion (desde modal matriz)
# ---------------------------------------------------------------------------
class CapMatrizIndicador(models.Model):
    """Indicador de gestion y competencias de la matriz de sustento."""

    capacitacion = models.ForeignKey(
        Capacitacion, on_delete=models.CASCADE, related_name="matriz_indicadores",
    )
    orden = models.PositiveSmallIntegerField(default=0)
    problema = models.TextField(blank=True, default="")
    indicador = models.TextField(blank=True, default="")
    competencia_cognitiva = models.TextField(blank=True, default="")
    competencia_actitudinal = models.TextField(blank=True, default="")
    desempenio = models.TextField(blank=True, default="")

    class Meta:
        db_table = "cap_matriz_indicadores"
        ordering = ["capacitacion", "orden"]


# ---------------------------------------------------------------------------
# Diseno instruccional: competencias, desempenos, malla (JSON → tablas)
# ---------------------------------------------------------------------------
class CapCompetencia(models.Model):
    """Competencia a fortalecer/desarrollar en la capacitacion."""

    capacitacion = models.ForeignKey(
        Capacitacion, on_delete=models.CASCADE, related_name="competencias",
    )
    orden = models.PositiveSmallIntegerField(default=0)
    descripcion = models.TextField()

    class Meta:
        db_table = "cap_competencias"
        ordering = ["capacitacion", "orden"]


class CapDesempenio(models.Model):
    """Desempeno esperado vinculado a la capacitacion."""

    capacitacion = models.ForeignKey(
        Capacitacion, on_delete=models.CASCADE, related_name="desempenios",
    )
    orden = models.PositiveSmallIntegerField(default=0)
    descripcion = models.TextField()

    class Meta:
        db_table = "cap_desempenios"
        ordering = ["capacitacion", "orden"]


class CapMallaCurricular(models.Model):
    """Entrada de la malla curricular."""

    capacitacion = models.ForeignKey(
        Capacitacion, on_delete=models.CASCADE, related_name="malla_curricular",
    )
    orden = models.PositiveSmallIntegerField(default=0)
    modulo = models.CharField(max_length=255, blank=True, default="")
    tema = models.CharField(max_length=255, blank=True, default="")
    horas = models.IntegerField(null=True, blank=True)
    descripcion = models.TextField(blank=True, default="")

    class Meta:
        db_table = "cap_malla_curricular"
        ordering = ["capacitacion", "orden"]


class CapFormulaEvaluacion(models.Model):
    """Componente de la formula de evaluacion."""

    capacitacion = models.ForeignKey(
        Capacitacion, on_delete=models.CASCADE, related_name="formula_evaluacion",
    )
    orden = models.PositiveSmallIntegerField(default=0)
    componente = models.CharField(max_length=255, blank=True, default="")
    peso = models.DecimalField(max_digits=6, decimal_places=2, null=True, blank=True)
    descripcion = models.TextField(blank=True, default="")

    class Meta:
        db_table = "cap_formula_evaluacion"
        ordering = ["capacitacion", "orden"]


# ---------------------------------------------------------------------------
# Plan de trabajo: productos e indicadores (antes pt_productos_indicadores_json)
# ---------------------------------------------------------------------------
class CapProductoIndicador(models.Model):
    """Producto e indicador del plan de trabajo."""

    capacitacion = models.ForeignKey(
        Capacitacion, on_delete=models.CASCADE, related_name="productos_indicadores",
    )
    orden = models.PositiveSmallIntegerField(default=0)
    producto = models.TextField(blank=True, default="")
    indicador = models.TextField(blank=True, default="")
    meta = models.CharField(max_length=255, blank=True, default="")

    class Meta:
        db_table = "cap_productos_indicadores"
        ordering = ["capacitacion", "orden"]


# ---------------------------------------------------------------------------
# Generacion de recursos: campos del cronograma (antes gr_cronograma_campos_json)
# ---------------------------------------------------------------------------
class CapCronogramaCampo(models.Model):
    """Campo seleccionado para el cronograma."""

    capacitacion = models.ForeignKey(
        Capacitacion, on_delete=models.CASCADE, related_name="cronograma_campos",
    )
    orden = models.PositiveSmallIntegerField(default=0)
    nombre_campo = models.CharField(max_length=255)

    class Meta:
        db_table = "cap_cronograma_campos"
        ordering = ["capacitacion", "orden"]
