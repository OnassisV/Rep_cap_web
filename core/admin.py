from django.contrib import admin

from .models import CapacitacionAuditLog, DniExcluido


@admin.register(DniExcluido)
class DniExcluidoAdmin(admin.ModelAdmin):
    list_display = ("dni", "motivo", "creado_en")
    search_fields = ("dni", "motivo")
    ordering = ("dni",)


@admin.register(CapacitacionAuditLog)
class CapacitacionAuditLogAdmin(admin.ModelAdmin):
    list_display = ("timestamp", "usuario", "accion", "cap_id", "cap_codigo", "cap_nombre", "detalle")
    list_filter = ("accion",)
    search_fields = ("usuario", "cap_codigo", "cap_nombre", "detalle")
    ordering = ("-timestamp",)
    date_hierarchy = "timestamp"

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False
