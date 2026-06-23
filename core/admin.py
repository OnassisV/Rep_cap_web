from django.contrib import admin

from .models import DniExcluido


@admin.register(DniExcluido)
class DniExcluidoAdmin(admin.ModelAdmin):
    list_display = ("dni", "motivo", "creado_en")
    search_fields = ("dni", "motivo")
    ordering = ("dni",)
