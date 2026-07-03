"""Management command: recalcula cap_estado para capacitaciones desactualizadas.

Los certificados se sincronizan externamente en bbdd_difoca, sin pasar por el
flujo del formulario web, así que cap_estado puede quedar desactualizado
(en "En proceso" aunque ya haya certificados y pasos completos). Este comando
reutiliza la misma lógica que corre automáticamente en los listados de
Gestión de Capacitaciones y Sincrónicas, para poder ejecutarla también por
cron o manualmente.
"""

from __future__ import annotations

from django.core.management.base import BaseCommand

from core.models import Capacitacion
from core.views import _recalcular_estados_pendientes


class Command(BaseCommand):
    help = "Recalcula cap_estado de capacitaciones con pasos completos y certificados emitidos."

    def handle(self, *args, **options):
        antes = dict(
            Capacitacion.objects.exclude(
                cap_estado__in=[Capacitacion.Estado.CANCELADA, Capacitacion.Estado.FINALIZADA]
            ).values_list("id", "cap_estado")
        )

        _recalcular_estados_pendientes(Capacitacion.objects.all())

        cambiados = Capacitacion.objects.filter(
            id__in=antes.keys(), cap_estado=Capacitacion.Estado.FINALIZADA
        )
        n = cambiados.count()
        if n:
            for cap in cambiados:
                self.stdout.write(f"  {cap.cap_codigo}-{cap.cap_id_curso} -> Finalizada")
        self.stdout.write(self.style.SUCCESS(f"Capacitaciones actualizadas a Finalizada: {n}"))
