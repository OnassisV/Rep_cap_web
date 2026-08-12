"""Purga el binario en Google Drive de evidencias cuyo caso lleva cerrado
mas de CASUISTICA_EVIDENCIA_RETENCION_DIAS (30 por defecto).

El mensaje y la metadata de la evidencia (nombre, tamaño, fecha) se
conservan como historial; solo se borra el archivo en Drive y se marca
`purgada_en`. Si el caso se reabre antes del plazo, deja de ser candidato.

Uso:
    python manage.py purgar_evidencias_casuisticas
    python manage.py purgar_evidencias_casuisticas --dry-run
"""

from datetime import timedelta

from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils import timezone

from core import drive_storage
from core.models import Casuistica, CasuisticaEvidencia


class Command(BaseCommand):
    help = "Borra en Google Drive las evidencias de casos cerrados hace mas del plazo de retencion."

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            default=False,
            help="Solo muestra qué se borraría, sin tocar Drive ni la BD.",
        )

    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        dias = getattr(settings, "CASUISTICA_EVIDENCIA_RETENCION_DIAS", 30)
        limite = timezone.now() - timedelta(days=dias)

        candidatas = (
            CasuisticaEvidencia.objects.filter(
                purgada_en__isnull=True,
                mensaje__casuistica__estado=Casuistica.Estado.CERRADO,
                mensaje__casuistica__cerrado_en__lte=limite,
            )
            .select_related("mensaje__casuistica")
        )

        total = candidatas.count()
        if total == 0:
            self.stdout.write("No hay evidencias por purgar.")
            return

        self.stdout.write(f"{total} evidencia(s) superan {dias} días desde el cierre de su caso.")
        if dry_run:
            for ev in candidatas:
                self.stdout.write(f"  [dry-run] {ev.nombre_original} (caso #{ev.mensaje.casuistica_id})")
            return

        borradas = 0
        fallidas = 0
        for ev in candidatas:
            try:
                ok = drive_storage.eliminar_evidencia(ev.drive_file_id)
            except drive_storage.DriveNoConfigurado as exc:
                self.stderr.write(self.style.ERROR(str(exc)))
                return
            if ok:
                ev.purgada_en = timezone.now()
                ev.save(update_fields=["purgada_en"])
                borradas += 1
            else:
                fallidas += 1

        self.stdout.write(self.style.SUCCESS(f"✔ {borradas} evidencia(s) purgada(s)."))
        if fallidas:
            self.stdout.write(self.style.WARNING(f"⚠ {fallidas} no se pudieron borrar (revisa el log)."))
