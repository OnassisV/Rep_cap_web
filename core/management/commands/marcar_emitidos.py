"""Comando para marcar capacitaciones como certificadas (cert_pdf_emitido=True).

Uso:
    python manage.py marcar_emitidos 42 87 103
    python manage.py marcar_emitidos 42 87 103 --desmarcar
"""

from django.core.management.base import BaseCommand, CommandError
from core.models import Capacitacion


class Command(BaseCommand):
    help = "Marca (o desmarca) capacitaciones como cert_pdf_emitido=True/False por ID."

    def add_arguments(self, parser):
        parser.add_argument(
            "ids",
            nargs="+",
            type=int,
            help="IDs de las capacitaciones a marcar como emitidas.",
        )
        parser.add_argument(
            "--desmarcar",
            action="store_true",
            default=False,
            help="Si se indica, pone cert_pdf_emitido=False en vez de True.",
        )

    def handle(self, *args, **options):
        ids = options["ids"]
        nuevo_valor = not options["desmarcar"]
        accion = "EMITIDO" if nuevo_valor else "PENDIENTE"

        # Verificar cuáles existen antes de actualizar
        existentes = Capacitacion.objects.filter(pk__in=ids).values("id", "cap_nombre", "cap_anio", "cert_pdf_emitido")
        ids_encontrados = [c["id"] for c in existentes]
        ids_no_encontrados = [i for i in ids if i not in ids_encontrados]

        if ids_no_encontrados:
            self.stdout.write(
                self.style.WARNING(
                    f"IDs no encontrados en BD: {ids_no_encontrados}"
                )
            )

        if not ids_encontrados:
            raise CommandError("Ningún ID encontrado. No se realizaron cambios.")

        # Mostrar resumen antes de aplicar
        self.stdout.write("\nCapacitaciones encontradas:")
        for cap in existentes:
            estado_actual = "emitido" if cap["cert_pdf_emitido"] else "pendiente"
            self.stdout.write(
                f"  [{cap['id']}] {cap['cap_nombre'][:60]} "
                f"({cap['cap_anio']}) — estado actual: {estado_actual}"
            )

        # Actualizar
        actualizadas = Capacitacion.objects.filter(pk__in=ids_encontrados).update(
            cert_pdf_emitido=nuevo_valor
        )

        self.stdout.write(
            self.style.SUCCESS(
                f"\n✔ {actualizadas} capacitacion(es) marcadas como {accion}."
            )
        )
