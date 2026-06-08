"""Comando para marcar capacitaciones como certificadas (cert_pdf_emitido=True).

Uso por ID:
    python manage.py marcar_emitidos 42 87 103
    python manage.py marcar_emitidos 42 87 103 --desmarcar

Uso por código:
    python manage.py marcar_emitidos --codigo 26001X 26002X 26003X
    python manage.py marcar_emitidos --codigo 26001X --desmarcar
"""

from django.core.management.base import BaseCommand, CommandError
from core.models import Capacitacion


class Command(BaseCommand):
    help = "Marca (o desmarca) capacitaciones como cert_pdf_emitido=True/False por ID o por código."

    def add_arguments(self, parser):
        parser.add_argument(
            "ids",
            nargs="*",
            type=int,
            help="IDs internos de las capacitaciones a marcar.",
        )
        parser.add_argument(
            "--codigo",
            nargs="+",
            dest="codigos",
            default=[],
            help="Códigos de capacitación (cap_codigo) en lugar de IDs. Ej: 26001X 26002X",
        )
        parser.add_argument(
            "--desmarcar",
            action="store_true",
            default=False,
            help="Si se indica, pone cert_pdf_emitido=False en vez de True.",
        )

    def handle(self, *args, **options):
        ids = options.get("ids") or []
        codigos = [str(c).strip() for c in (options.get("codigos") or []) if str(c).strip()]
        nuevo_valor = not options["desmarcar"]
        accion = "EMITIDO" if nuevo_valor else "PENDIENTE"

        if not ids and not codigos:
            raise CommandError("Debes indicar al menos un ID o usar --codigo con al menos un código.")

        # Resolver códigos a registros
        if codigos:
            por_codigo = list(
                Capacitacion.objects.filter(cap_codigo__in=codigos)
                .values("id", "cap_nombre", "cap_codigo", "cap_anio", "cert_pdf_emitido")
            )
            codigos_encontrados = [c["cap_codigo"] for c in por_codigo]
            codigos_no_encontrados = [c for c in codigos if c not in codigos_encontrados]
            if codigos_no_encontrados:
                self.stdout.write(self.style.WARNING(f"Códigos no encontrados: {codigos_no_encontrados}"))
            ids += [c["id"] for c in por_codigo]

        # Verificar IDs
        existentes = list(
            Capacitacion.objects.filter(pk__in=ids)
            .values("id", "cap_nombre", "cap_codigo", "cap_anio", "cert_pdf_emitido")
        )
        ids_encontrados = [c["id"] for c in existentes]
        ids_no_encontrados = [i for i in ids if i not in ids_encontrados]

        if ids_no_encontrados:
            self.stdout.write(self.style.WARNING(f"IDs no encontrados en BD: {ids_no_encontrados}"))

        if not ids_encontrados:
            raise CommandError("Ningún registro encontrado. No se realizaron cambios.")

        # Resumen antes de aplicar
        self.stdout.write("\nCapacitaciones a actualizar:")
        for cap in existentes:
            estado_actual = "emitido" if cap["cert_pdf_emitido"] else "pendiente"
            self.stdout.write(
                f"  [{cap['id']}] {cap['cap_codigo'] or '—'} — {cap['cap_nombre'][:55]} "
                f"({cap['cap_anio']}) — estado actual: {estado_actual}"
            )

        # Actualizar
        actualizadas = Capacitacion.objects.filter(pk__in=ids_encontrados).update(
            cert_pdf_emitido=nuevo_valor
        )

        self.stdout.write(
            self.style.SUCCESS(f"\n✔ {actualizadas} capacitacion(es) marcadas como {accion}.")
        )
