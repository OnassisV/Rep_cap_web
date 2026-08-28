"""Management command para diagnosticar el estado de matriculas en Chamilo."""

from django.core.management.base import BaseCommand
from core.legacy_adapters import diagnosticar_estado_chamilo


class Command(BaseCommand):
    help = "Diagnostica estado de matriculas en Chamilo para un código de capacitación"

    def add_arguments(self, parser):
        parser.add_argument(
            "codigo",
            type=str,
            help="Código de capacitación (ej. COMP2024-001)",
        )

    def handle(self, *args, **options):
        codigo = options["codigo"]
        self.stdout.write(f"\n📋 Diagnosticando Chamilo para código: {codigo}\n")
        
        resultado = diagnosticar_estado_chamilo(codigo)
        
        if "error" in resultado:
            self.stdout.write(self.style.ERROR(f"❌ Error: {resultado['error']}"))
            return
        
        self.stdout.write("=" * 70)
        self.stdout.write(self.style.SUCCESS(f"✓ Código: {resultado['codigo']}"))
        self.stdout.write(f"  Curso ID: {resultado['curso_id']}")
        self.stdout.write(f"  Total registros en curso: {resultado['total_registros_curso']}")
        self.stdout.write("=" * 70)
        
        self.stdout.write("\n📊 Distribución por status:")
        dist = resultado.get("distribucion_por_status", {})
        for status in sorted(dist.keys()):
            qty = dist[status]
            status_name = {
                5: "MATRICULADO",
                0: "No matriculado",
                1: "Registrado",
                3: "Retirado",
                4: "Suspendido",
            }.get(status, f"Status {status}")
            self.stdout.write(f"  Status {status} ({status_name}): {qty}")
        
        self.stdout.write(f"\n👥 Total con status=5 (MATRICULADO): {resultado['cantidad_status5']}")
        
        if resultado['cantidad_status5'] > 0 and resultado['cantidad_status5'] <= 20:
            self.stdout.write("\nDNIs con status=5:")
            for dni in resultado.get('dnis_status5', []):
                self.stdout.write(f"  • {dni}")
        elif resultado['cantidad_status5'] > 20:
            dnis_muestra = resultado.get('dnis_status5', [])[:20]
            self.stdout.write(f"\nPrimeros 20 de {resultado['cantidad_status5']} DNIs con status=5:")
            for dni in dnis_muestra:
                self.stdout.write(f"  • {dni}")
            self.stdout.write(f"  ... y {resultado['cantidad_status5'] - 20} más")
        
        self.stdout.write(f"\n📈 Total de DNIs distintos en el curso: {resultado['total_dnis_distintos']}")
        
        self.stdout.write("\n" + "=" * 70)
        self.stdout.write(self.style.WARNING(
            "\n⚠️  IMPORTANTE:"
            "\n- Si hay muchos registros con status=5 pero los desmatriculaste en Chamilo,"
            "\n  es posible que la sincronización Railway no se haya aplicado."
            "\n- Verifica que los cambios en Chamilo se hayan guardado correctamente."
            "\n- Si necesitas forzar resincronización, usa: python manage.py sync_chamilo"
        ))
        self.stdout.write("\n")
