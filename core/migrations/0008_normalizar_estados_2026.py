from django.db import migrations
from django.db.models import Q


def normalizar_estados_existentes(apps, schema_editor):
    Capacitacion = apps.get_model('core', 'Capacitacion')

    Capacitacion.objects.filter(cap_estado='Borrador').update(cap_estado='Formulada')

    Capacitacion.objects.exclude(cap_estado='Cancelada').filter(
        cap_anio__gte=2026,
        cap_estado__in=['Finalizada', 'Por finalizar'],
    ).update(cap_estado='En proceso')

    Capacitacion.objects.exclude(cap_estado='Cancelada').filter(
        Q(cap_codigo__isnull=True) | Q(cap_codigo=''),
        Q(cap_id_curso__isnull=True) | Q(cap_id_curso=''),
    ).update(cap_estado='Formulada')


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0007_alter_capacitacion_cap_estado'),
    ]

    operations = [
        migrations.RunPython(normalizar_estados_existentes, migrations.RunPython.noop),
    ]
