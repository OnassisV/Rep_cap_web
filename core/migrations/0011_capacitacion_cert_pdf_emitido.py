from django.db import migrations, models


def marcar_historicos_como_emitidos(apps, schema_editor):
    """Marca como emitidos todos los cursos de 2024 y años anteriores."""
    Capacitacion = apps.get_model("core", "Capacitacion")
    Capacitacion.objects.filter(cap_anio__lte=2024).update(cert_pdf_emitido=True)


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0010_eliminar_campos_escala_instrumento"),
    ]

    operations = [
        migrations.AddField(
            model_name="capacitacion",
            name="cert_pdf_emitido",
            field=models.BooleanField(
                default=False,
                help_text="Indica si se emitieron los certificados PDF por lote para este curso.",
            ),
        ),
        migrations.RunPython(
            marcar_historicos_como_emitidos,
            reverse_code=migrations.RunPython.noop,
        ),
    ]
