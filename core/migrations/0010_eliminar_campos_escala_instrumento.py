from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0009_capsincronicaprocesamiento"),
    ]

    operations = [
        migrations.RemoveField(
            model_name="capacitacion",
            name="diag_instr_misma_escala",
        ),
        migrations.RemoveField(
            model_name="capacitacion",
            name="diag_instr_puntos_escala",
        ),
        migrations.RemoveField(
            model_name="capacitacion",
            name="diag_instr_escala_1",
        ),
        migrations.RemoveField(
            model_name="capacitacion",
            name="diag_instr_escala_2",
        ),
        migrations.RemoveField(
            model_name="capacitacion",
            name="diag_instr_escala_3",
        ),
    ]
