from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0012_caracterizacion_oficial"),
    ]

    operations = [
        migrations.AddField(
            model_name="capacitacion",
            name="pt_modalidades_json",
            field=models.TextField(blank=True, default=""),
        ),
    ]
