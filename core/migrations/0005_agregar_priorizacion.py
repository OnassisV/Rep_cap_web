from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0004_agregar_campos_json_builders'),
    ]

    operations = [
        migrations.AddField(
            model_name='capacitacion',
            name='diag_priorizacion_json',
            field=models.TextField(blank=True, default=''),
        ),
        migrations.AddField(
            model_name='capacitacion',
            name='diag_ec2_relacion_logica',
            field=models.CharField(blank=True, default='', max_length=10),
        ),
    ]
