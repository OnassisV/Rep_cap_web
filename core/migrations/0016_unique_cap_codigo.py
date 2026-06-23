from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0015_capacitacionauditlog"),
    ]

    operations = [
        migrations.AddConstraint(
            model_name="capacitacion",
            constraint=models.UniqueConstraint(
                condition=models.Q(cap_codigo__gt=""),
                fields=["cap_codigo"],
                name="uniq_cap_codigo_no_vacio",
            ),
        ),
    ]
