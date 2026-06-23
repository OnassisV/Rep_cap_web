from django.db import migrations, models


DNIS_INICIALES = [
    ("40231243", "DNI administrativo/heredado fuera del universo operativo"),
    ("42116227", "DNI administrativo/heredado fuera del universo operativo"),
    ("09616505", "DNI administrativo/heredado fuera del universo operativo"),
    ("25790623", "DNI administrativo/heredado fuera del universo operativo"),
    ("40773688", "DNI administrativo/heredado fuera del universo operativo"),
    ("46935574", "DNI administrativo/heredado fuera del universo operativo"),
    ("40608330", "DNI administrativo/heredado fuera del universo operativo"),
    ("10818490", "DNI administrativo/heredado fuera del universo operativo"),
    ("10525745", "DNI administrativo/heredado fuera del universo operativo"),
    ("10090934", "DNI administrativo/heredado fuera del universo operativo"),
    ("40222323", "DNI administrativo/heredado fuera del universo operativo"),
    ("18113751", "DNI administrativo/heredado fuera del universo operativo"),
    ("44803184", "DNI administrativo/heredado fuera del universo operativo"),
    ("43989121", "DNI administrativo/heredado fuera del universo operativo"),
    ("41410367", "DNI administrativo/heredado fuera del universo operativo"),
    ("08977083", "DNI administrativo/heredado fuera del universo operativo"),
    ("87654321", "DNI de prueba/test"),
]


def cargar_dnis_iniciales(apps, schema_editor):
    DniExcluido = apps.get_model("core", "DniExcluido")
    for dni, motivo in DNIS_INICIALES:
        DniExcluido.objects.get_or_create(dni=dni, defaults={"motivo": motivo})


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0013_pt_modalidades_json"),
    ]

    operations = [
        migrations.CreateModel(
            name="DniExcluido",
            fields=[
                ("id", models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("dni", models.CharField(max_length=20, unique=True)),
                ("motivo", models.CharField(blank=True, default="", max_length=255)),
                ("creado_en", models.DateTimeField(auto_now_add=True)),
            ],
            options={
                "verbose_name": "DNI excluido",
                "verbose_name_plural": "DNIs excluidos",
                "db_table": "cap_dnis_excluidos",
                "ordering": ["dni"],
            },
        ),
        migrations.RunPython(cargar_dnis_iniciales, migrations.RunPython.noop),
    ]
