from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0014_dniexcluido"),
    ]

    operations = [
        migrations.CreateModel(
            name="CapacitacionAuditLog",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("cap_id", models.IntegerField(db_index=True)),
                ("cap_codigo", models.CharField(blank=True, default="", max_length=50)),
                ("cap_nombre", models.CharField(blank=True, default="", max_length=255)),
                ("usuario", models.CharField(max_length=150)),
                ("accion", models.CharField(
                    choices=[
                        ("creada", "Creada"),
                        ("modificada", "Modificada"),
                        ("cancelada", "Cancelada"),
                        ("eliminada", "Eliminada"),
                        ("id_plataforma", "ID plataforma asignado"),
                    ],
                    max_length=20,
                )),
                ("detalle", models.TextField(blank=True, default="")),
                ("timestamp", models.DateTimeField(auto_now_add=True, db_index=True)),
            ],
            options={
                "verbose_name": "Registro de auditoría",
                "verbose_name_plural": "Registros de auditoría",
                "db_table": "cap_audit_log",
                "ordering": ["-timestamp"],
            },
        ),
    ]
