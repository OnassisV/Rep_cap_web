"""Puebla `especialista_usuario` cruzando el nombre del responsable con `usuarios`.

Hasta ahora la propiedad de una capacitacion se resolvia con `creado_por`, que
mezclaba dos cosas distintas: quien la registro y quien esta a cargo. Cuando un
administrador registraba una capacitacion para otro especialista, el responsable
real no la veia en sus modulos.

Esta migracion deja la llave estable (correo) sobre el responsable declarado en
`especialista_cargo`. Los registros historicos sin correo resoluble quedan
vacios a proposito: los filtros conservan un respaldo por nombre.
"""

from django.db import migrations


def _normalizar(nombre: str) -> str:
    """Normaliza un nombre para cruzarlo tolerando el prefijo '(OS) ' y espacios."""
    texto = " ".join(str(nombre or "").split()).strip()
    if texto.upper().startswith("(OS)"):
        texto = texto[4:].strip()
    return texto.casefold()


def poblar(apps, schema_editor):
    Capacitacion = apps.get_model("core", "Capacitacion")

    # La lista de usuarios vive en la tabla legacy `usuarios`, fuera del ORM.
    with schema_editor.connection.cursor() as cursor:
        try:
            cursor.execute("SELECT usuario, especialista_cargo FROM usuarios")
            filas = cursor.fetchall()
        except Exception:
            # Sin la tabla legacy no se puede cruzar; el respaldo por nombre
            # mantiene la app funcionando hasta que se pueble manualmente.
            return

    mapa = {}
    for usuario, nombre in filas:
        clave = _normalizar(nombre)
        if clave:
            mapa.setdefault(clave, str(usuario or "").strip())

    por_actualizar = []
    for cap in Capacitacion.objects.exclude(especialista_cargo="").only(
        "id", "especialista_cargo", "especialista_usuario"
    ):
        correo = mapa.get(_normalizar(cap.especialista_cargo))
        if correo and cap.especialista_usuario != correo:
            cap.especialista_usuario = correo
            por_actualizar.append(cap)

    if por_actualizar:
        Capacitacion.objects.bulk_update(por_actualizar, ["especialista_usuario"], batch_size=200)


def revertir(apps, schema_editor):
    Capacitacion = apps.get_model("core", "Capacitacion")
    Capacitacion.objects.update(especialista_usuario="")


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0019_capacitacion_especialista_usuario"),
    ]

    operations = [
        migrations.RunPython(poblar, revertir),
    ]
