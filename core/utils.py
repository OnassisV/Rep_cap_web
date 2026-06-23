"""Utilidades compartidas entre los adaptadores de core."""

import unicodedata
from typing import Any


def normalizar_texto(valor: Any) -> str:
    """Convierte a string ASCII en minúsculas sin tildes para comparaciones robustas.

    Uso: comparar condiciones, roles, nombres con variantes de acentuación o case.
    Ejemplo: 'Implementación' → 'implementacion'
    """
    texto = str(valor or "").strip()
    texto = unicodedata.normalize("NFKD", texto).encode("ascii", "ignore").decode("ascii")
    return texto.lower()


def normalizar_texto_upper(valor: Any) -> str:
    """Convierte a string ASCII en MAYÚSCULAS sin tildes vocálicas (conserva Ñ).

    Uso: normalizar región e IGED para joins y comparaciones de catálogos.
    Ejemplo: 'Huánuco' → 'HUANUCO', 'Ñaupa' → 'ÑAUPA'
    """
    texto = str(valor or "").strip().upper()
    for orig, repl in [("Á", "A"), ("É", "E"), ("Í", "I"), ("Ó", "O"), ("Ú", "U"), ("Ü", "U")]:
        texto = texto.replace(orig, repl)
    return texto


def normalizar_token(valor: Any) -> str:
    """Normaliza para detección de patrones: minúsculas, sin tildes, espacios colapsados.

    Uso: comparar encabezados de encuestas con variantes ortográficas.
    Ejemplo: 'Aspecto  /  Pregunta' → 'aspecto / pregunta'
    """
    import re
    texto = str(valor or "").strip()
    texto = unicodedata.normalize("NFKD", texto).encode("ascii", "ignore").decode("ascii")
    texto = re.sub(r"\s+", " ", texto)
    return texto.lower()
