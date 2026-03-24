"""Configuracion Django para el proyecto web CAP DIFOCA."""

# Importaciones de libreria estandar para rutas y variables de entorno.
import os
from pathlib import Path
from urllib.parse import unquote, urlparse

# Utilidad externa para cargar variables desde el archivo local .env.
from dotenv import load_dotenv


# Carpeta raiz del proyecto, usada como base para otras rutas.
BASE_DIR = Path(__file__).resolve().parent.parent

# Carga variables de entorno desde .env si el archivo existe.
load_dotenv(BASE_DIR / ".env")


def env_bool(name: str, default: bool = False) -> bool:
    """Convierte variables tipo flag a booleano de forma consistente."""
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    return raw_value.strip().lower() in {"1", "true", "yes", "on"}


def env_str(name: str, default: str = "") -> str:
    """Lee strings desde entorno tratando vacios como ausencia de valor."""
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    cleaned_value = raw_value.strip()
    return cleaned_value if cleaned_value else default


def env_int(name: str, default: int) -> int:
    """Convierte variables numericas a entero con fallback seguro."""
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    cleaned_value = raw_value.strip()
    if not cleaned_value:
        return default
    return int(cleaned_value)


def split_env_list(name: str, default: str = "") -> list[str]:
    """Divide una variable separada por comas en lista limpia."""
    raw_value = env_str(name, default)
    return [item.strip() for item in raw_value.split(",") if item.strip()]


def parse_mysql_url(raw_url: str) -> dict[str, str | int]:
    """Descompone una URL MySQL estilo Railway en un diccionario utilizable."""
    parsed = urlparse(raw_url)
    if parsed.scheme.lower() not in {"mysql", "mysql+pymysql", "mysql+mysqlconnector"}:
        raise ValueError("La URL configurada no es MySQL.")

    database = parsed.path.lstrip("/")
    if not database:
        raise ValueError("La URL MySQL no incluye nombre de base de datos.")

    return {
        "host": parsed.hostname or "",
        "port": parsed.port or 3306,
        "user": unquote(parsed.username or ""),
        "password": unquote(parsed.password or ""),
        "database": unquote(database),
    }


def resolve_shared_mysql_url() -> dict[str, str | int] | None:
    """Devuelve la primera URL de entorno que realmente sea MySQL."""
    candidate_names = ["RAILWAY_MYSQL_URL", "MYSQL_URL", "DATABASE_URL"]
    for name in candidate_names:
        raw_url = env_str(name)
        if not raw_url:
            continue
        try:
            return parse_mysql_url(raw_url)
        except ValueError:
            continue
    return None


def build_mysql_connection(env_prefix: str, fallback: dict[str, str | int] | None = None) -> dict[str, str | int]:
    """Construye una configuracion MySQL desde variables sueltas con fallback opcional."""
    fallback = fallback or {}
    return {
        "host": env_str(f"{env_prefix}_HOST", str(fallback.get("host", ""))),
        "port": env_int(f"{env_prefix}_PORT", int(fallback.get("port", 3306))),
        "user": env_str(f"{env_prefix}_USER", str(fallback.get("user", ""))),
        "password": env_str(f"{env_prefix}_PASSWORD", str(fallback.get("password", ""))),
        "database": env_str(f"{env_prefix}_NAME", str(fallback.get("database", ""))),
    }


def build_django_database_config(fallback_mysql: dict[str, str | int] | None = None) -> dict[str, str | int]:
    """Resuelve la base principal de Django: MySQL remoto o SQLite local como respaldo."""
    fallback_mysql = fallback_mysql or {}

    host = env_str("DJANGO_DB_HOST", str(fallback_mysql.get("host", "")))
    name = env_str("DJANGO_DB_NAME", str(fallback_mysql.get("database", "")))
    user = env_str("DJANGO_DB_USER", str(fallback_mysql.get("user", "")))
    password = env_str("DJANGO_DB_PASSWORD", str(fallback_mysql.get("password", "")))
    port = env_int("DJANGO_DB_PORT", int(fallback_mysql.get("port", 3306)))

    if host and name and user:
        return {
            "ENGINE": "django.db.backends.mysql",
            "NAME": name,
            "USER": user,
            "PASSWORD": password,
            "HOST": host,
            "PORT": port,
            "OPTIONS": {
                "charset": "utf8mb4",
            },
        }

    return {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": BASE_DIR / "db.sqlite3",
    }


SHARED_MYSQL = resolve_shared_mysql_url()

# Clave secreta usada por Django para sesiones y firmado criptografico.
SECRET_KEY = os.getenv(
    "DJANGO_SECRET_KEY",
    "django-insecure-change-this-for-production",
)

# Interruptor de desarrollo. Usa "0" en .env para comportamiento tipo produccion.
DEBUG = env_bool("DJANGO_DEBUG", default=True)

# Hosts permitidos para servir esta aplicacion.
# Incluye "testserver" para que el cliente de pruebas de Django funcione sin ajustes extra.
ALLOWED_HOSTS = split_env_list(
    "DJANGO_ALLOWED_HOSTS",
    default="127.0.0.1,localhost,testserver",
)

# Dominios validos para CSRF en despliegues publicos detras de proxy.
CSRF_TRUSTED_ORIGINS = split_env_list("DJANGO_CSRF_TRUSTED_ORIGINS")

# Apps de Django y apps locales habilitadas en este proyecto.
INSTALLED_APPS = [
    "django.contrib.admin",  # Sitio de administracion.
    "django.contrib.auth",  # Framework de autenticacion.
    "django.contrib.contenttypes",  # Tipos de contenido genericos.
    "django.contrib.sessions",  # Manejo de sesiones.
    "django.contrib.messages",  # Mensajes temporales.
    "django.contrib.staticfiles",  # Manejo de archivos estaticos.
    "accounts",  # Login/logout y backend de autenticacion personalizado.
    "core",  # Inicio protegido y futuros modulos de la app.
]

# Cadena de middlewares para peticiones y respuestas.
MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",  # Encabezados de seguridad.
    "whitenoise.middleware.WhiteNoiseMiddleware",  # Sirve estaticos con servidor WSGI (ej. waitress).
    "django.contrib.sessions.middleware.SessionMiddleware",  # Soporte de sesion.
    "django.middleware.common.CommonMiddleware",  # Comportamiento HTTP comun.
    "django.middleware.csrf.CsrfViewMiddleware",  # Proteccion CSRF.
    "django.contrib.auth.middleware.AuthenticationMiddleware",  # request.user.
    "django.contrib.messages.middleware.MessageMiddleware",  # Sistema de mensajes.
    "django.middleware.clickjacking.XFrameOptionsMiddleware",  # Proteccion anti-iframe.
]

# Modulo principal de enrutamiento URL.
ROOT_URLCONF = "app_cap_difoca.urls"

# Configuracion del motor de plantillas.
TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",  # Motor de plantillas Django.
        "DIRS": [BASE_DIR / "templates"],  # Carpeta global de plantillas.
        "APP_DIRS": True,  # Tambien busca plantillas dentro de cada app.
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.request",  # Expone request.
                "django.contrib.auth.context_processors.auth",  # Expone user/auth.
                "django.contrib.messages.context_processors.messages",  # Expone mensajes.
            ],
        },
    },
]

# Modulo de entrada WSGI (usado por muchos servidores).
WSGI_APPLICATION = "app_cap_difoca.wsgi.application"

# Base principal de Django: MySQL remoto cuando existe, SQLite solo como fallback local.
DATABASES = {
    "default": build_django_database_config(SHARED_MYSQL),
}

# Validadores de contrasena integrados (principalmente para usuarios creados en admin Django).
AUTH_PASSWORD_VALIDATORS = [
    {
        "NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.MinimumLengthValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.CommonPasswordValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.NumericPasswordValidator",
    },
]

# Configuracion regional.
LANGUAGE_CODE = "es-pe"  # Espanol (Peru).
TIME_ZONE = "America/Lima"  # Zona horaria local para fechas y horas.
USE_I18N = True  # Activa sistema de traducciones.
USE_TZ = True  # Usa fechas con zona horaria.

# Configuracion de archivos estaticos.
STATIC_URL = "/static/"  # Prefijo URL para recursos estaticos.
STATICFILES_DIRS = [BASE_DIR / "static"]  # Carpetas estaticas en desarrollo.
STATIC_ROOT = BASE_DIR / "staticfiles"  # Carpeta destino para collectstatic.
STATICFILES_STORAGE = "whitenoise.storage.CompressedStaticFilesStorage"  # Compresion ligera de estaticos.

# Ajustes basicos de proxy/HTTPS para despliegues en Railway u otro PaaS.
SECURE_PROXY_SSL_HEADER = ("HTTP_X_FORWARDED_PROTO", "https")
USE_X_FORWARDED_HOST = True
# En Railway ya hay terminacion HTTPS delante de la app. Forzar redirect aqui
# produce bucles 301 porque la app recibe trafico interno detras del proxy.
SECURE_SSL_REDIRECT = False
SESSION_COOKIE_SECURE = env_bool("DJANGO_SESSION_COOKIE_SECURE", default=not DEBUG)
CSRF_COOKIE_SECURE = env_bool("DJANGO_CSRF_COOKIE_SECURE", default=not DEBUG)
SECURE_HSTS_SECONDS = env_int("DJANGO_SECURE_HSTS_SECONDS", 0 if DEBUG else 3600)
SECURE_HSTS_INCLUDE_SUBDOMAINS = env_bool("DJANGO_SECURE_HSTS_INCLUDE_SUBDOMAINS", default=not DEBUG)
SECURE_HSTS_PRELOAD = env_bool("DJANGO_SECURE_HSTS_PRELOAD", default=False)

# Tipo de clave primaria por defecto para modelos Django.
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

# Sincronizacion incremental disparada desde la web.
INCREMENTAL_SYNC_ON_CAP_SELECTION = env_bool(
    "INCREMENTAL_SYNC_ON_CAP_SELECTION",
    default=env_bool("INCREMENTAL_SYNC_ON_TEMPLATE_GENERATION", default=True),
)
INCREMENTAL_SYNC_ON_TEMPLATE_GENERATION = env_bool(
    "INCREMENTAL_SYNC_ON_TEMPLATE_GENERATION",
    default=True,
)
INCREMENTAL_SYNC_UI_REFRESH_SECONDS = env_int(
    "INCREMENTAL_SYNC_UI_REFRESH_SECONDS",
    15,
)
INCREMENTAL_SYNC_RUNNER = env_str(
    "INCREMENTAL_SYNC_RUNNER",
    str((BASE_DIR.parent / "sincronizacion_incremental_railway" / "ejecutar_sincronizacion_incremental.sh").resolve()),
)

# Cache en memoria para contadores de bloqueo y ventanas de bloqueo de login.
CACHES = {
    "default": {
        "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
        "LOCATION": "app-cap-difoca-login-cache",
    }
}

# URLs de redireccion para autenticacion.
LOGIN_URL = "accounts:login"  # Destino cuando login_required detecta usuario anonimo.
LOGIN_REDIRECT_URL = "core:home"  # Destino tras login exitoso.
LOGOUT_REDIRECT_URL = "accounts:login"  # Destino tras cerrar sesion.

# Orden de backends de autenticacion:
# 1) backend personalizado contra tabla MySQL legacy
# 2) backend por defecto de modelos Django
AUTHENTICATION_BACKENDS = [
    "accounts.backends.LocalhostUsuariosBackend",
    "django.contrib.auth.backends.ModelBackend",
]

# Parametros de seguridad para login.
MAX_LOGIN_ATTEMPTS = env_int("DIFOCA_MAX_LOGIN_ATTEMPTS", 5)
LOGIN_LOCKOUT_MINUTES = env_int("DIFOCA_LOGIN_LOCKOUT_MINUTES", 10)
LOCKOUT_FAILURE_TTL_MINUTES = env_int("DIFOCA_LOCKOUT_FAILURE_TTL_MINUTES", 60)

# Mapeo de cargos legacy hacia roles de la aplicacion.
ROLE_MAPPING = {
    "coordinador": "Administrador",
    "encargado": "Administrador",
    "soporte": "Administrador",
    "especialista": "Usuario estandar",
}

# Configuracion de conexion para tablas operativas sincronizadas en Railway.
LEGACY_DB = build_mysql_connection("DIFOCA_DB_LOCAL", SHARED_MYSQL)

# Configuracion de Aula Virtual. Puede compartir el mismo MySQL o usar otro si se requiere.
AULA_DB = build_mysql_connection("DIFOCA_DB_AULA", SHARED_MYSQL)
