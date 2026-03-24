# CAP DIFOCA Web en Railway

Esta carpeta contiene la version de `app_cap_difoca` trasladada a `WEB-CAP` y adaptada para trabajar contra una base MySQL alojada en Railway. La aplicacion usa esa base como origen principal para Django y tambien como respaldo compartido para las tablas legacy de DIFOCA y Aula Virtual, salvo que se definan conexiones separadas.

## 1. Preparacion local

```bash
cd "/Users/ovarillas/Library/CloudStorage/OneDrive-Personal/MINEDU backup/2026/CODIGOS/Web_CAP_DIFOCA/WEB-CAP"
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
cp .env.example .env
```

## 2. Configurar `.env`

Variables minimas para trabajar con Railway:

- `DJANGO_SECRET_KEY`
- `DJANGO_DEBUG`
- `DJANGO_ALLOWED_HOSTS`
- `DJANGO_CSRF_TRUSTED_ORIGINS`
- `RAILWAY_MYSQL_URL`

Comportamiento esperado:

- Si `RAILWAY_MYSQL_URL` esta definida, Django usara MySQL de Railway como base principal.
- `LEGACY_DB` y `AULA_DB` reutilizaran esa misma conexion, salvo que declares overrides con `DIFOCA_DB_LOCAL_*` o `DIFOCA_DB_AULA_*`.
- El login consulta la tabla `usuarios` en la base sincronizada y valida la clave con `bcrypt`.

## 3. Inicializar Django en local

```bash
source .venv/bin/activate
python manage.py migrate
python manage.py collectstatic --noinput
python manage.py runserver
```

## 4. Archivos de despliegue

Se incluyen artefactos listos para publicar en Railway:

- `Procfile`
- `build.sh`
- `start.sh`
- `.python-version`
- `runtime.txt`

## 5. Flujo recomendado de publicacion

1. Crear un servicio nuevo en Railway apuntando a esta carpeta `WEB-CAP`.
2. Cargar las variables del `.env` en Railway, ajustando hosts y dominios reales.
3. Confirmar que `RAILWAY_MYSQL_URL` apunte a la base donde ya importaste las tablas DIFOCA y Aula Virtual.
4. Ejecutar el primer despliegue.
5. Validar login, menu principal y el modulo de registro/seguimiento.

## 6. Rutas principales

- Login: `http://127.0.0.1:8000/cuentas/login/`
- Inicio: `http://127.0.0.1:8000/app/`

## 7. Estado funcional esperado

- Login moderno y responsive.
- Sugerencia de usuarios desde tabla `usuarios` en Railway.
- Bloqueo temporal por intentos fallidos.
- Sesion Django activa al autenticar correctamente.
- Registro de capacitaciones y consultas operativas usando la base MySQL sincronizada.

# Rep_cap_web
