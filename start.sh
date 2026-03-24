#!/usr/bin/env bash
set -euo pipefail

PORT="${PORT:-8000}"

python manage.py migrate --noinput
python manage.py collectstatic --noinput
exec waitress-serve --listen="0.0.0.0:${PORT}" app_cap_difoca.wsgi:application
