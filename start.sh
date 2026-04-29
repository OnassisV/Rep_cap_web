#!/usr/bin/env bash
set -euo pipefail

PORT="${PORT:-8000}"

python manage.py migrate --noinput
python manage.py importar_oferta_a_cap || echo "WARN: importar_oferta_a_cap falló, continuando..."
python manage.py collectstatic --noinput
# --channel-timeout=1800: requests largos (emisión masiva de certificados).
# --threads=8: permite que el polling de progreso siga atendiéndose mientras
#   un POST está generando PDFs.
exec waitress-serve \
    --listen="0.0.0.0:${PORT}" \
    --channel-timeout=1800 \
    --threads=8 \
    app_cap_difoca.wsgi:application
