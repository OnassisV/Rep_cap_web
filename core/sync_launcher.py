"""Lanzador no bloqueante de la sincronizacion incremental hacia Railway."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

from django.conf import settings


def launch_incremental_sync() -> tuple[bool, str]:
    """Inicia la sincronizacion incremental en background si esta habilitada."""
    enabled = bool(
        getattr(settings, "INCREMENTAL_SYNC_ON_CAP_SELECTION", False)
        or getattr(settings, "INCREMENTAL_SYNC_ON_TEMPLATE_GENERATION", False)
    )
    if not enabled:
        return False, "disabled"

    runner_path = Path(getattr(settings, "INCREMENTAL_SYNC_RUNNER", "")).expanduser()
    if not runner_path.is_file():
        return False, "missing_runner"

    lock_dir = runner_path.parent / ".sincronizacion_incremental.lock"
    if lock_dir.exists():
        return False, "already_running"

    try:
        subprocess.Popen(
            ["/bin/zsh", str(runner_path)],
            cwd=str(runner_path.parent),
            env=os.environ.copy(),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
    except OSError:
        return False, "start_failed"

    return True, "started"


def launch_incremental_sync_from_template() -> tuple[bool, str]:
    """Compatibilidad con el nombre anterior del helper."""
    return launch_incremental_sync()