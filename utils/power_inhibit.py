from __future__ import annotations

import shutil
import subprocess
from dataclasses import dataclass
from typing import Optional


@dataclass
class PowerInhibitor:
    process: Optional[subprocess.Popen] = None

    def stop(self) -> None:
        if self.process is None:
            return
        try:
            self.process.terminate()
            self.process.wait(timeout=5)
        except Exception:
            try:
                self.process.kill()
            except Exception:
                pass
        finally:
            self.process = None


def start_power_inhibitor(logger: object | None = None) -> PowerInhibitor:
    inhibitor = PowerInhibitor()
    systemd_inhibit = shutil.which("systemd-inhibit")
    if systemd_inhibit is None:
        if logger is not None:
            try:
                logger.warning("systemd-inhibit no esta disponible; no se puede bloquear suspension")
            except Exception:
                pass
        return inhibitor

    command = [
        systemd_inhibit,
        "--what=sleep:idle:handle-lid-switch",
        "--who=Trading Bot",
        "--why=Trading bot debe seguir ejecutandose",
        "sleep",
        "infinity",
    ]
    try:
        inhibitor.process = subprocess.Popen(command)
        if logger is not None:
            try:
                logger.info("Inhibidor de suspension activado")
            except Exception:
                pass
    except Exception as ex:
        if logger is not None:
            try:
                logger.warning("No se pudo activar inhibidor de suspension: %s", ex)
            except Exception:
                pass
    return inhibitor