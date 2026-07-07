from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import threading
import time
import tkinter as tk
from pathlib import Path


def _target_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _kill_target(pid: int) -> None:
    try:
        os.kill(pid, signal.SIGUSR1)
    except Exception:
        pass
    time.sleep(0.5)
    try:
        os.kill(pid, signal.SIGTERM)
    except Exception:
        pass
    deadline = time.time() + 3.0
    while _target_alive(pid) and time.time() < deadline:
        time.sleep(0.2)
    if _target_alive(pid):
        try:
            os.kill(pid, signal.SIGKILL)
        except Exception:
            pass


def _heartbeat_age_seconds(path: Path) -> float | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    timestamp = payload.get("updated_at")
    if timestamp is None:
        return None
    try:
        ts = float(timestamp)
    except (TypeError, ValueError):
        return None
    return max(time.time() - ts, 0.0)


def _launch_bot(python_executable: str, main_path: str, working_dir: str) -> bool:
    if _main_instance_count(main_path) > 0:
        print("[helper] Ya existe una instancia main.py activa. No se lanza duplicado.", file=sys.stderr, flush=True)
        return True

    candidates = [
        [python_executable, main_path],
        [sys.executable, main_path],
        ["python3", main_path],
    ]

    for command in candidates:
        try:
            process = subprocess.Popen(
                command,
                cwd=working_dir,
                start_new_session=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                text=True,
            )
            time.sleep(1.5)
            if process.poll() is None:
                print(f"[helper] Reinicio OK con comando: {' '.join(command)}", file=sys.stderr, flush=True)
                return True
            if _main_instance_count(main_path) > 0:
                print("[helper] Main ya activo tras intento de reinicio.", file=sys.stderr, flush=True)
                return True
            print(
                f"[helper] Proceso reiniciado terminó inmediatamente (rc={process.poll()}) con comando: {' '.join(command)}",
                file=sys.stderr,
                flush=True,
            )
        except Exception as ex:
            print(
                f"[helper] Error intentando reiniciar con comando {' '.join(command)}: {ex}",
                file=sys.stderr,
                flush=True,
            )

    return False


def _main_instance_count(main_path: str) -> int:
    target = str(Path(main_path).resolve())
    count = 0
    for proc_cmdline in Path("/proc").glob("[0-9]*/cmdline"):
        try:
            raw = proc_cmdline.read_bytes()
            if not raw:
                continue
            parts = [item.decode("utf-8", errors="ignore") for item in raw.split(b"\x00") if item]
            joined = " ".join(parts)
            if target in joined and "main.py" in joined and "emergency_close_helper.py" not in joined:
                count += 1
        except Exception:
            continue
    return count


def main() -> int:
    if len(sys.argv) < 7:
        return 2

    try:
        target_pid = int(sys.argv[1])
    except ValueError:
        return 2

    heartbeat_path = Path(sys.argv[2])
    try:
        heartbeat_timeout = max(float(sys.argv[3]), 8.0)
    except ValueError:
        heartbeat_timeout = 25.0
    restart_python = sys.argv[4]
    restart_main = sys.argv[5]
    restart_cwd = sys.argv[6]
    allow_restart = (sys.argv[7].strip().lower() != "0") if len(sys.argv) > 7 else True
    stale_checks_required = 1
    if len(sys.argv) > 8:
        try:
            stale_checks_required = max(int(float(sys.argv[8] or 1)), 1)
        except ValueError:
            stale_checks_required = 1
    min_uptime_before_restart_seconds = 0.0
    if len(sys.argv) > 9:
        try:
            min_uptime_before_restart_seconds = max(float(sys.argv[9] or 0.0), 0.0)
        except ValueError:
            min_uptime_before_restart_seconds = 0.0
    restarting = False
    started_at = time.time()
    stale_hits = 0
    missing_heartbeat_hits = 0

    root = tk.Tk()
    root.title("Cierre de emergencia")
    root.attributes("-topmost", True)
    root.resizable(False, False)
    root.geometry("280x110")

    label = tk.Label(
        root,
        text=f"Bot PID {target_pid}",
        font=("TkDefaultFont", 10, "bold"),
    )
    label.pack(pady=(10, 4))

    status_var = tk.StringVar(value="Watchdog activo")
    tk.Label(root, textvariable=status_var, fg="#444").pack(pady=(0, 6))

    def close_if_gone() -> None:
        if not _target_alive(target_pid):
            try:
                root.destroy()
            except tk.TclError:
                pass
            return
        root.after(1000, close_if_gone)

    def restart_bot(reason: str) -> None:
        nonlocal restarting
        if restarting:
            return
        restarting = True
        status_var.set(reason)

        def worker() -> None:
            nonlocal restarting
            _kill_target(target_pid)
            launched = False
            if allow_restart:
                launched = _launch_bot(restart_python, restart_main, restart_cwd)

            if allow_restart and not launched:
                restarting = False
                try:
                    root.after(0, status_var.set, "No se pudo reiniciar. Revisa runtime/emergency_helper.log")
                except tk.TclError:
                    pass
                return

            try:
                root.after(0, root.destroy)
            except tk.TclError:
                pass

        threading.Thread(target=worker, daemon=True).start()

    def force_kill() -> None:
        restart_bot("Reiniciando bot...")

    def watchdog_loop() -> None:
        nonlocal stale_hits, missing_heartbeat_hits
        if restarting:
            return
        if not _target_alive(target_pid):
            try:
                root.destroy()
            except tk.TclError:
                pass
            return

        age = _heartbeat_age_seconds(heartbeat_path)
        if age is not None:
            missing_heartbeat_hits = 0
            status_var.set(f"Heartbeat: {age:.1f}s")
            if age >= heartbeat_timeout:
                stale_hits += 1
                uptime = max(time.time() - started_at, 0.0)
                status_var.set(
                    f"Heartbeat stale {stale_hits}/{stale_checks_required} ({age:.1f}s)"
                )
                if stale_hits >= stale_checks_required and uptime >= min_uptime_before_restart_seconds:
                    restart_bot("Freeze detectado, reiniciando...")
                    return
            else:
                stale_hits = 0
        else:
            # Missing heartbeat file or unreadable payload can also indicate a stuck UI.
            uptime = max(time.time() - started_at, 0.0)
            missing_heartbeat_hits += 1
            status_var.set(f"Heartbeat no disponible ({missing_heartbeat_hits})")
            if uptime >= min_uptime_before_restart_seconds and missing_heartbeat_hits >= stale_checks_required:
                restart_bot("Heartbeat ausente, reiniciando...")
                return
        root.after(1000, watchdog_loop)

    button = tk.Button(
        root,
        text="FORZAR REINICIO",
        command=force_kill,
        fg="white",
        bg="#8a1c1c",
        activebackground="#6b1414",
        relief="raised",
        bd=2,
        padx=12,
        pady=6,
    )
    button.pack(pady=(0, 10))

    root.after(1000, close_if_gone)
    root.after(1000, watchdog_loop)
    root.mainloop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())