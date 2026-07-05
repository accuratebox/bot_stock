from __future__ import annotations

import os
import signal
import sys
import threading
import time
import tkinter as tk


def _target_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def main() -> int:
    if len(sys.argv) < 2:
        return 2

    try:
        target_pid = int(sys.argv[1])
    except ValueError:
        return 2

    root = tk.Tk()
    root.title("Cierre de emergencia")
    root.attributes("-topmost", True)
    root.resizable(False, False)
    root.geometry("240x90")

    label = tk.Label(
        root,
        text=f"Bot PID {target_pid}",
        font=("TkDefaultFont", 10, "bold"),
    )
    label.pack(pady=(10, 4))

    status_var = tk.StringVar(value="Listo para forzar cierre")
    tk.Label(root, textvariable=status_var, fg="#444").pack(pady=(0, 6))

    def close_if_gone() -> None:
        if not _target_alive(target_pid):
            try:
                root.destroy()
            except tk.TclError:
                pass
            return
        root.after(1000, close_if_gone)

    def force_kill() -> None:
        status_var.set("Dumping stacks y cerrando...")

        def worker() -> None:
            try:
                os.kill(target_pid, signal.SIGUSR1)
            except Exception:
                pass
            time.sleep(0.5)
            try:
                os.kill(target_pid, signal.SIGTERM)
            except Exception:
                pass
            time.sleep(1.0)
            if _target_alive(target_pid):
                try:
                    os.kill(target_pid, signal.SIGKILL)
                except Exception:
                    pass
            try:
                root.after(0, root.destroy)
            except tk.TclError:
                pass

        threading.Thread(target=worker, daemon=True).start()

    button = tk.Button(
        root,
        text="FORZAR CIERRE",
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
    root.mainloop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())