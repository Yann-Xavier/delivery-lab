"""Demonstração com processos reais: emissor -> proxy com perda -> receptor.

Uso: python demo.py
"""

import os
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
from contextlib import closing, contextmanager
from pathlib import Path

import httpx

from sender import dispatch, initialize, record

HERE = Path(__file__).resolve().parent


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def wait_for_port(port, timeout=15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.1)
    raise TimeoutError(f"nada escutando na porta {port}")


@contextmanager
def running(args, port, env=None, show_output=False):
    process = subprocess.Popen(
        [sys.executable, *args], cwd=HERE, env={**os.environ, **(env or {})},
        stdout=None if show_output else subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    try:
        wait_for_port(port)
        yield process
    finally:
        process.terminate()
        process.wait(timeout=10)


@contextmanager
def receiver_behind_lossy_proxy(receiver_db, drop=1):
    """Sobe o receptor e o proxy e devolve a URL que o emissor deve usar."""
    receiver_port, proxy_port = free_port(), free_port()
    receiver_args = ["-m", "uvicorn", "receiver:app", "--port", str(receiver_port), "--log-level", "warning"]
    proxy_args = ["lossy_proxy.py", "--port", str(proxy_port),
                  "--upstream", f"http://127.0.0.1:{receiver_port}", "--drop", str(drop)]
    with running(receiver_args, receiver_port, {"RECEIVER_DB": str(receiver_db)}):
        with running(proxy_args, proxy_port, show_output=True):
            yield f"http://127.0.0.1:{proxy_port}/events"


def rows(path, query):
    with closing(sqlite3.connect(path)) as db:
        return db.execute(query).fetchall()


def main():
    sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)
    folder = Path(tempfile.mkdtemp(prefix="delivery-lab-"))
    sender_db, receiver_db = folder / "sender.sqlite3", folder / "receiver.sqlite3"
    initialize(sender_db)
    record(sender_db, "pedido-42", {"valor": 150, "moeda": "BRL"})
    print("emissor: evento pedido-42 gravado na outbox")

    with receiver_behind_lossy_proxy(receiver_db) as url, httpx.Client() as client:
        print("1ª tentativa:", dispatch(sender_db, client, url))
        print("2ª tentativa:", dispatch(sender_db, client, url))

    print("\noutbox do emissor:    ", rows(sender_db, "SELECT id, status FROM outbox"))
    print("tentativas do emissor:", rows(sender_db, "SELECT outcome, detail FROM attempts"))
    print("eventos no receptor:  ", rows(receiver_db, "SELECT id FROM events"))
    print(f"\nbancos em {folder}")


if __name__ == "__main__":
    main()
