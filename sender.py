"""Emissor com Transactional Outbox que entrega eventos por HTTP."""

import json
import sqlite3
from contextlib import closing

import httpx

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (id TEXT PRIMARY KEY, payload TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS outbox (
    id TEXT PRIMARY KEY REFERENCES events (id),
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'delivered', 'conflict', 'rejected'))
);
CREATE TABLE IF NOT EXISTS attempts (
    event_id TEXT NOT NULL REFERENCES events (id),
    outcome TEXT NOT NULL,
    detail TEXT NOT NULL
);
"""

# Resultados que encerram a entrega. Os demais deixam o evento pendente.
FINAL = {"delivered", "conflict", "rejected"}


def initialize(path):
    with closing(sqlite3.connect(path)) as db, db:
        db.executescript(SCHEMA)


def record(path, event_id, payload):
    """Grava o evento e a entrega pendente na mesma transação."""
    with closing(sqlite3.connect(path)) as db, db:
        db.execute(
            "INSERT INTO events VALUES (?, ?)",
            (event_id, json.dumps(payload, ensure_ascii=False)),
        )
        db.execute("INSERT INTO outbox (id) VALUES (?)", (event_id,))


def classify(response):
    if response.status_code in (200, 201):
        return "delivered"
    if response.status_code == 409:
        return "conflict"
    if response.status_code >= 500:
        return "server_error"
    return "rejected"


def dispatch(path, client, url, timeout=2.0):
    """Tenta entregar cada evento pendente uma vez, em ordem de gravação.

    Uma falha não interrompe as demais. Retorna [(id, resultado), ...].
    """
    results = []
    with closing(sqlite3.connect(path)) as db:
        pending = db.execute(
            "SELECT outbox.id, events.payload FROM outbox "
            "JOIN events ON events.id = outbox.id "
            "WHERE outbox.status = 'pending' ORDER BY outbox.rowid"
        ).fetchall()
        for event_id, payload in pending:
            body = {"id": event_id, "payload": json.loads(payload)}
            try:
                response = client.post(url, json=body, timeout=timeout)
                outcome, detail = classify(response), str(response.status_code)
            except httpx.TransportError as error:
                outcome, detail = "transport_error", type(error).__name__
            with db:
                db.execute(
                    "INSERT INTO attempts VALUES (?, ?, ?)", (event_id, outcome, detail)
                )
                if outcome in FINAL:
                    db.execute(
                        "UPDATE outbox SET status = ? WHERE id = ?", (outcome, event_id)
                    )
            results.append((event_id, outcome))
    return results
