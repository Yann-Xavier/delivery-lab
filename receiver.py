"""Receptor de webhooks com deduplicação durável e detecção de conflito."""

import hashlib
import json
import os
import sqlite3
from contextlib import closing

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel

app = FastAPI()


class Event(BaseModel):
    id: str
    payload: dict


def fingerprint(payload):
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode()).hexdigest()


def connect():
    db = sqlite3.connect(os.getenv("RECEIVER_DB", "receiver.sqlite3"), timeout=10)
    db.execute(
        "CREATE TABLE IF NOT EXISTS events "
        "(id TEXT PRIMARY KEY, payload TEXT NOT NULL, fingerprint TEXT NOT NULL)"
    )
    return db


@app.get("/health")
def health():
    return {"ok": True}


@app.post("/events")
def receive(event: Event):
    digest = fingerprint(event.payload)
    with closing(connect()) as db, db:
        inserted = db.execute(
            "INSERT INTO events VALUES (?, ?, ?) ON CONFLICT(id) DO NOTHING",
            (event.id, json.dumps(event.payload, ensure_ascii=False), digest),
        ).rowcount == 1
        if inserted:
            return JSONResponse({"status": "created"}, status_code=201)
        (stored,) = db.execute(
            "SELECT fingerprint FROM events WHERE id = ?", (event.id,)
        ).fetchone()
    if stored == digest:
        return {"status": "duplicate"}
    return JSONResponse({"status": "conflict"}, status_code=409)
