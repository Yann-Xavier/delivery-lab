import sqlite3
import subprocess
import sys
from contextlib import closing
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

from demo import receiver_behind_lossy_proxy
from receiver import app
from sender import dispatch, initialize, record

HERE = Path(__file__).resolve().parent
URL = "http://receiver/events"


class ForwardToReceiver(httpx.BaseTransport):
    """Entrega a requisição ao receptor e descarta as primeiras `losses` respostas."""

    def __init__(self, receiver, losses=0):
        self.receiver = receiver
        self.losses = losses

    def handle_request(self, request):
        response = self.receiver.request(
            request.method, request.url.path, content=request.content, headers=request.headers
        )
        if self.losses:
            self.losses -= 1
            raise httpx.ReadTimeout("resposta perdida após o processamento", request=request)
        return httpx.Response(response.status_code, headers=response.headers, content=response.content)


class AlwaysFails(httpx.BaseTransport):
    def __init__(self, status=None):
        self.status = status

    def handle_request(self, request):
        if self.status is None:
            raise httpx.ConnectError("receptor fora do ar", request=request)
        return httpx.Response(self.status)


def rows(path, query):
    with closing(sqlite3.connect(path)) as db:
        return db.execute(query).fetchall()


@pytest.fixture
def dbs(tmp_path, monkeypatch):
    sender_db, receiver_db = tmp_path / "sender.sqlite3", tmp_path / "receiver.sqlite3"
    monkeypatch.setenv("RECEIVER_DB", str(receiver_db))
    initialize(sender_db)
    return sender_db, receiver_db


@pytest.fixture
def receiver():
    with TestClient(app) as receiver_app:
        with httpx.Client(transport=ForwardToReceiver(receiver_app)) as client:
            yield client


@pytest.fixture
def lossy():
    """Cliente cuja primeira resposta se perde depois de o receptor processar."""
    with TestClient(app) as receiver_app:
        with httpx.Client(transport=ForwardToReceiver(receiver_app, losses=1)) as client:
            yield client


def test_lost_response_is_resent_and_applied_once(dbs, lossy):
    sender_db, receiver_db = dbs
    record(sender_db, "pedido-1", {"valor": 150})
    assert dispatch(sender_db, lossy, URL) == [("pedido-1", "transport_error")]
    assert dispatch(sender_db, lossy, URL) == [("pedido-1", "delivered")]
    assert rows(receiver_db, "SELECT id FROM events") == [("pedido-1",)]
    assert rows(sender_db, "SELECT outcome, detail FROM attempts") == [
        ("transport_error", "ReadTimeout"),
        ("delivered", "200"),
    ]


def test_receiver_down_keeps_event_pending_until_it_returns(dbs, receiver):
    sender_db, receiver_db = dbs
    record(sender_db, "pedido-1", {"valor": 150})
    with httpx.Client(transport=AlwaysFails()) as down:
        assert dispatch(sender_db, down, URL) == [("pedido-1", "transport_error")]
    assert rows(sender_db, "SELECT status FROM outbox") == [("pending",)]
    assert dispatch(sender_db, receiver, URL) == [("pedido-1", "delivered")]
    assert rows(receiver_db, "SELECT id FROM events") == [("pedido-1",)]


def test_server_error_is_retried_and_client_error_is_not(dbs):
    sender_db, _ = dbs
    record(sender_db, "pedido-1", {"valor": 150})
    with httpx.Client(transport=AlwaysFails(503)) as client:
        assert dispatch(sender_db, client, URL) == [("pedido-1", "server_error")]
    with httpx.Client(transport=AlwaysFails(400)) as client:
        assert dispatch(sender_db, client, URL) == [("pedido-1", "rejected")]
        assert dispatch(sender_db, client, URL) == []
    assert rows(sender_db, "SELECT status FROM outbox") == [("rejected",)]


def test_same_id_with_different_payload_is_a_conflict(dbs, receiver, tmp_path):
    sender_db, receiver_db = dbs
    record(sender_db, "pedido-1", {"valor": 150})
    dispatch(sender_db, receiver, URL)

    # Outro emissor (ou um bug) reutiliza o ID com outro conteúdo.
    other_db = tmp_path / "other-sender.sqlite3"
    initialize(other_db)
    record(other_db, "pedido-1", {"valor": 999})
    assert dispatch(other_db, receiver, URL) == [("pedido-1", "conflict")]
    assert dispatch(other_db, receiver, URL) == []
    assert rows(receiver_db, "SELECT payload FROM events") == [('{"valor": 150}',)]


def test_sender_crash_after_send_resends_without_duplicate(dbs, receiver):
    sender_db, receiver_db = dbs
    record(sender_db, "pedido-1", {"valor": 150})
    # O processo filho envia e morre antes de marcar a entrega.
    script = (
        "import os, sys, httpx\n"
        "from fastapi.testclient import TestClient\n"
        "from receiver import app\n"
        "from sender import dispatch\n"
        "from test_delivery import URL, ForwardToReceiver\n"
        "class CrashAfterSend:\n"
        "    def __init__(self, client): self.client = client\n"
        "    def post(self, *args, **kwargs):\n"
        "        self.client.post(*args, **kwargs)\n"
        "        os._exit(1)\n"
        "client = httpx.Client(transport=ForwardToReceiver(TestClient(app)))\n"
        "dispatch(sys.argv[1], CrashAfterSend(client), URL)\n"
    )
    child = subprocess.run([sys.executable, "-c", script, str(sender_db)], cwd=HERE, check=False)
    assert child.returncode == 1
    assert rows(receiver_db, "SELECT id FROM events") == [("pedido-1",)]
    assert rows(sender_db, "SELECT status FROM outbox") == [("pending",)]
    # A tentativa interrompida não chegou a ser registrada pelo emissor.
    assert rows(sender_db, "SELECT * FROM attempts") == []

    assert dispatch(sender_db, receiver, URL) == [("pedido-1", "delivered")]
    assert rows(receiver_db, "SELECT id FROM events") == [("pedido-1",)]


def test_one_failure_does_not_block_other_events(dbs, lossy):
    sender_db, receiver_db = dbs
    for event_id in ("pedido-1", "pedido-2"):
        record(sender_db, event_id, {"valor": 150})
    assert dispatch(sender_db, lossy, URL) == [
        ("pedido-1", "transport_error"),
        ("pedido-2", "delivered"),
    ]
    assert rows(receiver_db, "SELECT id FROM events ORDER BY id") == [("pedido-1",), ("pedido-2",)]


def test_end_to_end_with_real_processes(tmp_path):
    sender_db, receiver_db = tmp_path / "sender.sqlite3", tmp_path / "receiver.sqlite3"
    initialize(sender_db)
    record(sender_db, "pedido-42", {"valor": 150, "moeda": "BRL"})
    with receiver_behind_lossy_proxy(receiver_db) as url, httpx.Client() as client:
        assert dispatch(sender_db, client, url) == [("pedido-42", "transport_error")]
        assert dispatch(sender_db, client, url) == [("pedido-42", "delivered")]
    assert rows(receiver_db, "SELECT id FROM events") == [("pedido-42",)]
    assert rows(sender_db, "SELECT outcome, detail FROM attempts") == [
        ("transport_error", "RemoteProtocolError"),
        ("delivered", "200"),
    ]
