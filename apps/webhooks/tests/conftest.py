import fakeredis
import pytest

from apps.webhooks import tasks
from apps.webhooks.models import Endpoint
from apps.webhooks.services import circuit_breaker


@pytest.fixture(autouse=True)
def fake_redis(monkeypatch):
    client = fakeredis.FakeRedis(decode_responses=True)
    monkeypatch.setattr(circuit_breaker, "_client", client)
    return client


class TaskRecorder:
    def __init__(self):
        self.calls = []

    def __call__(self, *args, **kwargs):
        self.calls.append((args, kwargs))


@pytest.fixture(autouse=True)
def enqueued(monkeypatch):
    """Record Celery enqueues instead of talking to a broker."""
    recorders = {"relay": TaskRecorder(), "deliver": TaskRecorder(), "deliver_later": TaskRecorder()}
    monkeypatch.setattr(tasks.relay_outbox, "delay", recorders["relay"])
    monkeypatch.setattr(tasks.deliver, "delay", recorders["deliver"])
    monkeypatch.setattr(tasks.deliver, "apply_async", recorders["deliver_later"])
    return recorders


@pytest.fixture
def hookrelay_settings(settings):
    settings.HOOKRELAY = {
        **settings.HOOKRELAY,
        "API_KEY": "",
        "MAX_ATTEMPTS": 3,
        "BACKOFF_BASE_SECONDS": 2.0,
        "BACKOFF_CAP_SECONDS": 60.0,
        "BREAKER_FAILURE_THRESHOLD": 3,
        "ALLOW_PRIVATE_URLS": True,
    }
    return settings.HOOKRELAY


@pytest.fixture
def endpoint(db, hookrelay_settings):
    return Endpoint.objects.create(url="https://receiver.example.com/webhooks", event_types=["order.created"])
