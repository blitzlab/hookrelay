import pytest
from rest_framework.test import APIClient

from apps.webhooks.models import Delivery, Event, OutboxMessage

pytestmark = pytest.mark.django_db


@pytest.fixture
def api(hookrelay_settings):
    return APIClient()


def publish(api, payload=None, key=None, event_type="order.created"):
    headers = {"HTTP_IDEMPOTENCY_KEY": key} if key else {}
    return api.post(
        "/api/events/",
        {"event_type": event_type, "payload": payload or {"order_id": 42}},
        format="json",
        **headers,
    )


class TestEndpoints:
    def test_create_endpoint_generates_a_signing_secret(self, api):
        response = api.post(
            "/api/endpoints/",
            {"url": "https://receiver.example.com/hooks", "event_types": ["order.created", "order.created"]},
            format="json",
        )
        assert response.status_code == 201
        assert response.data["secret"].startswith("whsec_")
        assert response.data["event_types"] == ["order.created"]

    def test_private_destinations_are_rejected_when_disallowed(self, api, hookrelay_settings):
        hookrelay_settings["ALLOW_PRIVATE_URLS"] = False
        response = api.post("/api/endpoints/", {"url": "http://127.0.0.1:8000/admin"}, format="json")
        assert response.status_code == 400
        assert "non-public" in str(response.data["url"])

    def test_invalid_event_types_are_rejected(self, api):
        response = api.post(
            "/api/endpoints/", {"url": "https://a.example.com", "event_types": ["Bad Type!"]}, format="json"
        )
        assert response.status_code == 400


class TestEventIngestion:
    def test_publish_writes_event_and_outbox_row_atomically(self, api, enqueued):
        response = publish(api)
        assert response.status_code == 201
        event = Event.objects.get(pk=response.data["id"])
        assert OutboxMessage.objects.filter(event=event, published_at__isnull=True).exists()
        assert len(enqueued["relay"].calls) == 1  # relay nudged for low latency

    def test_same_idempotency_key_returns_original_event(self, api, enqueued):
        first = publish(api, key="order-42-created")
        second = publish(api, key="order-42-created")

        assert first.status_code == 201
        assert second.status_code == 200
        assert second["Idempotent-Replayed"] == "true"
        assert second.data["id"] == first.data["id"]
        assert Event.objects.count() == 1
        assert OutboxMessage.objects.count() == 1
        assert len(enqueued["relay"].calls) == 1

    def test_reusing_a_key_with_a_different_body_conflicts(self, api):
        publish(api, payload={"order_id": 1}, key="k1")
        response = publish(api, payload={"order_id": 2}, key="k1")
        assert response.status_code == 409

    def test_ingestion_survives_a_broker_outage(self, api, monkeypatch):
        from apps.webhooks import tasks

        def broker_down(*args, **kwargs):
            raise ConnectionError("redis unavailable")

        monkeypatch.setattr(tasks.relay_outbox, "delay", broker_down)
        response = publish(api)
        assert response.status_code == 201
        assert OutboxMessage.objects.filter(published_at__isnull=True).count() == 1

    def test_rejects_bad_event_type(self, api):
        assert publish(api, event_type="NOT VALID").status_code == 400


class TestAuthAndOps:
    def test_api_key_is_enforced_when_configured(self, api, hookrelay_settings):
        hookrelay_settings["API_KEY"] = "s3cret"
        assert api.get("/api/events/").status_code == 403
        api.credentials(HTTP_AUTHORIZATION="Bearer s3cret")
        assert api.get("/api/events/").status_code == 200

    def test_replay_requeues_a_dead_delivery(self, api, endpoint, enqueued):
        event = Event.objects.create(event_type="order.created", payload={})
        delivery = Delivery.objects.create(
            event=event, endpoint=endpoint, status=Delivery.Status.DEAD, attempt_count=3, max_attempts=3
        )

        response = api.post(f"/api/deliveries/{delivery.pk}/replay/")

        assert response.status_code == 202
        delivery.refresh_from_db()
        assert delivery.status == Delivery.Status.PENDING
        assert delivery.max_attempts == 6
        assert enqueued["deliver"].calls == [((str(delivery.pk),), {})]

    def test_replay_refuses_pending_deliveries(self, api, endpoint):
        event = Event.objects.create(event_type="order.created", payload={})
        delivery = Delivery.objects.create(event=event, endpoint=endpoint)
        assert api.post(f"/api/deliveries/{delivery.pk}/replay/").status_code == 409

    def test_deliveries_can_be_filtered_by_status(self, api, endpoint):
        event = Event.objects.create(event_type="order.created", payload={})
        Delivery.objects.create(event=event, endpoint=endpoint, status=Delivery.Status.DEAD)
        response = api.get("/api/deliveries/?status=dead")
        assert response.data["count"] == 1
        assert api.get("/api/deliveries/?status=succeeded").data["count"] == 0

    def test_healthz(self, client):
        response = client.get("/healthz")
        assert response.status_code == 200
        assert response.json()["checks"] == {"database": "ok", "redis": "ok"}
