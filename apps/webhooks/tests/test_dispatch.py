import threading
from datetime import timedelta

import pytest
import requests
import responses
from django.db import connection, transaction
from django.utils import timezone

from apps.webhooks import tasks
from apps.webhooks.models import Delivery, DeliveryAttempt, Endpoint, OutboxMessage
from apps.webhooks.services.circuit_breaker import CircuitBreaker
from apps.webhooks.services.dispatch import (
    AttemptOutcome,
    attempt_delivery,
    claim_delivery,
    fan_out_outbox_batch,
    find_stale_deliveries,
)
from apps.webhooks.services.outbox import publish_event
from apps.webhooks.services.signing import SIGNATURE_HEADER, verify_signature

pytestmark = pytest.mark.django_db
URL = "https://receiver.example.com/webhooks"


@pytest.fixture
def delivery(endpoint):
    publish_event(event_type="order.created", payload={"order_id": 7})
    fan_out_outbox_batch()
    return Delivery.objects.get()


class TestOutboxRelay:
    def test_fans_out_only_to_active_subscribed_endpoints(self, endpoint):
        Endpoint.objects.create(url="https://all.example.com")  # subscribes to everything
        Endpoint.objects.create(url="https://other.example.com", event_types=["user.created"])
        Endpoint.objects.create(url="https://off.example.com", is_active=False)

        event, _ = publish_event(event_type="order.created", payload={})
        result = fan_out_outbox_batch()

        assert result.claimed == 1
        assert {d.endpoint.url for d in Delivery.objects.filter(event=event)} == {
            endpoint.url,
            "https://all.example.com",
        }
        assert set(result.delivery_ids) == set(Delivery.objects.values_list("id", flat=True))
        assert not OutboxMessage.objects.filter(published_at__isnull=True).exists()

    def test_rerunning_the_relay_never_duplicates_deliveries(self, endpoint):
        publish_event(event_type="order.created", payload={})
        fan_out_outbox_batch()
        OutboxMessage.objects.update(published_at=None)  # simulate a crash before the mark was durable

        fan_out_outbox_batch()

        assert Delivery.objects.count() == 1

    def test_respects_batch_size_and_keeps_draining(self, endpoint, hookrelay_settings, enqueued):
        hookrelay_settings["OUTBOX_BATCH_SIZE"] = 2
        for i in range(3):
            publish_event(event_type="order.created", payload={"i": i})

        assert tasks.relay_outbox() == 2
        assert len(enqueued["relay"].calls) == 1  # full batch, so it re-queues itself
        assert len(enqueued["deliver"].calls) == 2

        assert tasks.relay_outbox() == 1
        assert len(enqueued["relay"].calls) == 1


@pytest.mark.django_db(transaction=True)
def test_concurrent_relays_skip_rows_locked_by_each_other(hookrelay_settings):
    """Two relay workers draining at once must claim disjoint batches."""
    Endpoint.objects.create(url=URL)
    for i in range(4):
        publish_event(event_type="order.created", payload={"i": i})

    locked = threading.Event()
    release = threading.Event()

    def worker_holding_two_rows():
        try:
            with transaction.atomic():
                list(OutboxMessage.objects.select_for_update().order_by("id")[:2])
                locked.set()
                release.wait(timeout=10)
        finally:
            connection.close()

    holder = threading.Thread(target=worker_holding_two_rows)
    holder.start()
    assert locked.wait(timeout=10)
    try:
        result = fan_out_outbox_batch(batch_size=10)
    finally:
        release.set()
        holder.join()

    assert result.claimed == 2  # took the two unlocked rows without blocking
    assert OutboxMessage.objects.filter(published_at__isnull=True).count() == 2


class TestAttemptDelivery:
    @responses.activate
    def test_success_sends_a_verifiable_signed_request(self, delivery, endpoint):
        responses.post(URL, status=204)

        outcome = attempt_delivery(delivery.pk)

        assert outcome.status == AttemptOutcome.SUCCEEDED
        request = responses.calls[0].request
        verify_signature(endpoint.secret, request.body, request.headers[SIGNATURE_HEADER])
        assert request.headers["Webhook-Id"] == str(delivery.event_id)
        assert request.headers["Webhook-Attempt"] == "1"
        delivery.refresh_from_db()
        assert delivery.status == Delivery.Status.SUCCEEDED
        assert delivery.delivered_at is not None
        assert delivery.attempts.get().status_code == 204

    @responses.activate
    def test_server_error_schedules_a_backoff_retry(self, delivery):
        responses.post(URL, status=503, body="busy")

        outcome = attempt_delivery(delivery.pk)

        assert outcome.status == AttemptOutcome.RETRY
        assert 1.0 <= outcome.retry_in <= 2.0
        delivery.refresh_from_db()
        assert delivery.status == Delivery.Status.PENDING
        assert delivery.attempt_count == 1
        assert delivery.next_attempt_at > timezone.now()
        assert delivery.last_error == "HTTP 503"
        assert delivery.attempts.get().response_body == "busy"

    @responses.activate
    def test_network_errors_are_retried(self, delivery):
        responses.post(URL, body=requests.ConnectionError("connection refused"))

        assert attempt_delivery(delivery.pk).status == AttemptOutcome.RETRY
        delivery.refresh_from_db()
        assert "ConnectionError" in delivery.last_error

    @responses.activate
    def test_exhausted_retries_park_in_dead_letter_queue(self, delivery):
        responses.post(URL, status=500)
        outcomes = []
        for _ in range(3):
            Delivery.objects.filter(pk=delivery.pk).update(next_attempt_at=timezone.now())
            outcomes.append(attempt_delivery(delivery.pk).status)

        assert outcomes == [AttemptOutcome.RETRY, AttemptOutcome.RETRY, AttemptOutcome.DEAD]
        delivery.refresh_from_db()
        assert delivery.status == Delivery.Status.DEAD
        assert list(delivery.attempts.values_list("attempt_number", flat=True)) == [1, 2, 3]

    @responses.activate
    def test_410_gone_kills_delivery_and_disables_endpoint(self, delivery, endpoint):
        responses.post(URL, status=410)

        assert attempt_delivery(delivery.pk).status == AttemptOutcome.DEAD
        endpoint.refresh_from_db()
        assert endpoint.is_active is False

    @responses.activate
    def test_open_circuit_defers_without_spending_an_attempt(self, delivery, endpoint):
        breaker = CircuitBreaker.for_endpoint(endpoint)
        for _ in range(3):
            breaker.record_failure()

        outcome = attempt_delivery(delivery.pk)

        assert outcome.status == AttemptOutcome.DEFERRED
        assert outcome.retry_in > 0
        assert len(responses.calls) == 0
        delivery.refresh_from_db()
        assert delivery.attempt_count == 0
        assert delivery.status == Delivery.Status.PENDING
        assert delivery.last_error == "circuit open"

    @responses.activate
    def test_repeated_failures_trip_the_breaker(self, endpoint, hookrelay_settings):
        hookrelay_settings["MAX_ATTEMPTS"] = 10
        responses.post(URL, status=500)
        for i in range(4):
            publish_event(event_type="order.created", payload={"i": i})
        ids = fan_out_outbox_batch().delivery_ids

        statuses = [attempt_delivery(pk).status for pk in ids]

        assert statuses == [AttemptOutcome.RETRY] * 3 + [AttemptOutcome.DEFERRED]
        assert len(responses.calls) == 3
        assert CircuitBreaker.for_endpoint(endpoint).state() == CircuitBreaker.OPEN

    @responses.activate
    def test_client_errors_do_not_trip_the_breaker(self, endpoint, hookrelay_settings):
        responses.post(URL, status=400)
        for i in range(4):
            publish_event(event_type="order.created", payload={"i": i})
        for pk in fan_out_outbox_batch().delivery_ids:
            attempt_delivery(pk)
        assert CircuitBreaker.for_endpoint(endpoint).state() == CircuitBreaker.CLOSED

    def test_deliver_task_reschedules_retries_with_countdown(self, delivery, enqueued, monkeypatch):
        monkeypatch.setattr(tasks, "attempt_delivery", lambda pk: AttemptOutcome(AttemptOutcome.RETRY, retry_in=8.0))

        assert tasks.deliver(str(delivery.pk)) == AttemptOutcome.RETRY
        assert enqueued["deliver_later"].calls == [((), {"args": [str(delivery.pk)], "countdown": 8.0})]


class TestClaimsAndRecovery:
    def test_not_yet_due_delivery_is_skipped(self, delivery):
        Delivery.objects.filter(pk=delivery.pk).update(next_attempt_at=timezone.now() + timedelta(minutes=5))
        assert attempt_delivery(delivery.pk).status == AttemptOutcome.SKIPPED

    def test_finished_delivery_is_never_resent(self, delivery):
        Delivery.objects.filter(pk=delivery.pk).update(status=Delivery.Status.SUCCEEDED)
        assert claim_delivery(delivery.pk) is None

    def test_live_lease_blocks_a_second_worker(self, delivery):
        assert claim_delivery(delivery.pk) is not None
        assert claim_delivery(delivery.pk) is None

    def test_expired_lease_can_be_reclaimed(self, delivery):
        claim_delivery(delivery.pk)
        Delivery.objects.filter(pk=delivery.pk).update(locked_until=timezone.now() - timedelta(seconds=1))
        assert claim_delivery(delivery.pk) is not None

    def test_sweeper_finds_lost_and_abandoned_deliveries(self, endpoint, hookrelay_settings, enqueued):
        for i in range(4):
            publish_event(event_type="order.created", payload={"i": i})
        lost, abandoned, fresh, running = fan_out_outbox_batch().delivery_ids
        now = timezone.now()
        Delivery.objects.filter(pk=lost).update(next_attempt_at=now - timedelta(minutes=5))
        Delivery.objects.filter(pk=abandoned).update(
            status=Delivery.Status.IN_FLIGHT, locked_until=now - timedelta(seconds=1)
        )
        Delivery.objects.filter(pk=running).update(
            status=Delivery.Status.IN_FLIGHT, locked_until=now + timedelta(minutes=1)
        )

        assert set(find_stale_deliveries()) == {lost, abandoned}
        assert tasks.sweep_stale_deliveries() == 2
        assert {call[0][0] for call in enqueued["deliver"].calls} == {str(lost), str(abandoned)}

    @responses.activate
    def test_every_attempt_is_logged(self, delivery):
        responses.post(URL, status=500)
        attempt_delivery(delivery.pk)
        attempt = DeliveryAttempt.objects.get()
        assert attempt.attempt_number == 1
        assert attempt.status_code == 500
        assert attempt.duration_ms is not None
