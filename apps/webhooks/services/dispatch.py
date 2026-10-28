"""Outbox relay and delivery engine.

The functions here hold all the delivery logic and know nothing about Celery,
which keeps them easy to test. `apps.webhooks.tasks` is a thin scheduling layer
on top.

Guarantees
    * At-least-once: a delivery leaves the system only as `succeeded` or `dead`.
    * No lost work: every state change is committed to PostgreSQL before the
      broker is involved. The sweeper re-enqueues anything the broker dropped.
    * No concurrent sends of the same delivery: workers take a row lock plus a
      time-bound lease before making the HTTP request.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from datetime import timedelta

import requests
from django.conf import settings
from django.db import transaction
from django.db.models import Q
from django.utils import timezone

from ..models import Delivery, DeliveryAttempt, Endpoint, OutboxMessage
from .backoff import backoff_delay
from .circuit_breaker import CircuitBreaker
from .signing import SIGNATURE_HEADER, build_signature_header

logger = logging.getLogger(__name__)

USER_AGENT = "hookrelay/0.1"
CLOCK_SKEW = timedelta(seconds=1)
MAX_STORED_BODY = 2048


def _conf(name: str):
    return settings.HOOKRELAY[name]


# ---------------------------------------------------------------------------
# Outbox relay
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RelayResult:
    claimed: int
    delivery_ids: list


def fan_out_outbox_batch(batch_size: int | None = None) -> RelayResult:
    """Claim a batch of unpublished outbox rows and create their deliveries.

    SELECT ... FOR UPDATE SKIP LOCKED lets any number of relay workers drain
    the outbox in parallel: each one claims a disjoint batch and nobody blocks.
    Fan-out and the `published_at` update commit together, and the unique
    (event, endpoint) constraint makes a re-run after a crash harmless.
    """
    batch_size = batch_size or _conf("OUTBOX_BATCH_SIZE")

    with transaction.atomic():
        messages = list(
            OutboxMessage.objects.select_for_update(skip_locked=True, of=("self",))
            .filter(published_at__isnull=True)
            .select_related("event")
            .order_by("id")[:batch_size]
        )
        if not messages:
            return RelayResult(claimed=0, delivery_ids=[])

        endpoints = list(Endpoint.objects.filter(is_active=True))
        Delivery.objects.bulk_create(
            [
                Delivery(event=message.event, endpoint=endpoint)
                for message in messages
                for endpoint in endpoints
                if endpoint.subscribes_to(message.event.event_type)
            ],
            ignore_conflicts=True,
        )
        OutboxMessage.objects.filter(pk__in=[m.pk for m in messages]).update(published_at=timezone.now())

        delivery_ids = list(
            Delivery.objects.filter(
                event_id__in=[m.event_id for m in messages],
                status=Delivery.Status.PENDING,
            ).values_list("id", flat=True)
        )

    logger.info("relay claimed=%s deliveries=%s", len(messages), len(delivery_ids))
    return RelayResult(claimed=len(messages), delivery_ids=delivery_ids)


# ---------------------------------------------------------------------------
# Delivery
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class AttemptOutcome:
    SUCCEEDED = "succeeded"
    RETRY = "retry"
    DEAD = "dead"
    DEFERRED = "deferred"  # circuit open; attempt budget not consumed
    SKIPPED = "skipped"  # not due, already done, or another worker owns it

    status: str
    retry_in: float | None = None


def claim_delivery(delivery_id) -> Delivery | None:
    """Take a lease on a due delivery, or return None if it is not ours to send.

    This is what makes the `deliver` task idempotent: the broker may hand the
    same message to two workers (redelivery after a crash, sweeper overlap),
    but only one of them can move the row to `in_flight`.
    """
    now = timezone.now()
    with transaction.atomic():
        delivery = (
            Delivery.objects.select_for_update(skip_locked=True, of=("self",))
            .select_related("event", "endpoint")
            .filter(pk=delivery_id)
            .first()
        )
        if delivery is None:
            return None

        due = delivery.status == Delivery.Status.PENDING and delivery.next_attempt_at <= now + CLOCK_SKEW
        lease_expired = (
            delivery.status == Delivery.Status.IN_FLIGHT
            and delivery.locked_until is not None
            and delivery.locked_until <= now
        )
        if not (due or lease_expired):
            return None

        delivery.status = Delivery.Status.IN_FLIGHT
        delivery.locked_until = now + timedelta(seconds=_conf("DELIVERY_LEASE_SECONDS"))
        delivery.save(update_fields=["status", "locked_until", "updated_at"])
    return delivery


def build_request_body(delivery: Delivery) -> bytes:
    event = delivery.event
    envelope = {
        "id": str(event.id),
        "type": event.event_type,
        "created_at": event.created_at.isoformat(),
        "data": event.payload,
    }
    return json.dumps(envelope, separators=(",", ":"), sort_keys=True).encode()


def _counts_against_breaker(status_code: int | None) -> bool:
    # Network errors, throttling and server errors signal an unhealthy receiver.
    # Other 4xx responses are about the request, not the receiver's health.
    return status_code is None or status_code == 429 or status_code >= 500


def _release(delivery: Delivery, *, delay: float, reason: str) -> None:
    delivery.status = Delivery.Status.PENDING
    delivery.locked_until = None
    delivery.next_attempt_at = timezone.now() + timedelta(seconds=delay)
    delivery.last_error = reason
    delivery.save(update_fields=["status", "locked_until", "next_attempt_at", "last_error", "updated_at"])


def attempt_delivery(
    delivery_id,
    *,
    session: requests.Session | None = None,
    breaker_factory=CircuitBreaker.for_endpoint,
) -> AttemptOutcome:
    delivery = claim_delivery(delivery_id)
    if delivery is None:
        return AttemptOutcome(AttemptOutcome.SKIPPED)

    endpoint = delivery.endpoint
    if not endpoint.is_active:
        delivery.status = Delivery.Status.DEAD
        delivery.locked_until = None
        delivery.last_error = "endpoint disabled"
        delivery.save(update_fields=["status", "locked_until", "last_error", "updated_at"])
        return AttemptOutcome(AttemptOutcome.DEAD)

    breaker = breaker_factory(endpoint)
    allowed, retry_after = breaker.allow_request()
    if not allowed:
        _release(delivery, delay=retry_after, reason="circuit open")
        return AttemptOutcome(AttemptOutcome.DEFERRED, retry_in=retry_after)

    attempt_number = delivery.attempt_count + 1
    body = build_request_body(delivery)
    headers = {
        "Content-Type": "application/json",
        "User-Agent": USER_AGENT,
        # Stable across retries: receivers dedupe on this to get exactly-once effects.
        "Webhook-Id": str(delivery.event_id),
        "Webhook-Delivery": str(delivery.id),
        "Webhook-Attempt": str(attempt_number),
        SIGNATURE_HEADER: build_signature_header(endpoint.secret, body),
    }

    status_code: int | None = None
    error = ""
    response_body = ""
    started = time.monotonic()
    try:
        response = (session or requests).post(
            endpoint.url,
            data=body,
            headers=headers,
            timeout=_conf("REQUEST_TIMEOUT_SECONDS"),
            allow_redirects=False,
        )
        status_code = response.status_code
        response_body = response.text[:MAX_STORED_BODY]
    except requests.RequestException as exc:
        error = f"{type(exc).__name__}: {exc}"[:MAX_STORED_BODY]
    duration_ms = int((time.monotonic() - started) * 1000)

    succeeded = status_code is not None and 200 <= status_code < 300
    gone = status_code == 410

    with transaction.atomic():
        DeliveryAttempt.objects.create(
            delivery=delivery,
            attempt_number=attempt_number,
            status_code=status_code,
            duration_ms=duration_ms,
            error=error,
            response_body=response_body,
        )
        delivery.attempt_count = attempt_number
        delivery.last_status_code = status_code
        delivery.locked_until = None

        if succeeded:
            delivery.status = Delivery.Status.SUCCEEDED
            delivery.delivered_at = timezone.now()
            delivery.last_error = ""
            outcome = AttemptOutcome(AttemptOutcome.SUCCEEDED)
        elif gone or attempt_number >= delivery.max_attempts:
            # 410 Gone is the receiver asking us to stop; anything else has
            # exhausted its retry budget. Both park in the dead-letter queue.
            delivery.status = Delivery.Status.DEAD
            delivery.last_error = error or f"HTTP {status_code}"
            outcome = AttemptOutcome(AttemptOutcome.DEAD)
            if gone:
                Endpoint.objects.filter(pk=endpoint.pk).update(is_active=False)
        else:
            delay = backoff_delay(
                attempt_number,
                base=_conf("BACKOFF_BASE_SECONDS"),
                cap=_conf("BACKOFF_CAP_SECONDS"),
            )
            delivery.status = Delivery.Status.PENDING
            delivery.next_attempt_at = timezone.now() + timedelta(seconds=delay)
            delivery.last_error = error or f"HTTP {status_code}"
            outcome = AttemptOutcome(AttemptOutcome.RETRY, retry_in=delay)

        delivery.save()

    if succeeded:
        breaker.record_success()
    elif _counts_against_breaker(status_code):
        breaker.record_failure()

    logger.info(
        "delivery=%s endpoint=%s attempt=%s status_code=%s outcome=%s duration_ms=%s",
        delivery.id,
        endpoint.pk,
        attempt_number,
        status_code,
        outcome.status,
        duration_ms,
    )
    return outcome


# ---------------------------------------------------------------------------
# Recovery
# ---------------------------------------------------------------------------


def find_stale_deliveries(limit: int = 500) -> list:
    """Deliveries whose broker message was probably lost.

    * pending and overdue by more than the grace period (message dropped by a
      broker restart, or a countdown task lost with its worker)
    * in_flight with an expired lease (worker died mid-request)
    """
    now = timezone.now()
    grace = timedelta(seconds=_conf("SWEEP_GRACE_SECONDS"))
    return list(
        Delivery.objects.filter(
            Q(status=Delivery.Status.PENDING, next_attempt_at__lte=now - grace)
            | Q(status=Delivery.Status.IN_FLIGHT, locked_until__lte=now)
        )
        .order_by("next_attempt_at")
        .values_list("id", flat=True)[:limit]
    )


class ReplayError(Exception):
    pass


def replay_delivery(delivery_id) -> Delivery:
    """Move a dead or succeeded delivery back to pending with a fresh retry budget."""
    with transaction.atomic():
        delivery = Delivery.objects.select_for_update().get(pk=delivery_id)
        if delivery.status not in (Delivery.Status.DEAD, Delivery.Status.SUCCEEDED):
            raise ReplayError(f"cannot replay a delivery in state {delivery.status}")
        delivery.status = Delivery.Status.PENDING
        delivery.max_attempts = delivery.attempt_count + _conf("MAX_ATTEMPTS")
        delivery.next_attempt_at = timezone.now()
        delivery.locked_until = None
        delivery.last_error = ""
        delivery.save(
            update_fields=["status", "max_attempts", "next_attempt_at", "locked_until", "last_error", "updated_at"]
        )
    return delivery
