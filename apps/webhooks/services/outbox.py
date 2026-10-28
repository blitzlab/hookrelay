"""Event ingestion: idempotent writes into the transactional outbox."""

from django.db import IntegrityError, transaction

from ..models import Event, OutboxMessage


class IdempotencyConflict(Exception):
    """The Idempotency-Key was already used for a different request body."""


def _matches(event: Event, event_type: str, payload: dict) -> bool:
    return event.event_type == event_type and event.payload == payload


def publish_event(*, event_type: str, payload: dict, idempotency_key: str | None = None) -> tuple[Event, bool]:
    """Record an event and its outbox row in one transaction.

    Returns (event, created). Retrying a request with the same Idempotency-Key
    returns the original event instead of publishing a duplicate, so producers
    can safely retry on timeouts.
    """
    key = idempotency_key or None

    if key:
        existing = Event.objects.filter(idempotency_key=key).first()
        if existing:
            if not _matches(existing, event_type, payload):
                raise IdempotencyConflict(key)
            return existing, False

    try:
        with transaction.atomic():
            event = Event.objects.create(event_type=event_type, payload=payload, idempotency_key=key)
            OutboxMessage.objects.create(event=event)
    except IntegrityError:
        if not key:
            raise
        # Lost a race against a concurrent request carrying the same key.
        existing = Event.objects.get(idempotency_key=key)
        if not _matches(existing, event_type, payload):
            raise IdempotencyConflict(key) from None
        return existing, False

    return event, True
