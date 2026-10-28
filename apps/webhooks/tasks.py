"""Celery entry points. Thin wrappers that schedule work around services.dispatch."""

import logging

from celery import shared_task
from django.conf import settings

from .services.dispatch import AttemptOutcome, attempt_delivery, fan_out_outbox_batch, find_stale_deliveries

logger = logging.getLogger(__name__)


@shared_task(ignore_result=True)
def relay_outbox() -> int:
    result = fan_out_outbox_batch()
    for delivery_id in result.delivery_ids:
        deliver.delay(str(delivery_id))
    if result.claimed >= settings.HOOKRELAY["OUTBOX_BATCH_SIZE"]:
        # A full batch means there is probably a backlog: keep draining now
        # instead of waiting for the next beat tick.
        relay_outbox.delay()
    return result.claimed


@shared_task(ignore_result=True, acks_late=True)
def deliver(delivery_id: str) -> str:
    outcome = attempt_delivery(delivery_id)
    if outcome.status in (AttemptOutcome.RETRY, AttemptOutcome.DEFERRED):
        deliver.apply_async(args=[delivery_id], countdown=outcome.retry_in)
    return outcome.status


@shared_task(ignore_result=True)
def sweep_stale_deliveries() -> int:
    stale = find_stale_deliveries()
    for delivery_id in stale:
        deliver.delay(str(delivery_id))
    if stale:
        logger.warning("sweeper re-enqueued %s stale deliveries", len(stale))
    return len(stale)


def nudge_relay() -> None:
    """Wake the relay right after a commit for low latency.

    Best effort only: if the broker is down the API still succeeds, because the
    event is already in the outbox and the beat schedule will pick it up.
    """
    try:
        relay_outbox.delay()
    except Exception:  # noqa: BLE001 - broker outages must not fail ingestion
        logger.warning("could not nudge outbox relay; beat will pick the event up", exc_info=True)
