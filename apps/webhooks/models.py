import secrets
import uuid

from django.conf import settings
from django.db import models
from django.utils import timezone


def generate_secret() -> str:
    return f"whsec_{secrets.token_urlsafe(32)}"


def default_max_attempts() -> int:
    return settings.HOOKRELAY["MAX_ATTEMPTS"]


class Endpoint(models.Model):
    """A subscriber URL that receives signed webhook requests."""

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    url = models.URLField(max_length=2048)
    description = models.CharField(max_length=255, blank=True)
    secret = models.CharField(max_length=128, default=generate_secret)
    event_types = models.JSONField(
        default=list,
        blank=True,
        help_text="Event types this endpoint receives. An empty list subscribes to everything.",
    )
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-created_at"]

    def __str__(self) -> str:
        return self.url

    def subscribes_to(self, event_type: str) -> bool:
        return not self.event_types or event_type in self.event_types


class Event(models.Model):
    """An immutable fact published by a producer, fanned out to every subscriber."""

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    event_type = models.CharField(max_length=128, db_index=True)
    payload = models.JSONField()
    idempotency_key = models.CharField(max_length=255, null=True, blank=True, unique=True)
    created_at = models.DateTimeField(auto_now_add=True, db_index=True)

    class Meta:
        ordering = ["-created_at"]

    def __str__(self) -> str:
        return f"{self.event_type} {self.id}"


class OutboxMessage(models.Model):
    """Transactional outbox row, written in the same transaction as its Event.

    Producers never talk to the broker directly. The relay drains this table, so
    an event is either fully recorded and eventually delivered, or not recorded
    at all. There is no window where the database and the queue disagree.
    """

    event = models.OneToOneField(Event, on_delete=models.CASCADE, related_name="outbox_message")
    created_at = models.DateTimeField(auto_now_add=True)
    published_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ["id"]
        indexes = [
            # Partial index: the relay only ever scans the small unpublished tail.
            models.Index(
                fields=["id"],
                name="outbox_unpublished_idx",
                condition=models.Q(published_at__isnull=True),
            ),
        ]

    def __str__(self) -> str:
        return f"outbox#{self.pk} -> {self.event_id}"


class Delivery(models.Model):
    """The state machine for one event going to one endpoint."""

    class Status(models.TextChoices):
        PENDING = "pending", "Pending"
        IN_FLIGHT = "in_flight", "In flight"
        SUCCEEDED = "succeeded", "Succeeded"
        DEAD = "dead", "Dead (retries exhausted)"

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    event = models.ForeignKey(Event, on_delete=models.CASCADE, related_name="deliveries")
    endpoint = models.ForeignKey(Endpoint, on_delete=models.CASCADE, related_name="deliveries")
    status = models.CharField(max_length=16, choices=Status.choices, default=Status.PENDING)
    attempt_count = models.PositiveIntegerField(default=0)
    max_attempts = models.PositiveIntegerField(default=default_max_attempts)
    next_attempt_at = models.DateTimeField(default=timezone.now)
    # Lease held by the worker currently sending this delivery. If the worker
    # dies mid-request the lease expires and the sweeper hands the job to another.
    locked_until = models.DateTimeField(null=True, blank=True)
    last_status_code = models.PositiveSmallIntegerField(null=True, blank=True)
    last_error = models.TextField(blank=True)
    delivered_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-created_at"]
        verbose_name_plural = "deliveries"
        constraints = [
            # Fan-out is idempotent: replaying the outbox can never double-create.
            models.UniqueConstraint(fields=["event", "endpoint"], name="uniq_delivery_per_event_endpoint"),
        ]
        indexes = [
            models.Index(fields=["status", "next_attempt_at"], name="delivery_due_idx"),
        ]

    def __str__(self) -> str:
        return f"delivery {self.id} [{self.status}]"


class DeliveryAttempt(models.Model):
    """Append-only log of every HTTP request made for a delivery."""

    delivery = models.ForeignKey(Delivery, on_delete=models.CASCADE, related_name="attempts")
    attempt_number = models.PositiveIntegerField()
    status_code = models.PositiveSmallIntegerField(null=True, blank=True)
    duration_ms = models.PositiveIntegerField(null=True, blank=True)
    error = models.TextField(blank=True)
    response_body = models.TextField(blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["attempt_number"]
        constraints = [
            models.UniqueConstraint(fields=["delivery", "attempt_number"], name="uniq_attempt_number"),
        ]

    def __str__(self) -> str:
        return f"attempt {self.attempt_number} of {self.delivery_id}"
