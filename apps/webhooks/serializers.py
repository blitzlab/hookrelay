import re

from django.conf import settings
from rest_framework import serializers

from .models import Delivery, DeliveryAttempt, Endpoint, Event
from .services.url_guard import UnsafeURLError, validate_destination_url

EVENT_TYPE_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_.-]{0,127}$")


class EndpointSerializer(serializers.ModelSerializer):
    class Meta:
        model = Endpoint
        fields = ["id", "url", "description", "event_types", "is_active", "secret", "created_at", "updated_at"]
        read_only_fields = ["id", "secret", "created_at", "updated_at"]

    def validate_url(self, value: str) -> str:
        try:
            validate_destination_url(value, allow_private=settings.HOOKRELAY["ALLOW_PRIVATE_URLS"])
        except UnsafeURLError as exc:
            raise serializers.ValidationError(str(exc)) from exc
        return value

    def validate_event_types(self, value):
        if not isinstance(value, list) or not all(isinstance(v, str) and EVENT_TYPE_PATTERN.match(v) for v in value):
            raise serializers.ValidationError("Must be a list of event type names like 'order.created'.")
        return sorted(set(value))


class EventCreateSerializer(serializers.Serializer):
    event_type = serializers.RegexField(EVENT_TYPE_PATTERN)
    payload = serializers.DictField()


class DeliverySummarySerializer(serializers.ModelSerializer):
    class Meta:
        model = Delivery
        fields = ["id", "endpoint", "status", "attempt_count", "next_attempt_at", "last_status_code"]


class EventSerializer(serializers.ModelSerializer):
    deliveries = DeliverySummarySerializer(many=True, read_only=True)

    class Meta:
        model = Event
        fields = ["id", "event_type", "payload", "idempotency_key", "created_at", "deliveries"]


class DeliveryAttemptSerializer(serializers.ModelSerializer):
    class Meta:
        model = DeliveryAttempt
        fields = ["attempt_number", "status_code", "duration_ms", "error", "response_body", "created_at"]


class DeliverySerializer(serializers.ModelSerializer):
    attempts = DeliveryAttemptSerializer(many=True, read_only=True)

    class Meta:
        model = Delivery
        fields = [
            "id",
            "event",
            "endpoint",
            "status",
            "attempt_count",
            "max_attempts",
            "next_attempt_at",
            "last_status_code",
            "last_error",
            "delivered_at",
            "created_at",
            "updated_at",
            "attempts",
        ]
