from django.contrib import admin, messages

from .models import Delivery, DeliveryAttempt, Endpoint, Event, OutboxMessage


@admin.register(Endpoint)
class EndpointAdmin(admin.ModelAdmin):
    list_display = ("url", "is_active", "event_types", "created_at")
    list_filter = ("is_active",)
    search_fields = ("url", "description")
    readonly_fields = ("id", "created_at", "updated_at")


@admin.register(Event)
class EventAdmin(admin.ModelAdmin):
    list_display = ("id", "event_type", "idempotency_key", "created_at")
    list_filter = ("event_type",)
    search_fields = ("id", "idempotency_key")
    readonly_fields = ("id", "created_at")


@admin.register(OutboxMessage)
class OutboxMessageAdmin(admin.ModelAdmin):
    list_display = ("id", "event", "created_at", "published_at")
    list_filter = (("published_at", admin.EmptyFieldListFilter),)


class DeliveryAttemptInline(admin.TabularInline):
    model = DeliveryAttempt
    extra = 0
    can_delete = False
    readonly_fields = ("attempt_number", "status_code", "duration_ms", "error", "response_body", "created_at")


@admin.register(Delivery)
class DeliveryAdmin(admin.ModelAdmin):
    list_display = ("id", "endpoint", "status", "attempt_count", "next_attempt_at", "last_status_code")
    list_filter = ("status",)
    search_fields = ("id", "event__id", "endpoint__url")
    readonly_fields = ("id", "created_at", "updated_at", "delivered_at")
    inlines = [DeliveryAttemptInline]
    actions = ["replay_selected"]

    @admin.action(description="Replay selected dead deliveries")
    def replay_selected(self, request, queryset):
        from .services.dispatch import ReplayError, replay_delivery
        from .tasks import deliver

        replayed = 0
        for delivery in queryset.filter(status=Delivery.Status.DEAD):
            try:
                replay_delivery(delivery.pk)
            except ReplayError:
                continue
            deliver.delay(str(delivery.pk))
            replayed += 1
        self.message_user(request, f"Queued {replayed} deliveries for replay.", messages.SUCCESS)
