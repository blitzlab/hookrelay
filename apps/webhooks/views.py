from django.db import connection
from django.http import JsonResponse
from rest_framework import mixins, status, viewsets
from rest_framework.decorators import action
from rest_framework.response import Response

from .models import Delivery, Endpoint, Event
from .serializers import DeliverySerializer, EndpointSerializer, EventCreateSerializer, EventSerializer
from .services.circuit_breaker import CircuitBreaker, get_redis
from .services.dispatch import ReplayError, replay_delivery
from .services.outbox import IdempotencyConflict, publish_event
from .tasks import deliver, nudge_relay


class EndpointViewSet(viewsets.ModelViewSet):
    queryset = Endpoint.objects.all()
    serializer_class = EndpointSerializer

    @action(detail=True, methods=["get"])
    def circuit(self, request, pk=None):
        endpoint = self.get_object()
        return Response({"endpoint": str(endpoint.pk), "state": CircuitBreaker.for_endpoint(endpoint).state()})


class EventViewSet(mixins.CreateModelMixin, mixins.RetrieveModelMixin, mixins.ListModelMixin, viewsets.GenericViewSet):
    queryset = Event.objects.prefetch_related("deliveries")
    serializer_class = EventSerializer

    def create(self, request, *args, **kwargs):
        incoming = EventCreateSerializer(data=request.data)
        incoming.is_valid(raise_exception=True)
        try:
            event, created = publish_event(
                event_type=incoming.validated_data["event_type"],
                payload=incoming.validated_data["payload"],
                idempotency_key=request.headers.get("Idempotency-Key"),
            )
        except IdempotencyConflict:
            return Response(
                {"detail": "Idempotency-Key was already used with a different request body."},
                status=status.HTTP_409_CONFLICT,
            )

        if created:
            nudge_relay()
            return Response(EventSerializer(event).data, status=status.HTTP_201_CREATED)
        return Response(EventSerializer(event).data, status=status.HTTP_200_OK, headers={"Idempotent-Replayed": "true"})


class DeliveryViewSet(viewsets.ReadOnlyModelViewSet):
    serializer_class = DeliverySerializer

    def get_queryset(self):
        queryset = Delivery.objects.prefetch_related("attempts")
        params = self.request.query_params
        if value := params.get("status"):
            queryset = queryset.filter(status=value)
        if value := params.get("endpoint"):
            queryset = queryset.filter(endpoint_id=value)
        if value := params.get("event"):
            queryset = queryset.filter(event_id=value)
        return queryset

    @action(detail=True, methods=["post"])
    def replay(self, request, pk=None):
        delivery = self.get_object()
        try:
            replay_delivery(delivery.pk)
        except ReplayError as exc:
            return Response({"detail": str(exc)}, status=status.HTTP_409_CONFLICT)
        deliver.delay(str(delivery.pk))
        return Response({"id": str(delivery.pk), "status": Delivery.Status.PENDING}, status=status.HTTP_202_ACCEPTED)


def healthz(request):
    checks = {}
    try:
        connection.ensure_connection()
        checks["database"] = "ok"
    except Exception as exc:  # noqa: BLE001
        checks["database"] = f"error: {exc.__class__.__name__}"
    try:
        get_redis().ping()
        checks["redis"] = "ok"
    except Exception as exc:  # noqa: BLE001
        checks["redis"] = f"error: {exc.__class__.__name__}"

    healthy = all(v == "ok" for v in checks.values())
    return JsonResponse({"status": "ok" if healthy else "degraded", "checks": checks}, status=200 if healthy else 503)
