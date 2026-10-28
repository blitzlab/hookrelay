from rest_framework.routers import DefaultRouter

from .views import DeliveryViewSet, EndpointViewSet, EventViewSet

router = DefaultRouter()
router.register("endpoints", EndpointViewSet, basename="endpoint")
router.register("events", EventViewSet, basename="event")
router.register("deliveries", DeliveryViewSet, basename="delivery")

urlpatterns = router.urls
