from django.contrib import admin
from django.urls import include, path

from apps.webhooks.views import healthz

urlpatterns = [
    path("admin/", admin.site.urls),
    path("api/", include("apps.webhooks.urls")),
    path("healthz", healthz, name="healthz"),
]
