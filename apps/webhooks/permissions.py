import hmac

from django.conf import settings
from rest_framework.permissions import BasePermission


class ApiKeyPermission(BasePermission):
    """Require `Authorization: Bearer <HOOKRELAY_API_KEY>` when a key is configured."""

    message = "Missing or invalid API key."

    def has_permission(self, request, view) -> bool:
        expected = settings.HOOKRELAY["API_KEY"]
        if not expected:
            return True
        provided = request.headers.get("Authorization", "")
        return hmac.compare_digest(provided.encode(), f"Bearer {expected}".encode())
