"""HMAC-SHA256 request signing.

Header format (Stripe-style):  Webhook-Signature: t=<unix_ts>,v1=<hex digest>
The digest covers "<timestamp>.<raw body>", so a captured request cannot be
replayed outside the tolerance window or with a modified body.

This module has no Django dependency so receivers can vendor it as-is.
"""

import hashlib
import hmac
import time

SIGNATURE_HEADER = "Webhook-Signature"
SIGNATURE_VERSION = "v1"
DEFAULT_TOLERANCE_SECONDS = 300


class SignatureError(Exception):
    """Raised when a signature header is missing, malformed, stale or wrong."""


def compute_signature(secret: str, timestamp: int, body: bytes) -> str:
    signed_payload = f"{timestamp}.".encode() + body
    return hmac.new(secret.encode(), signed_payload, hashlib.sha256).hexdigest()


def build_signature_header(secret: str, body: bytes, timestamp: int | None = None) -> str:
    ts = int(time.time()) if timestamp is None else timestamp
    return f"t={ts},{SIGNATURE_VERSION}={compute_signature(secret, ts, body)}"


def verify_signature(
    secret: str,
    body: bytes,
    header: str | None,
    tolerance_seconds: int = DEFAULT_TOLERANCE_SECONDS,
    now: int | None = None,
) -> None:
    if not header:
        raise SignatureError("missing signature header")
    try:
        parts = dict(item.split("=", 1) for item in header.split(","))
        timestamp = int(parts["t"])
        received = parts[SIGNATURE_VERSION]
    except (KeyError, ValueError) as exc:
        raise SignatureError("malformed signature header") from exc

    current = int(time.time()) if now is None else now
    if abs(current - timestamp) > tolerance_seconds:
        raise SignatureError("timestamp outside tolerance window")

    expected = compute_signature(secret, timestamp, body)
    if not hmac.compare_digest(expected, received):
        raise SignatureError("signature mismatch")
