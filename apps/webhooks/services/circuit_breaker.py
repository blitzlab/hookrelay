"""Per-endpoint circuit breaker with state shared across workers in Redis.

States
    closed     requests flow; failures are counted in a fixed window
    open       requests are refused until the cooldown expires
    half_open  cooldown expired; exactly one worker may send a probe request

Transitions
    closed    -> open       `threshold` failures within `window` seconds
    half_open -> closed     probe succeeded
    half_open -> open       probe failed; cooldown doubles up to `max_cooldown`

Because every worker reads the same keys, one dead receiver stops consuming
attempts and worker slots fleet-wide within a few requests, instead of each
worker discovering the outage on its own.
"""

from __future__ import annotations

import time
from collections.abc import Callable

import redis
from django.conf import settings

_client: redis.Redis | None = None


def get_redis() -> redis.Redis:
    global _client
    if _client is None:
        _client = redis.Redis.from_url(settings.REDIS_URL, decode_responses=True)
    return _client


class CircuitBreaker:
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

    def __init__(
        self,
        key: str,
        *,
        client: redis.Redis | None = None,
        threshold: int = 5,
        window: int = 60,
        cooldown: float = 30.0,
        max_cooldown: float = 900.0,
        probe_ttl: int = 30,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.key = key
        self.client = client or get_redis()
        self.threshold = threshold
        self.window = window
        self.cooldown = cooldown
        self.max_cooldown = max_cooldown
        self.probe_ttl = probe_ttl
        self.clock = clock

    @classmethod
    def for_endpoint(cls, endpoint) -> CircuitBreaker:
        conf = settings.HOOKRELAY
        return cls(
            f"endpoint:{endpoint.pk}",
            threshold=conf["BREAKER_FAILURE_THRESHOLD"],
            window=conf["BREAKER_WINDOW_SECONDS"],
            cooldown=conf["BREAKER_COOLDOWN_SECONDS"],
            max_cooldown=conf["BREAKER_MAX_COOLDOWN_SECONDS"],
        )

    # Redis keys -----------------------------------------------------------
    @property
    def _failures_key(self) -> str:
        return f"cb:{self.key}:failures"

    @property
    def _open_until_key(self) -> str:
        return f"cb:{self.key}:open_until"

    @property
    def _cooldown_key(self) -> str:
        return f"cb:{self.key}:cooldown"

    @property
    def _probe_key(self) -> str:
        return f"cb:{self.key}:probe"

    # Public API -----------------------------------------------------------
    def state(self) -> str:
        open_until = self.client.get(self._open_until_key)
        if open_until is None:
            return self.CLOSED
        return self.OPEN if float(open_until) > self.clock() else self.HALF_OPEN

    def allow_request(self) -> tuple[bool, float]:
        """Return (allowed, retry_after_seconds)."""
        open_until = self.client.get(self._open_until_key)
        if open_until is None:
            return True, 0.0

        remaining = float(open_until) - self.clock()
        if remaining > 0:
            return False, remaining

        # Half-open: SET NX guarantees a single probe across the whole fleet.
        if self.client.set(self._probe_key, "1", nx=True, ex=self.probe_ttl):
            return True, 0.0
        return False, float(self.probe_ttl)

    def record_success(self) -> None:
        self.client.delete(self._failures_key, self._open_until_key, self._cooldown_key, self._probe_key)

    def record_failure(self) -> None:
        if self.client.exists(self._probe_key):
            self._trip(escalate=True)
            return
        if self.client.exists(self._open_until_key):
            # Already open. Late failures from requests that started before the
            # trip must not extend the cooldown.
            return

        pipe = self.client.pipeline()
        pipe.incr(self._failures_key)
        pipe.ttl(self._failures_key)
        failures, ttl = pipe.execute()
        if ttl == -1:
            self.client.expire(self._failures_key, self.window)
        if failures >= self.threshold:
            self._trip(escalate=False)

    # Internals ------------------------------------------------------------
    def _trip(self, *, escalate: bool) -> None:
        previous = self.client.get(self._cooldown_key)
        if escalate and previous is not None:
            cooldown = min(self.max_cooldown, float(previous) * 2)
        else:
            cooldown = self.cooldown

        pipe = self.client.pipeline()
        pipe.set(self._open_until_key, self.clock() + cooldown)
        pipe.set(self._cooldown_key, cooldown)
        pipe.delete(self._failures_key, self._probe_key)
        pipe.execute()
