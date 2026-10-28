import fakeredis
import pytest

from apps.webhooks.services.circuit_breaker import CircuitBreaker


class Clock:
    def __init__(self, now: float = 1_000.0):
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture
def clock():
    return Clock()


@pytest.fixture
def breaker(clock):
    return CircuitBreaker(
        "endpoint:test",
        client=fakeredis.FakeRedis(decode_responses=True),
        threshold=3,
        window=60,
        cooldown=30,
        max_cooldown=120,
        clock=clock,
    )


def trip(breaker):
    for _ in range(breaker.threshold):
        breaker.record_failure()


def test_closed_breaker_allows_requests(breaker):
    assert breaker.state() == CircuitBreaker.CLOSED
    assert breaker.allow_request() == (True, 0.0)


def test_failures_below_threshold_keep_it_closed(breaker):
    breaker.record_failure()
    breaker.record_failure()
    assert breaker.state() == CircuitBreaker.CLOSED


def test_trips_open_at_threshold_and_reports_retry_after(breaker, clock):
    trip(breaker)
    assert breaker.state() == CircuitBreaker.OPEN
    clock.advance(10)
    allowed, retry_after = breaker.allow_request()
    assert allowed is False
    assert retry_after == pytest.approx(20)


def test_success_resets_failure_count(breaker):
    breaker.record_failure()
    breaker.record_failure()
    breaker.record_success()
    breaker.record_failure()
    assert breaker.state() == CircuitBreaker.CLOSED


def test_half_open_admits_exactly_one_probe(breaker, clock):
    trip(breaker)
    clock.advance(31)
    assert breaker.state() == CircuitBreaker.HALF_OPEN
    assert breaker.allow_request()[0] is True
    # Every other worker is turned away while the probe is in flight.
    assert breaker.allow_request()[0] is False


def test_successful_probe_closes_the_circuit(breaker, clock):
    trip(breaker)
    clock.advance(31)
    breaker.allow_request()
    breaker.record_success()
    assert breaker.state() == CircuitBreaker.CLOSED


def test_failed_probe_reopens_with_doubled_cooldown(breaker, clock):
    trip(breaker)
    clock.advance(31)
    breaker.allow_request()
    breaker.record_failure()

    assert breaker.state() == CircuitBreaker.OPEN
    _, retry_after = breaker.allow_request()
    assert retry_after == pytest.approx(60)

    clock.advance(61)
    breaker.allow_request()
    breaker.record_failure()
    clock.advance(1)
    assert breaker.allow_request()[1] == pytest.approx(119)  # capped at max_cooldown


def test_late_failures_while_open_do_not_extend_cooldown(breaker, clock):
    trip(breaker)
    breaker.record_failure()
    breaker.record_failure()
    clock.advance(31)
    assert breaker.state() == CircuitBreaker.HALF_OPEN
