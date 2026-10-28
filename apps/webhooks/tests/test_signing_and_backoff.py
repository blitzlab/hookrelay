import random

import pytest

from apps.webhooks.services.backoff import backoff_delay
from apps.webhooks.services.signing import SignatureError, build_signature_header, verify_signature

SECRET = "whsec_test"
BODY = b'{"id":"evt_1","type":"order.created"}'


class TestSigning:
    def test_round_trip(self):
        header = build_signature_header(SECRET, BODY, timestamp=1_700_000_000)
        verify_signature(SECRET, BODY, header, now=1_700_000_010)

    def test_tampered_body_is_rejected(self):
        header = build_signature_header(SECRET, BODY, timestamp=1_700_000_000)
        with pytest.raises(SignatureError, match="mismatch"):
            verify_signature(SECRET, BODY + b" ", header, now=1_700_000_000)

    def test_wrong_secret_is_rejected(self):
        header = build_signature_header(SECRET, BODY, timestamp=1_700_000_000)
        with pytest.raises(SignatureError, match="mismatch"):
            verify_signature("whsec_other", BODY, header, now=1_700_000_000)

    def test_replay_outside_tolerance_is_rejected(self):
        header = build_signature_header(SECRET, BODY, timestamp=1_700_000_000)
        with pytest.raises(SignatureError, match="tolerance"):
            verify_signature(SECRET, BODY, header, now=1_700_000_000 + 301)

    @pytest.mark.parametrize("header", [None, "", "garbage", "t=abc,v1=00", "t=1700000000"])
    def test_malformed_headers_are_rejected(self, header):
        with pytest.raises(SignatureError):
            verify_signature(SECRET, BODY, header, now=1_700_000_000)


class TestBackoff:
    def test_ceiling_grows_exponentially_until_cap(self):
        # With rng.uniform returning its upper bound we observe the ceiling itself.
        class Max(random.Random):
            def uniform(self, a, b):
                return b

        ceilings = [backoff_delay(n, base=2, cap=60, rng=Max()) for n in range(1, 8)]
        assert ceilings == [2, 4, 8, 16, 32, 60, 60]

    def test_full_jitter_stays_within_bounds(self):
        rng = random.Random(42)
        for attempt in range(1, 12):
            delay = backoff_delay(attempt, base=2, cap=60, rng=rng)
            assert 1.0 <= delay <= min(60, 2 * 2 ** (attempt - 1))

    def test_attempt_must_be_positive(self):
        with pytest.raises(ValueError):
            backoff_delay(0, base=2, cap=60)
