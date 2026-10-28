import random


def backoff_delay(
    attempt: int,
    base: float,
    cap: float,
    floor: float = 1.0,
    rng: random.Random | None = None,
) -> float:
    """Exponential backoff with full jitter.

    delay = uniform(0, min(cap, base * 2 ** (attempt - 1))), clamped to `floor`.

    Jitter spreads retries from many workers over the window instead of having
    them hit a recovering receiver in synchronized waves (thundering herd).
    """
    if attempt < 1:
        raise ValueError("attempt must be >= 1")
    ceiling = min(cap, base * (2 ** (attempt - 1)))
    return max(floor, (rng or random).uniform(0, ceiling))
