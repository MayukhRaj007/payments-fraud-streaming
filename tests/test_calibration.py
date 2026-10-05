"""Guards the relationship between the traffic rate and the velocity threshold.

The velocity rule ("more than N transactions per card in a 60s window") is only
meaningful if ordinary traffic rarely reaches N by chance. Those two settings are
coupled, and getting it wrong does not break the job -- it quietly floods the
alert table with statistically inevitable false positives.

That is not hypothetical. An early run at 20 events/sec across 500 cards gave
each card a mean of 2.4 transactions per 60s window. Over 326k transactions that
produced ~11.9k chance velocity alerts against ~2k genuine injected bursts,
making precision meaningless. Dropping to 5 events/sec is what fixed it.

These tests encode the arithmetic so the defaults cannot silently drift back.
"""

from __future__ import annotations

import math
import random

import pytest
from rules import RULE_VELOCITY, CardState, default_config, evaluate

# Must mirror the defaults in .env.example and docker-compose.yml.
DEFAULT_EVENTS_PER_SEC = 5
DEFAULT_NUM_CARDS = 500
WINDOW_SEC = 60


def mean_txns_per_window(events_per_sec: float, num_cards: int) -> float:
    """Expected transactions for one card in one window."""
    return events_per_sec * WINDOW_SEC / num_cards


def p_chance_velocity_alert(lam: float, limit: int) -> float:
    """Probability one transaction trips the velocity rule by chance.

    Note the boundary. The rule counts the arriving transaction *plus* those in
    the preceding window, so the count is 1 + Poisson(lam) and the rule fires
    when that exceeds `limit` -- i.e. when Poisson(lam) >= limit. Using
    P(X > limit) here understates the rate by roughly an order of magnitude; the
    simulation below is what caught that.

    This is also an upper bound on the observed alert count, because it ignores
    the per-card suppression cooldown, which collapses repeated alerts inside one
    window into one.
    """
    cdf_below = sum(math.exp(-lam) * lam**k / math.factorial(k) for k in range(limit))
    return 1.0 - cdf_below


def test_default_rate_keeps_chance_alerts_rare():
    """At the shipped defaults a card averages well under one transaction per
    window, so reaching 6 by chance is a tail event."""
    cfg = default_config()
    lam = mean_txns_per_window(DEFAULT_EVENTS_PER_SEC, DEFAULT_NUM_CARDS)
    p = p_chance_velocity_alert(lam, cfg["velocity_max_txns"])
    assert lam == pytest.approx(0.6)
    assert p < 1e-3, f"chance false-positive probability {p:.2e} is too high"


def test_the_old_default_really_was_miscalibrated():
    """Documents the bug this file prevents, so the bound above is not mistaken
    for an arbitrary number. 20 events/sec is ~250x worse."""
    cfg = default_config()
    lam_bad = mean_txns_per_window(20, DEFAULT_NUM_CARDS)
    lam_ok = mean_txns_per_window(DEFAULT_EVENTS_PER_SEC, DEFAULT_NUM_CARDS)
    p_bad = p_chance_velocity_alert(lam_bad, cfg["velocity_max_txns"])
    p_ok = p_chance_velocity_alert(lam_ok, cfg["velocity_max_txns"])
    assert lam_bad == pytest.approx(2.4)
    assert p_bad > 1e-2, "20 events/sec across 500 cards should be visibly unsafe"
    assert p_bad / p_ok > 100


def test_simulated_background_traffic_is_quiet():
    """End-to-end check through the real rule logic rather than pure maths.

    Drives evaluate() with Poisson-distributed background traffic for one card at
    the shipped rate. The bound is set from the analytic expectation (~7.6 over
    20k transactions) with headroom for seed variance -- not from whatever number
    happened to come out.
    """
    cfg = default_config()
    rng = random.Random(4242)
    per_card_rate = DEFAULT_EVENTS_PER_SEC / DEFAULT_NUM_CARDS  # txns per second
    n_txns = 20_000  # at this rate, several weeks of traffic for one card

    expected = n_txns * p_chance_velocity_alert(
        mean_txns_per_window(DEFAULT_EVENTS_PER_SEC, DEFAULT_NUM_CARDS),
        cfg["velocity_max_txns"],
    )

    state = CardState()
    now_ms = 1_791_000_000_000
    alerts_seen = 0
    for i in range(n_txns):
        # Exponential gaps reproduce a Poisson arrival process.
        now_ms += int(rng.expovariate(per_card_rate) * 1000)
        alerts, state = evaluate(_txn(f"t{i}", now_ms), state, cfg)
        alerts_seen += sum(1 for a in alerts if a["rule"] == RULE_VELOCITY)

    assert expected < 15, f"analytic expectation {expected:.1f} already too high"
    # Suppression means observed should not exceed the analytic expectation by
    # much; 3x covers seed variance without tolerating a real regression.
    assert alerts_seen <= 3 * expected, (
        f"{alerts_seen} chance velocity alerts in {n_txns} txns " f"(expected ~{expected:.1f})"
    )


def test_velocity_burst_still_detected_at_the_calibrated_rate():
    """Calibration must not be achieved by making the rule insensitive: a real
    8-second burst still has to fire exactly once."""
    cfg = default_config()
    state = CardState()
    base = 1_791_000_000_000
    fired = 0
    for i in range(cfg["velocity_max_txns"] + 1):
        alerts, state = evaluate(_txn(f"burst{i}", base + int(i * 1600)), state, cfg)
        fired += sum(1 for a in alerts if a["rule"] == RULE_VELOCITY)
    assert fired == 1


def _txn(txn_id: str, event_ms: int) -> dict:
    return {
        "txn_id": txn_id,
        "card_id": "card_00001",
        "amount": 50.0,
        "currency": "CAD",
        "channel": "POS",
        "city": "Toronto",
        "country": "CA",
        "event_time": _iso(event_ms),
    }


def _iso(ms: int) -> str:
    from datetime import UTC, datetime

    return datetime.fromtimestamp(ms / 1000, UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
