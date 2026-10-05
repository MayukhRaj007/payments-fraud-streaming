"""Tests for the fraud rules.

These import `flink_jobs/rules.py` directly -- no Flink cluster, no Kafka. That
is deliberate: the job delegates every decision to `evaluate()`, so these tests
cover the real production logic rather than a parallel implementation.
"""

from __future__ import annotations

import anomalies
import pytest
from rules import (
    RULE_AMOUNT,
    RULE_IMPOSSIBLE_TRAVEL,
    RULE_VELOCITY,
    SEVERITY_HIGH,
    SEVERITY_MEDIUM,
    CardState,
    check_amount,
    check_impossible_travel,
    check_velocity,
    default_config,
    evaluate,
    is_suppressed,
    new_alert,
    parse_event_time,
    prune_recent,
)

BASE_MS = parse_event_time("2026-10-05T12:00:00.000Z")


def ms_to_iso(ms: int) -> str:
    """Inverse of parse_event_time, for building events at precise offsets."""
    from datetime import UTC, datetime

    return datetime.fromtimestamp(ms / 1000, UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def txn(
    *,
    offset_ms: int = 0,
    amount: float = 50.0,
    country: str = "CA",
    channel: str = "POS",
    card_id: str = "card_00001",
    txn_id: str | None = None,
) -> dict:
    """A minimal transaction at BASE_MS + offset_ms."""
    return {
        "txn_id": txn_id or f"txn-{offset_ms}-{amount}-{country}-{channel}",
        "card_id": card_id,
        "customer_id": "cust_00001",
        "merchant_id": "m_groc_001",
        "merchant_category": "grocery",
        "amount": amount,
        "currency": "CAD",
        "channel": channel,
        "city": "Toronto" if country == "CA" else "Elsewhere",
        "country": country,
        "event_time": ms_to_iso(BASE_MS + offset_ms),
    }


# ------------------------------------------------------------ time parsing ----
def test_parse_event_time_handles_the_producer_format():
    assert parse_event_time("1970-01-01T00:00:00.000Z") == 0
    assert parse_event_time("1970-01-01T00:00:01.500Z") == 1500


def test_parse_event_time_is_millisecond_precise():
    """Flink timers and watermarks are millisecond-based; truncating to seconds
    would collapse the 8s burst into indistinguishable timestamps."""
    a = parse_event_time("2026-10-05T12:00:00.001Z")
    b = parse_event_time("2026-10-05T12:00:00.002Z")
    assert b - a == 1


def test_parse_event_time_round_trips_with_the_test_helper():
    assert parse_event_time(ms_to_iso(BASE_MS + 1234)) == BASE_MS + 1234


# ------------------------------------------------------- contract with producer ----
def test_rule_names_match_the_producer():
    """The simulator labels anomalies with its own constants and evaluate.py
    joins on them. If the two sides drift, every label silently stops matching
    and the measured recall would be zero for a working detector."""
    assert anomalies.RULE_VELOCITY == RULE_VELOCITY
    assert anomalies.RULE_AMOUNT == RULE_AMOUNT
    assert anomalies.RULE_IMPOSSIBLE_TRAVEL == RULE_IMPOSSIBLE_TRAVEL


# -------------------------------------------------------------- alert shape ----
def test_alert_id_is_deterministic_per_txn_and_rule():
    """At-least-once delivery relies on this: a replayed transaction must produce
    the identical alert_id so the Postgres upsert collapses the duplicate."""
    t = txn()
    a = new_alert(t, RULE_AMOUNT, SEVERITY_HIGH, {}, BASE_MS)
    b = new_alert(t, RULE_AMOUNT, SEVERITY_HIGH, {}, BASE_MS + 9999)
    assert a["alert_id"] == b["alert_id"], "alert_id must not depend on detection time"


def test_alert_id_differs_across_rules_for_one_txn():
    """One transaction can breach two rules; those are separate alerts."""
    t = txn()
    assert (
        new_alert(t, RULE_AMOUNT, SEVERITY_HIGH, {}, BASE_MS)["alert_id"]
        != new_alert(t, RULE_VELOCITY, SEVERITY_HIGH, {}, BASE_MS)["alert_id"]
    )


def test_alert_carries_the_sink_schema():
    alert = new_alert(txn(), RULE_AMOUNT, SEVERITY_HIGH, {"k": 1}, BASE_MS)
    assert set(alert) == {
        "alert_id",
        "txn_id",
        "card_id",
        "rule",
        "severity",
        "details",
        "detected_at_ms",
    }


# ------------------------------------------------------------------ pruning ----
def test_prune_recent_drops_only_what_left_the_window():
    now = BASE_MS + 60_000
    kept = prune_recent([BASE_MS, BASE_MS + 30_000, now], now, 60_000)
    assert kept == [BASE_MS + 30_000, now], "the boundary timestamp is exclusive"


def test_prune_recent_bounds_state_growth():
    """Unbounded per-card state is how a long-running job dies."""
    stamps = [BASE_MS + i * 1000 for i in range(600)]
    now = stamps[-1]
    assert len(prune_recent(stamps, now, 60_000)) == 60


def test_prune_recent_tolerates_an_empty_history():
    assert prune_recent([], BASE_MS, 60_000) == []


# -------------------------------------------------------------- suppression ----
def test_is_suppressed_only_within_the_cooldown():
    state = CardState(last_alert_ms={RULE_VELOCITY: BASE_MS})
    assert is_suppressed(state, RULE_VELOCITY, BASE_MS + 30_000, 60_000)
    assert not is_suppressed(state, RULE_VELOCITY, BASE_MS + 60_000, 60_000)


def test_is_suppressed_is_per_rule():
    state = CardState(last_alert_ms={RULE_VELOCITY: BASE_MS})
    assert not is_suppressed(state, RULE_IMPOSSIBLE_TRAVEL, BASE_MS + 1, 600_000)


def test_zero_cooldown_disables_suppression():
    state = CardState(last_alert_ms={RULE_AMOUNT: BASE_MS})
    assert not is_suppressed(state, RULE_AMOUNT, BASE_MS, 0)


# ------------------------------------------------------------ rule 2: amount ----
def test_amount_rule_fires_above_the_threshold():
    alert = check_amount(txn(amount=5000.01), 5000.0, BASE_MS)
    assert alert is not None and alert["rule"] == RULE_AMOUNT


def test_amount_rule_is_exclusive_at_the_boundary():
    """Exactly at the threshold is not 'over' it."""
    assert check_amount(txn(amount=5000.0), 5000.0, BASE_MS) is None


def test_amount_rule_ignores_ordinary_spend():
    assert check_amount(txn(amount=49.99), 5000.0, BASE_MS) is None


def test_amount_severity_escalates_with_size():
    assert check_amount(txn(amount=6000.0), 5000.0, BASE_MS)["severity"] == SEVERITY_MEDIUM
    assert check_amount(txn(amount=15_000.0), 5000.0, BASE_MS)["severity"] == SEVERITY_HIGH


def test_amount_details_explain_the_decision():
    details = check_amount(txn(amount=7119.69), 5000.0, BASE_MS)["details"]
    assert details["amount"] == 7119.69
    assert details["threshold_cad"] == 5000.0


# ---------------------------------------------------------- rule 1: velocity ----
def test_velocity_rule_fires_past_the_limit():
    recent = [BASE_MS + i * 1000 for i in range(6)]
    alert = check_velocity(txn(), recent, recent[-1], 60_000, 5)
    assert alert is not None
    assert alert["details"]["txn_count"] == 6


def test_velocity_rule_allows_exactly_the_limit():
    """'More than 5' means 5 is fine -- an off-by-one here fabricates alerts."""
    recent = [BASE_MS + i * 1000 for i in range(5)]
    assert check_velocity(txn(), recent, recent[-1], 60_000, 5) is None


def test_velocity_severity_escalates_with_count():
    six = [BASE_MS + i * 1000 for i in range(6)]
    ten = [BASE_MS + i * 1000 for i in range(10)]
    assert check_velocity(txn(), six, six[-1], 60_000, 5)["severity"] == SEVERITY_MEDIUM
    assert check_velocity(txn(), ten, ten[-1], 60_000, 5)["severity"] == SEVERITY_HIGH


def test_velocity_details_report_the_observed_span():
    recent = [BASE_MS + i * 1600 for i in range(6)]
    details = check_velocity(txn(), recent, recent[-1], 60_000, 5)["details"]
    assert details["observed_span_sec"] == 8.0
    assert details["window_sec"] == 60


# ------------------------------------------------- rule 3: impossible travel ----
def test_travel_rule_fires_on_a_fast_country_change():
    alert = check_impossible_travel(
        txn(offset_ms=90_000, country="AU"), "CA", BASE_MS, BASE_MS + 90_000, 600_000
    )
    assert alert is not None
    assert alert["details"]["from_country"] == "CA"
    assert alert["details"]["to_country"] == "AU"
    assert alert["details"]["gap_sec"] == 90.0


def test_travel_rule_ignores_the_same_country():
    assert (
        check_impossible_travel(
            txn(offset_ms=90_000, country="CA"), "CA", BASE_MS, BASE_MS + 90_000, 600_000
        )
        is None
    )


def test_travel_rule_ignores_a_gap_outside_the_window():
    """Beyond the window the journey is physically possible."""
    late = 600_001
    assert (
        check_impossible_travel(
            txn(offset_ms=late, country="AU"), "CA", BASE_MS, BASE_MS + late, 600_000
        )
        is None
    )


def test_travel_rule_needs_a_prior_location():
    """A card's very first transaction cannot be impossible travel."""
    assert check_impossible_travel(txn(country="AU"), None, None, BASE_MS, 600_000) is None


def test_travel_rule_ignores_online_purchases():
    """Buying from a foreign website does not place the cardholder abroad. This
    is the single biggest false-positive source if you get it wrong."""
    assert (
        check_impossible_travel(
            txn(offset_ms=90_000, country="AU", channel="online"),
            "CA",
            BASE_MS,
            BASE_MS + 90_000,
            600_000,
        )
        is None
    )


def test_travel_rule_accepts_atm_as_card_present():
    assert (
        check_impossible_travel(
            txn(offset_ms=90_000, country="AU", channel="ATM"),
            "CA",
            BASE_MS,
            BASE_MS + 90_000,
            600_000,
        )
        is not None
    )


def test_travel_rule_rejects_out_of_order_events():
    """A negative gap means the data arrived out of order, not that a card
    teleported; it must not be reported as fraud."""
    assert (
        check_impossible_travel(txn(country="AU"), "CA", BASE_MS + 90_000, BASE_MS, 600_000) is None
    )


def test_travel_severity_escalates_as_the_gap_shrinks():
    assert (
        check_impossible_travel(
            txn(offset_ms=60_000, country="AU"), "CA", BASE_MS, BASE_MS + 60_000, 600_000
        )["severity"]
        == SEVERITY_HIGH
    )
    assert (
        check_impossible_travel(
            txn(offset_ms=500_000, country="AU"), "CA", BASE_MS, BASE_MS + 500_000, 600_000
        )["severity"]
        == SEVERITY_MEDIUM
    )


# ------------------------------------------------- evaluate(): the real path ----
@pytest.fixture
def cfg():
    return default_config()


def test_evaluate_is_quiet_on_ordinary_traffic(cfg):
    state = CardState()
    for i in range(20):
        # One transaction every 30s: never more than 2 inside a 60s window.
        alerts, state = evaluate(txn(offset_ms=i * 30_000), state, cfg)
        assert alerts == [], f"false positive on ordinary txn {i}"


def test_evaluate_detects_an_injected_velocity_burst(cfg):
    """Mirrors the simulator exactly: 6 transactions spread over 8s."""
    state = CardState()
    fired = []
    for i in range(6):
        alerts, state = evaluate(txn(offset_ms=int(i * 1600)), state, cfg)
        fired.extend(a["rule"] for a in alerts)
    assert fired == [RULE_VELOCITY], "exactly one velocity alert, on the 6th txn"


def test_evaluate_raises_one_alert_per_burst_not_per_txn(cfg):
    """Suppression is what keeps a 50-transaction burst from paging someone 45
    times. Background traffic continuing inside the window must stay quiet."""
    state = CardState()
    alert_count = 0
    for i in range(20):
        alerts, state = evaluate(txn(offset_ms=int(i * 1000)), state, cfg)
        alert_count += sum(1 for a in alerts if a["rule"] == RULE_VELOCITY)
    assert alert_count == 1


def test_evaluate_can_alert_again_after_the_cooldown(cfg):
    """Suppression must not permanently blind the rule for a card."""
    state = CardState()
    first = []
    for i in range(6):
        alerts, state = evaluate(txn(offset_ms=int(i * 1600)), state, cfg)
        first.extend(a["rule"] for a in alerts)
    assert first == [RULE_VELOCITY]

    # A second burst, well after both the window and the cooldown have elapsed.
    second = []
    for i in range(6):
        alerts, state = evaluate(txn(offset_ms=200_000 + int(i * 1600)), state, cfg)
        second.extend(a["rule"] for a in alerts)
    assert second == [RULE_VELOCITY]


def test_evaluate_detects_injected_impossible_travel(cfg):
    """Mirrors the simulator: POS at home, then POS abroad 90s later."""
    state = CardState()
    alerts, state = evaluate(txn(country="CA", channel="POS"), state, cfg)
    assert alerts == []
    alerts, state = evaluate(txn(offset_ms=90_000, country="AU", channel="POS"), state, cfg)
    assert [a["rule"] for a in alerts] == [RULE_IMPOSSIBLE_TRAVEL]


def test_evaluate_suppresses_the_return_home_echo(cfg):
    """After a CA -> AU alert, the card's next ordinary home transaction is
    another country change inside the window. Without suppression that is a
    false positive on every single trip."""
    state = CardState()
    _, state = evaluate(txn(country="CA"), state, cfg)
    alerts, state = evaluate(txn(offset_ms=90_000, country="AU"), state, cfg)
    assert [a["rule"] for a in alerts] == [RULE_IMPOSSIBLE_TRAVEL]

    alerts, state = evaluate(txn(offset_ms=120_000, country="CA"), state, cfg)
    assert alerts == [], "the trip home must not raise a second alert"


def test_evaluate_detects_an_injected_large_amount(cfg):
    alerts, _ = evaluate(txn(amount=7119.69), CardState(), cfg)
    assert [a["rule"] for a in alerts] == [RULE_AMOUNT]


def test_evaluate_can_raise_two_rules_on_one_transaction(cfg):
    """A large amount that also completes a burst is two distinct findings."""
    state = CardState()
    for i in range(5):
        _, state = evaluate(txn(offset_ms=int(i * 1000)), state, cfg)
    alerts, _ = evaluate(txn(offset_ms=5000, amount=9999.0), state, cfg)
    assert {a["rule"] for a in alerts} == {RULE_VELOCITY, RULE_AMOUNT}


def test_evaluate_only_records_location_for_card_present(cfg):
    """An online purchase abroad must not become the reference location, or the
    next genuine home transaction looks like impossible travel."""
    state = CardState()
    _, state = evaluate(txn(country="AU", channel="online"), state, cfg)
    assert state.last_country is None
    alerts, _ = evaluate(txn(offset_ms=60_000, country="CA", channel="POS"), state, cfg)
    assert alerts == []


def test_evaluate_keeps_state_bounded(cfg):
    """500 transactions on one card must not leave 500 timestamps in state."""
    state = CardState()
    for i in range(500):
        _, state = evaluate(txn(offset_ms=i * 1000), state, cfg)
    assert len(state.recent_ms) <= 61


def test_evaluate_respects_overridden_thresholds():
    """The job configures these from the environment, so they must be honoured
    rather than read from module defaults."""
    cfg = default_config() | {"amount_threshold_cad": 100.0, "velocity_max_txns": 2}
    alerts, _ = evaluate(txn(amount=150.0), CardState(), cfg)
    assert [a["rule"] for a in alerts] == [RULE_AMOUNT]


# ---------------------------------------------------------- state round-trip ----
def test_card_state_survives_a_serialisation_round_trip():
    """Flink stores this in keyed state as a plain dict; a lossy round trip
    would silently reset a card's history on every event."""
    state = CardState(
        recent_ms=[1, 2, 3],
        last_country="CA",
        last_country_ms=BASE_MS,
        last_alert_ms={RULE_VELOCITY: BASE_MS},
    )
    restored = CardState.from_dict(state.to_dict())
    assert restored == state


def test_card_state_from_empty_state_is_fresh():
    """Flink hands back None for a key it has never seen."""
    assert CardState.from_dict(None) == CardState()
    assert CardState.from_dict({}) == CardState()


def test_card_state_round_trip_preserves_detection_across_events(cfg):
    """The job serialises state between every event, so detection must still
    work when state is rebuilt each time -- not just when it stays in memory."""
    raw = None
    fired = []
    for i in range(6):
        state = CardState.from_dict(raw)
        alerts, state = evaluate(txn(offset_ms=int(i * 1600)), state, cfg)
        raw = state.to_dict()
        fired.extend(a["rule"] for a in alerts)
    assert fired == [RULE_VELOCITY]
