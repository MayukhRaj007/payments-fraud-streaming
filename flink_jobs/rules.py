"""Fraud rule logic -- pure, and deliberately free of any Flink import.

This module is the actual detection logic used in production by
`fraud_detector.py`. Keeping it importable without a cluster is the whole point:
`tests/test_rules.py` drives these functions directly, so the tests exercise the
real rules rather than a reimplementation that could pass while the job is
broken.

The Flink job owns *storage* of per-card state (via keyed state) and calls
`evaluate()` once per transaction. Everything here is a plain function over
plain dicts.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime

# Canonical rule identifiers. producer/anomalies.py mirrors these, and
# tests/test_rules.py asserts the two sides agree so they cannot drift.
RULE_VELOCITY = "VELOCITY"
RULE_AMOUNT = "AMOUNT"
RULE_IMPOSSIBLE_TRAVEL = "IMPOSSIBLE_TRAVEL"

SEVERITY_MEDIUM = "MEDIUM"
SEVERITY_HIGH = "HIGH"

# A namespace for deterministic alert ids (see new_alert).
_ALERT_NAMESPACE = uuid.UUID("6f1a9d2e-4c3b-4a7e-9f10-2b8c5d7e3a41")

# Physical presence is only implied when the card itself is at the terminal. An
# online purchase from another country is routine and says nothing about where
# the cardholder is, so it must not drive the travel rule.
CARD_PRESENT_CHANNELS = frozenset({"POS", "ATM"})


def parse_event_time(value: str) -> int:
    """ISO-8601 UTC timestamp -> epoch milliseconds.

    The trailing 'Z' is normalised by hand rather than left to fromisoformat:
    only Python 3.11+ accepts 'Z' directly, and the Flink image ships 3.10. This
    module therefore has to parse the producer's format on both.

    Milliseconds (not seconds) throughout, because Flink watermarks and timers
    are millisecond-based.
    """
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return int(datetime.fromisoformat(value).timestamp() * 1000)


@dataclass
class CardState:
    """Everything the rules remember about one card.

    Held by the Flink job in keyed state. It is intentionally small and
    JSON-serialisable: `to_dict`/`from_dict` move it in and out of Flink's
    ValueState without depending on a custom serialiser.
    """

    # Event-time millis of recent transactions, used by the velocity rule.
    recent_ms: list[int] = field(default_factory=list)
    # Last *card-present* country and when it was seen.
    last_country: str | None = None
    last_country_ms: int | None = None
    # Per-rule time of the last alert raised, for suppression.
    last_alert_ms: dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "recent_ms": list(self.recent_ms),
            "last_country": self.last_country,
            "last_country_ms": self.last_country_ms,
            "last_alert_ms": dict(self.last_alert_ms),
        }

    @classmethod
    def from_dict(cls, raw: dict | None) -> CardState:
        if not raw:
            return cls()
        return cls(
            recent_ms=list(raw.get("recent_ms") or []),
            last_country=raw.get("last_country"),
            last_country_ms=raw.get("last_country_ms"),
            last_alert_ms=dict(raw.get("last_alert_ms") or {}),
        )


def default_config() -> dict:
    """Rule thresholds. The job overrides these from the environment."""
    return {
        "velocity_window_ms": 60_000,
        "velocity_max_txns": 5,
        "amount_threshold_cad": 5000.0,
        "travel_window_ms": 600_000,
    }


def new_alert(txn: dict, rule: str, severity: str, details: dict, detected_at_ms: int) -> dict:
    """Build one alert.

    `alert_id` is a deterministic UUID5 over (txn_id, rule) rather than random.
    That is what makes at-least-once delivery safe: a replayed transaction
    produces a byte-identical alert id, so the Postgres upsert collapses the
    duplicate instead of inserting a second row.
    """
    return {
        "alert_id": str(uuid.uuid5(_ALERT_NAMESPACE, f"{txn['txn_id']}:{rule}")),
        "txn_id": txn["txn_id"],
        "card_id": txn["card_id"],
        "rule": rule,
        "severity": severity,
        "details": details,
        "detected_at_ms": detected_at_ms,
    }


# ----------------------------------------------------------------- pruning ----
def prune_recent(recent_ms: list[int], now_ms: int, window_ms: int) -> list[int]:
    """Drop timestamps that have fallen out of the window.

    Bounds the state: without this, a long-lived card's list would grow for the
    lifetime of the job.
    """
    cutoff = now_ms - window_ms
    return [ts for ts in recent_ms if ts > cutoff]


def is_suppressed(state: CardState, rule: str, now_ms: int, cooldown_ms: int) -> bool:
    """True when this rule already alerted on this card recently.

    One burst should raise one alert, not one per transaction. It also stops a
    genuine echo: after a CA -> AU travel alert, the card's next ordinary home
    transaction is *another* country change inside the window and would
    otherwise alert again.
    """
    if cooldown_ms <= 0:
        return False
    last = state.last_alert_ms.get(rule)
    return last is not None and (now_ms - last) < cooldown_ms


# ------------------------------------------------------------------- rules ----
def check_amount(txn: dict, threshold_cad: float, now_ms: int) -> dict | None:
    """Rule 2: a single transaction above the threshold.

    Stateless, so it carries no suppression -- each large transaction is
    independently suspicious and is labelled individually by the simulator.
    """
    amount = float(txn["amount"])
    if amount <= threshold_cad:
        return None
    # Far above the threshold is a stronger signal than just over it.
    severity = SEVERITY_HIGH if amount >= threshold_cad * 3 else SEVERITY_MEDIUM
    return new_alert(
        txn,
        RULE_AMOUNT,
        severity,
        {
            "amount": round(amount, 2),
            "currency": txn.get("currency", "CAD"),
            "threshold_cad": threshold_cad,
            "merchant_category": txn.get("merchant_category"),
        },
        now_ms,
    )


def check_velocity(
    txn: dict, recent_ms: list[int], now_ms: int, window_ms: int, max_txns: int
) -> dict | None:
    """Rule 1: more than `max_txns` transactions on one card inside the window.

    `recent_ms` must already include the current transaction and be pruned to
    the window. Evaluated as a rolling look-back on every event rather than at
    window boundaries, so the alert fires on the transaction that crosses the
    limit instead of whenever a window happens to close.
    """
    count = len(recent_ms)
    if count <= max_txns:
        return None
    span_ms = (max(recent_ms) - min(recent_ms)) if count > 1 else 0
    # Double the limit means something far more aggressive than a busy shopper.
    severity = SEVERITY_HIGH if count >= max_txns * 2 else SEVERITY_MEDIUM
    return new_alert(
        txn,
        RULE_VELOCITY,
        severity,
        {
            "txn_count": count,
            "window_sec": window_ms // 1000,
            "max_allowed": max_txns,
            "observed_span_sec": round(span_ms / 1000, 2),
        },
        now_ms,
    )


def check_impossible_travel(
    txn: dict,
    last_country: str | None,
    last_country_ms: int | None,
    now_ms: int,
    window_ms: int,
) -> dict | None:
    """Rule 3: the same card used card-present in two countries too close
    together for the cardholder to have travelled between them.

    Only card-present channels count, in both directions: an online purchase
    neither proves presence abroad nor establishes a location to compare with.
    """
    if txn.get("channel") not in CARD_PRESENT_CHANNELS:
        return None
    if last_country is None or last_country_ms is None:
        return None
    if txn["country"] == last_country:
        return None
    gap_ms = now_ms - last_country_ms
    # Negative gap would mean out-of-order data; the window check covers it.
    if not (0 <= gap_ms <= window_ms):
        return None
    # The less time between the two countries, the less plausible it is.
    severity = SEVERITY_HIGH if gap_ms <= window_ms // 2 else SEVERITY_MEDIUM
    return new_alert(
        txn,
        RULE_IMPOSSIBLE_TRAVEL,
        severity,
        {
            "from_country": last_country,
            "to_country": txn["country"],
            "gap_sec": round(gap_ms / 1000, 2),
            "window_sec": window_ms // 1000,
            "to_city": txn.get("city"),
            "channel": txn.get("channel"),
        },
        now_ms,
    )


# ---------------------------------------------------------------- evaluate ----
def evaluate(txn: dict, state: CardState, cfg: dict) -> tuple[list[dict], CardState]:
    """Apply all three rules to one transaction.

    Single entry point, so the Flink job's process function only has to load
    state, call this, store state and emit. Returns the alerts raised and the
    updated state; `state` is mutated in place and also returned for clarity.

    Order matters: velocity needs the current transaction counted, while travel
    must compare against the *previous* location before it is overwritten.
    """
    now_ms = parse_event_time(txn["event_time"])
    alerts: list[dict] = []

    # --- Rule 3 first: it reads the previous location, so it must run before
    # the current transaction updates it.
    travel = check_impossible_travel(
        txn, state.last_country, state.last_country_ms, now_ms, cfg["travel_window_ms"]
    )
    if travel and not is_suppressed(state, RULE_IMPOSSIBLE_TRAVEL, now_ms, cfg["travel_window_ms"]):
        alerts.append(travel)
        state.last_alert_ms[RULE_IMPOSSIBLE_TRAVEL] = now_ms

    # --- Rule 1: count this transaction, then test the rolling window.
    state.recent_ms = prune_recent([*state.recent_ms, now_ms], now_ms, cfg["velocity_window_ms"])
    velocity = check_velocity(
        txn, state.recent_ms, now_ms, cfg["velocity_window_ms"], cfg["velocity_max_txns"]
    )
    if velocity and not is_suppressed(state, RULE_VELOCITY, now_ms, cfg["velocity_window_ms"]):
        alerts.append(velocity)
        state.last_alert_ms[RULE_VELOCITY] = now_ms

    # --- Rule 2: stateless, no suppression.
    amount = check_amount(txn, cfg["amount_threshold_cad"], now_ms)
    if amount:
        alerts.append(amount)

    # --- Finally, record this card-present location for the next transaction.
    if txn.get("channel") in CARD_PRESENT_CHANNELS:
        state.last_country = txn["country"]
        state.last_country_ms = now_ms

    return alerts, state
