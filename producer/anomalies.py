"""Deliberate, labelled fraud injection.

Each injector returns a list of `ScheduledTxn`: a transaction plus the delay (in
seconds from now) at which the simulator should emit it. Scheduling forward in
time -- rather than backdating event_time -- matters. The Flink job allows only
5s of lateness, so a backdated burst would be discarded as late data and the
detector would appear to miss anomalies it was never actually shown.

A `label` is attached to exactly the transaction that *should* cause the rule to
fire. scripts/evaluate.py scores detector output against these labels, so the
labels are the ground truth and must not be generous: one expected alert per
injected anomaly.

Everything here is pure -- no Kafka, no clock, no I/O -- so tests drive it with a
seeded Random and assert on the output.
"""

from __future__ import annotations

from dataclasses import dataclass

from profiles import FOREIGN_LOCATIONS, Card, make_transaction

# Canonical rule identifiers. These strings must match RULE_* in
# flink_jobs/rules.py -- tests/test_anomalies.py asserts they do, so the two
# sides cannot drift apart silently.
RULE_VELOCITY = "VELOCITY"
RULE_AMOUNT = "AMOUNT"
RULE_IMPOSSIBLE_TRAVEL = "IMPOSSIBLE_TRAVEL"

ANOMALY_KINDS = (RULE_VELOCITY, RULE_AMOUNT, RULE_IMPOSSIBLE_TRAVEL)


@dataclass
class ScheduledTxn:
    """A transaction to emit `delay_sec` from the moment of injection."""

    delay_sec: float
    txn: dict
    label: dict | None = None  # set only on the txn expected to trigger an alert


def _label(txn: dict, rule: str, note: str) -> dict:
    """Ground-truth record for one expected alert."""
    return {
        "txn_id": txn["txn_id"],
        "card_id": txn["card_id"],
        "rule": rule,
        "note": note,
    }


def velocity_burst(
    rng,
    card: Card,
    *,
    n_txns: int = 6,
    spread_sec: float = 8.0,
    max_txns_in_window: int = 5,
) -> list[ScheduledTxn]:
    """Fire `n_txns` on one card inside a few seconds -- card-testing behaviour.

    Spread over `spread_sec` (default 8s), comfortably inside the 60s detection
    window, so the burst is unambiguous even if watermarks lag a little.

    Only the transaction that crosses the threshold is labelled: with a limit of
    5 per window, that is the 6th. Labelling all six would inflate recall by
    counting one real detection as several.
    """
    out: list[ScheduledTxn] = []
    step = spread_sec / max(n_txns - 1, 1)
    for i in range(n_txns):
        txn = make_transaction(rng, card)
        label = None
        if i == max_txns_in_window:  # zero-indexed: the (limit+1)-th transaction
            label = _label(
                txn, RULE_VELOCITY, f"{n_txns} txns within {spread_sec:.0f}s on one card"
            )
        out.append(ScheduledTxn(delay_sec=round(i * step, 3), txn=txn, label=label))
    return out


def large_amount(
    rng, card: Card, *, threshold_cad: float = 5000.0, max_multiple: float = 4.0
) -> list[ScheduledTxn]:
    """A single transaction clearly above the amount threshold.

    Drawn from [1.05x, max_multiple x] the threshold. The 1.05 floor keeps the
    amount far enough above the boundary that float rounding can never put the
    labelled transaction on the wrong side of the comparison.
    """
    amount = rng.uniform(threshold_cad * 1.05, threshold_cad * max_multiple)
    # High-value categories make a large amount plausible rather than absurd.
    txn = make_transaction(
        rng, card, amount=amount, category=rng.choice(["electronics", "travel"])
    )
    return [
        ScheduledTxn(
            delay_sec=0.0,
            txn=txn,
            label=_label(txn, RULE_AMOUNT, f"amount {amount:.2f} > {threshold_cad:.0f} CAD"),
        )
    ]


def impossible_travel(
    rng, card: Card, *, gap_sec: float = 90.0
) -> list[ScheduledTxn]:
    """Same card in two countries too close together to be physically possible.

    Leg 1 at home, leg 2 abroad `gap_sec` later (default 90s -- well inside the
    10 minute rule window, and short enough that a demo run sees the alert).
    Only leg 2 is labelled: it is the event that reveals the impossibility.
    """
    home = make_transaction(rng, card)
    city, country = rng.choice(FOREIGN_LOCATIONS)
    # Card-present abroad is what makes this implausible; an online purchase from
    # another country is perfectly normal and would be a bad label.
    away = make_transaction(rng, card, city=city, country=country, channel="POS")
    return [
        ScheduledTxn(delay_sec=0.0, txn=home),
        ScheduledTxn(
            delay_sec=gap_sec,
            txn=away,
            label=_label(
                away,
                RULE_IMPOSSIBLE_TRAVEL,
                f"{home['country']} -> {country} in {gap_sec:.0f}s",
            ),
        ),
    ]


def inject(rng, card: Card, kind: str, cfg: dict) -> list[ScheduledTxn]:
    """Dispatch to one injector by rule name, using runtime config."""
    if kind == RULE_VELOCITY:
        return velocity_burst(
            rng,
            card,
            n_txns=cfg["velocity_max_txns"] + 1,
            spread_sec=cfg["velocity_burst_spread_sec"],
            max_txns_in_window=cfg["velocity_max_txns"],
        )
    if kind == RULE_AMOUNT:
        return large_amount(rng, card, threshold_cad=cfg["amount_threshold_cad"])
    if kind == RULE_IMPOSSIBLE_TRAVEL:
        return impossible_travel(rng, card, gap_sec=cfg["travel_gap_sec"])
    raise ValueError(f"unknown anomaly kind: {kind}")
