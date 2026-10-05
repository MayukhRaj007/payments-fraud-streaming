"""Tests for the producer's labelled anomaly injection.

These run without Kafka: the injectors are pure functions over a seeded Random,
which is the whole reason they live in their own module.
"""

from __future__ import annotations

import random

import pytest
from anomalies import (
    RULE_AMOUNT,
    RULE_IMPOSSIBLE_TRAVEL,
    RULE_VELOCITY,
    impossible_travel,
    inject,
    large_amount,
    velocity_burst,
)
from profiles import MERCHANT_CATEGORIES, build_cards, make_transaction, sample_amount

CFG = {
    "velocity_max_txns": 5,
    "velocity_burst_spread_sec": 8.0,
    "amount_threshold_cad": 5000.0,
    "travel_gap_sec": 90.0,
}


@pytest.fixture
def rng():
    return random.Random(1234)


@pytest.fixture
def card(rng):
    return build_cards(rng, 10)[0]


# --------------------------------------------------------------- velocity ----
def test_velocity_burst_exceeds_the_window_limit(rng, card):
    """A burst must contain one more transaction than the rule tolerates."""
    out = velocity_burst(rng, card, n_txns=6, spread_sec=8.0, max_txns_in_window=5)
    assert len(out) == 6
    assert {s.txn["card_id"] for s in out} == {card.card_id}


def test_velocity_burst_fits_inside_the_detection_window(rng, card):
    """All transactions must land inside the 60s window, or the rule cannot see
    them as one burst."""
    out = velocity_burst(rng, card, n_txns=6, spread_sec=8.0, max_txns_in_window=5)
    delays = [s.delay_sec for s in out]
    assert delays == sorted(delays), "burst must be emitted in order"
    assert delays[0] == 0.0
    assert max(delays) <= 8.0
    assert max(delays) < 60.0


def test_velocity_burst_labels_only_the_threshold_crossing_txn(rng, card):
    """Exactly one label per injected anomaly: labelling all six would count a
    single detection as six and inflate recall."""
    out = velocity_burst(rng, card, n_txns=6, spread_sec=8.0, max_txns_in_window=5)
    labelled = [i for i, s in enumerate(out) if s.label is not None]
    assert labelled == [5], "the 6th txn (index 5) crosses a limit of 5"
    assert out[5].label["rule"] == RULE_VELOCITY
    assert out[5].label["txn_id"] == out[5].txn["txn_id"]


def test_velocity_burst_scales_with_the_configured_limit(rng, card):
    """Raising the rule's limit must move the labelled transaction with it."""
    out = velocity_burst(rng, card, n_txns=11, spread_sec=8.0, max_txns_in_window=10)
    labelled = [i for i, s in enumerate(out) if s.label is not None]
    assert labelled == [10]


# ----------------------------------------------------------------- amount ----
def test_large_amount_clears_the_threshold(rng, card):
    out = large_amount(rng, card, threshold_cad=5000.0)
    assert len(out) == 1
    assert out[0].txn["amount"] > 5000.0
    assert out[0].label["rule"] == RULE_AMOUNT


def test_large_amount_is_never_marginal(rng, card):
    """A 5% floor above the threshold keeps rounding from putting a labelled
    transaction on the wrong side of the detector's comparison."""
    for _ in range(200):
        out = large_amount(rng, card, threshold_cad=5000.0)
        assert out[0].txn["amount"] >= 5000.0 * 1.05


def test_large_amount_is_emitted_immediately(rng, card):
    assert large_amount(rng, card)[0].delay_sec == 0.0


# ----------------------------------------------------- impossible travel ----
def test_impossible_travel_crosses_a_border(rng, card):
    out = impossible_travel(rng, card, gap_sec=90.0)
    assert len(out) == 2
    assert out[0].txn["country"] != out[1].txn["country"]
    assert out[0].txn["card_id"] == out[1].txn["card_id"]


def test_impossible_travel_second_leg_is_inside_the_rule_window(rng, card):
    """90s gap must sit well inside the 600s rule window."""
    out = impossible_travel(rng, card, gap_sec=90.0)
    assert out[0].delay_sec == 0.0
    assert out[1].delay_sec == 90.0
    assert out[1].delay_sec < 600.0


def test_impossible_travel_labels_only_the_revealing_leg(rng, card):
    """Leg 1 is unremarkable on its own; only leg 2 proves impossibility."""
    out = impossible_travel(rng, card, gap_sec=90.0)
    assert out[0].label is None
    assert out[1].label["rule"] == RULE_IMPOSSIBLE_TRAVEL
    assert out[1].label["txn_id"] == out[1].txn["txn_id"]


def test_impossible_travel_is_card_present_abroad(rng, card):
    """An online purchase from another country is normal, so it would be a bad
    label. The foreign leg must be POS to be genuinely suspicious."""
    for _ in range(50):
        out = impossible_travel(rng, card, gap_sec=90.0)
        assert out[1].txn["channel"] == "POS"


# --------------------------------------------------------------- dispatch ----
@pytest.mark.parametrize(
    "kind,expected_txns",
    [(RULE_VELOCITY, 6), (RULE_AMOUNT, 1), (RULE_IMPOSSIBLE_TRAVEL, 2)],
)
def test_inject_dispatches_each_kind(rng, card, kind, expected_txns):
    out = inject(rng, card, kind, CFG)
    assert len(out) == expected_txns
    assert sum(1 for s in out if s.label is not None) == 1, "exactly one label"
    assert all(s.label["rule"] == kind for s in out if s.label)


def test_inject_rejects_unknown_kind(rng, card):
    with pytest.raises(ValueError, match="unknown anomaly kind"):
        inject(rng, card, "NOT_A_RULE", CFG)


# ----------------------------------------------------------- determinism ----
def test_same_seed_reproduces_the_same_anomaly():
    """The README promises reproducible runs, so the seed must fully determine
    generated ids and amounts."""
    def run():
        r = random.Random(99)
        c = build_cards(r, 10)[0]
        return [(s.txn["txn_id"], s.txn["amount"]) for s in inject(r, c, RULE_VELOCITY, CFG)]

    assert run() == run()


def test_different_seeds_diverge():
    def run(seed):
        r = random.Random(seed)
        c = build_cards(r, 10)[0]
        return [s.txn["txn_id"] for s in inject(r, c, RULE_VELOCITY, CFG)]

    assert run(1) != run(2)


# ------------------------------------------------------- amount modelling ----
def test_background_amounts_are_positive(rng, card):
    for _ in range(500):
        assert make_transaction(rng, card)["amount"] > 0


def test_background_traffic_rarely_trips_the_amount_rule(rng, card):
    """The lognormal is tuned so ordinary spend almost never exceeds 5,000 CAD.
    If it did, unlabelled true alerts would depress measured precision and the
    evaluation numbers would be misleading.
    """
    n = 20_000
    over = sum(1 for _ in range(n) if make_transaction(rng, card)["amount"] > 5000.0)
    assert over / n < 0.005, f"{over}/{n} background txns exceeded the threshold"


def test_amount_scales_with_merchant_category(rng):
    """Category multipliers must actually shift the distribution, otherwise the
    simulation is a single flat spend profile wearing different labels."""
    def median_for(category):
        r = random.Random(7)
        return sorted(sample_amount(r, category) for _ in range(2000))[1000]

    assert median_for("travel") > median_for("electronics") > median_for("grocery")
    assert median_for("grocery") > median_for("pharmacy")


def test_every_category_has_a_multiplier(rng, card):
    """make_transaction picks categories freely, so each must be priceable."""
    for category in MERCHANT_CATEGORIES:
        assert make_transaction(rng, card, category=category)["amount"] > 0


def test_transaction_has_the_full_documented_schema(rng, card):
    txn = make_transaction(rng, card)
    # event_time is deliberately absent: the simulator stamps it at emit time.
    assert set(txn) == {
        "txn_id",
        "card_id",
        "customer_id",
        "merchant_id",
        "merchant_category",
        "amount",
        "currency",
        "channel",
        "city",
        "country",
    }
    assert txn["currency"] == "CAD"
    assert txn["channel"] in {"POS", "online", "ATM"}


def test_cards_are_stable_for_a_seed():
    assert [c.card_id for c in build_cards(random.Random(5), 500)] == [
        c.card_id for c in build_cards(random.Random(5), 500)
    ]


def test_customers_can_hold_two_cards(rng):
    """Modelling two cards per customer is what makes customer_id meaningful."""
    cards = build_cards(rng, 10)
    assert cards[0].customer_id == cards[1].customer_id
    assert cards[0].card_id != cards[1].card_id
