"""Static world model for the simulation: cards, merchants, geography, amounts.

Everything here is pure and deterministic given a seeded `random.Random`, so the
tests can assert on generated data without touching Kafka.
"""

from __future__ import annotations

import math
import uuid
from dataclasses import dataclass

# (city, country) pairs. Country codes are ISO-3166 alpha-2; the fraud rules only
# ever compare countries, never cities.
LOCATIONS: list[tuple[str, str]] = [
    ("Toronto", "CA"),
    ("Montreal", "CA"),
    ("Vancouver", "CA"),
    ("Calgary", "CA"),
    ("Ottawa", "CA"),
    ("New York", "US"),
    ("Chicago", "US"),
    ("Seattle", "US"),
    ("London", "GB"),
    ("Paris", "FR"),
    ("Berlin", "DE"),
    ("Tokyo", "JP"),
    ("Sydney", "AU"),
    ("Sao Paulo", "BR"),
    ("Mumbai", "IN"),
    ("Singapore", "SG"),
]

# Canadian locations are the "home" pool: most cards are domestic, which makes a
# sudden foreign transaction genuinely anomalous rather than routine.
HOME_LOCATIONS = [loc for loc in LOCATIONS if loc[1] == "CA"]
FOREIGN_LOCATIONS = [loc for loc in LOCATIONS if loc[1] != "CA"]

# Each category carries a multiplier applied to the base lognormal draw, so a
# grocery run and an electronics purchase do not have the same spend profile.
MERCHANT_CATEGORIES: dict[str, float] = {
    "grocery": 1.0,
    "restaurant": 0.7,
    "fuel": 1.1,
    "pharmacy": 0.5,
    "clothing": 1.4,
    "entertainment": 0.8,
    "hardware": 1.6,
    "electronics": 3.0,
    "travel": 5.0,
    "online_services": 0.6,
}

CHANNELS = ["POS", "online", "ATM"]
CHANNEL_WEIGHTS = [0.62, 0.31, 0.07]

CURRENCY = "CAD"

# Base lognormal parameters, chosen so the median transaction is ~CAD 45 with a
# realistic long right tail. At sigma=1.1 the 5,000 threshold sits ~3.9 sigma
# out, so ordinary traffic essentially never trips the amount rule by accident --
# which keeps injected labels the only source of truth for evaluation.
AMOUNT_LOG_MU = math.log(45.0)
AMOUNT_LOG_SIGMA = 1.1


@dataclass(frozen=True)
class Card:
    """A payment card and the customer/home location it normally transacts from."""

    card_id: str
    customer_id: str
    home_city: str
    home_country: str


def build_cards(rng, n_cards: int) -> list[Card]:
    """Create a stable roster of cards. Same seed and n_cards => same roster."""
    cards = []
    for i in range(n_cards):
        city, country = rng.choice(HOME_LOCATIONS)
        cards.append(
            Card(
                card_id=f"card_{i:05d}",
                customer_id=f"cust_{i // 2:05d}",  # some customers hold two cards
                home_city=city,
                home_country=country,
            )
        )
    return cards


def sample_amount(rng, category: str) -> float:
    """Lognormal draw scaled by merchant category, rounded to cents."""
    base = rng.lognormvariate(AMOUNT_LOG_MU, AMOUNT_LOG_SIGMA)
    return round(base * MERCHANT_CATEGORIES[category], 2)


def make_transaction(
    rng,
    card: Card,
    *,
    amount: float | None = None,
    city: str | None = None,
    country: str | None = None,
    category: str | None = None,
    channel: str | None = None,
) -> dict:
    """Build one transaction dict. `event_time` is deliberately absent -- the
    simulator stamps it at the moment the event is actually emitted, so event
    time always advances with wall-clock time.

    Any field can be overridden, which is how the anomaly injectors force a
    large amount or a foreign country without duplicating this construction.
    """
    category = category or rng.choice(list(MERCHANT_CATEGORIES))
    if city is None or country is None:
        city, country = card.home_city, card.home_country
    return {
        "txn_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
        "card_id": card.card_id,
        "customer_id": card.customer_id,
        # Merchant id is derived from the category so dashboards can group on it.
        "merchant_id": f"m_{category[:4]}_{rng.randint(1, 120):03d}",
        "merchant_category": category,
        "amount": sample_amount(rng, category) if amount is None else round(amount, 2),
        "currency": CURRENCY,
        "channel": channel or rng.choices(CHANNELS, weights=CHANNEL_WEIGHTS, k=1)[0],
        "city": city,
        "country": country,
    }
