"""Score the detector against the simulator's injected labels.

Reads ground truth from data/labels.jsonl (written by the producer) and alerts
from the Postgres fraud_alerts table, then reports precision/recall per rule.

How matching works, and why it is not just txn_id
-------------------------------------------------
The simulator labels exactly one transaction per injected anomaly: the one that
should trip the rule. But the detector does not always fire on that exact
transaction. A velocity burst is interleaved with the card's ordinary traffic,
so the transaction that actually crosses "more than 5 in 60s" can be a
background one arriving mid-burst. Scoring on txn_id alone would count that as
both a false positive and a miss, reporting ~50% precision for a detector that
behaved correctly.

So the primary metric matches on (card_id, rule) within a time tolerance: did we
catch this anomaly, on this card, at about this time. Each label consumes at most
one alert, so one alert cannot cover several labels.

The stricter txn_id-exact score is reported alongside it, because a tolerant
metric chosen by the person being measured deserves a sanity check.

Two scoping rules keep the comparison fair:
  * Only the latest run_id is scored. The seed is fixed, so restarts replay
    identical txn_ids and ground truth would otherwise be double counted.
  * Labels inside the final `--grace-sec` are skipped. Impossible travel's second
    leg lands 90s after the first, so the newest labels have not had time to be
    detected yet and would read as misses.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

try:
    import psycopg2
except ImportError:  # pragma: no cover - surfaced as a clear message, not a stack
    print("psycopg2 is required: pip install psycopg2-binary", file=sys.stderr)
    raise

RULES = ("VELOCITY", "AMOUNT", "IMPOSSIBLE_TRAVEL")


def parse_iso(value: str) -> datetime:
    """ISO-8601 -> aware datetime. Handles the producer's trailing 'Z'."""
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value)


def load_labels(path: Path, run_id: str | None) -> tuple[list[dict], str | None]:
    """Read ground truth, keeping only one run."""
    if not path.exists():
        return [], None
    labels = []
    with path.open() as fh:
        for line in fh:
            line = line.strip()
            if line:
                labels.append(json.loads(line))
    if not labels:
        return [], None

    # Default to the most recent run present in the file.
    if run_id is None:
        run_ids = [lab.get("run_id") for lab in labels if lab.get("run_id")]
        run_id = max(run_ids) if run_ids else None

    if run_id is not None:
        labels = [lab for lab in labels if lab.get("run_id") == run_id]
    for lab in labels:
        lab["_ts"] = parse_iso(lab["event_time"])
    return labels, run_id


def fetch_alerts(conn, start: datetime, end: datetime) -> list[dict]:
    """Alerts whose event time falls in the scored window."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT txn_id::text, card_id, rule, detected_at
            FROM fraud_alerts
            WHERE detected_at BETWEEN %s AND %s
            ORDER BY detected_at
            """,
            (start, end),
        )
        return [
            {"txn_id": r[0], "card_id": r[1], "rule": r[2], "_ts": r[3]} for r in cur.fetchall()
        ]


def match_tolerant(labels: list[dict], alerts: list[dict], tolerance: timedelta) -> dict:
    """Match on (card_id, rule) within `tolerance`, nearest alert first.

    Greedy nearest-first is deliberate: it gives each label its best candidate
    and prevents a single alert from satisfying multiple labels.
    """
    # Index unconsumed alerts by (card_id, rule).
    pool: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for a in alerts:
        pool[(a["card_id"], a["rule"])].append(a)

    consumed: set[int] = set()
    matched_labels: list[dict] = []
    missed_labels: list[dict] = []

    for lab in labels:
        candidates = [
            a
            for a in pool[(lab["card_id"], lab["rule"])]
            if id(a) not in consumed and abs(a["_ts"] - lab["_ts"]) <= tolerance
        ]
        if candidates:
            best = min(candidates, key=lambda a: abs(a["_ts"] - lab["_ts"]))
            consumed.add(id(best))
            matched_labels.append(lab)
        else:
            missed_labels.append(lab)

    unmatched_alerts = [a for a in alerts if id(a) not in consumed]
    return {
        "tp": matched_labels,
        "fn": missed_labels,
        "fp": unmatched_alerts,
    }


def match_exact(labels: list[dict], alerts: list[dict]) -> dict:
    """Strict variant: the alert must name the exact labelled transaction."""
    alert_keys = {(a["txn_id"], a["rule"]) for a in alerts}
    label_keys = {(lab["txn_id"], lab["rule"]) for lab in labels}
    return {
        "tp": [lab for lab in labels if (lab["txn_id"], lab["rule"]) in alert_keys],
        "fn": [lab for lab in labels if (lab["txn_id"], lab["rule"]) not in alert_keys],
        "fp": [a for a in alerts if (a["txn_id"], a["rule"]) not in label_keys],
    }


def score(tp: int, fp: int, fn: int) -> tuple[float, float, float]:
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0
    return precision, recall, f1


def by_rule(items: list[dict]) -> dict[str, int]:
    out = dict.fromkeys(RULES, 0)
    for it in items:
        out[it["rule"]] = out.get(it["rule"], 0) + 1
    return out


def print_table(title: str, result: dict) -> None:
    tp_r, fp_r, fn_r = (by_rule(result[k]) for k in ("tp", "fp", "fn"))

    print(f"\n{title}")
    print("-" * 78)
    print(f"{'rule':<20}{'TP':>7}{'FP':>7}{'FN':>7}{'precision':>12}{'recall':>10}{'F1':>8}")
    print("-" * 78)
    for rule in RULES:
        p, r, f = score(tp_r[rule], fp_r[rule], fn_r[rule])
        print(
            f"{rule:<20}{tp_r[rule]:>7}{fp_r[rule]:>7}{fn_r[rule]:>7}"
            f"{p:>11.1%}{r:>10.1%}{f:>8.2f}"
        )
    print("-" * 78)
    tp, fp, fn = len(result["tp"]), len(result["fp"]), len(result["fn"])
    p, r, f = score(tp, fp, fn)
    print(f"{'OVERALL':<20}{tp:>7}{fp:>7}{fn:>7}{p:>11.1%}{r:>10.1%}{f:>8.2f}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--labels", default=os.environ.get("LABELS_PATH", "data/labels.jsonl"))
    ap.add_argument("--run-id", default=None, help="default: the latest run in the file")
    ap.add_argument(
        "--tolerance-sec",
        type=float,
        default=60.0,
        help="time tolerance for the (card_id, rule) match; defaults to the "
        "velocity window length",
    )
    ap.add_argument(
        "--grace-sec",
        type=float,
        default=120.0,
        help="ignore labels this close to the end of the run; they may not have "
        "been detected yet (impossible travel's second leg lands 90s late)",
    )
    args = ap.parse_args()

    labels, run_id = load_labels(Path(args.labels), args.run_id)
    if not labels:
        print(
            f"No labels found in {args.labels}. Is the producer running?",
            file=sys.stderr,
        )
        return 1

    labels.sort(key=lambda lab: lab["_ts"])
    run_start, run_end = labels[0]["_ts"], labels[-1]["_ts"]

    # Drop labels too recent to have been detected.
    cutoff = run_end - timedelta(seconds=args.grace_sec)
    scored = [lab for lab in labels if lab["_ts"] <= cutoff]
    skipped = len(labels) - len(scored)
    if not scored:
        print(
            f"All {len(labels)} labels fall inside the {args.grace_sec:.0f}s grace "
            "period. Let the producer run a little longer.",
            file=sys.stderr,
        )
        return 1

    tolerance = timedelta(seconds=args.tolerance_sec)
    conn = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=os.environ.get("POSTGRES_DB", "fraud"),
        user=os.environ.get("POSTGRES_USER", "fraud"),
        password=os.environ.get("POSTGRES_PASSWORD", "fraud"),
    )
    try:
        # Widen the alert window by the tolerance so an alert just outside the
        # label range can still match the label it belongs to.
        alerts = fetch_alerts(conn, run_start - tolerance, cutoff + tolerance)
    finally:
        conn.close()

    print("=" * 78)
    print("FRAUD DETECTOR EVALUATION")
    print("=" * 78)
    print(f"run_id             {run_id}")
    print(f"window             {run_start.isoformat()}  ->  {cutoff.isoformat()}")
    print(f"labels scored      {len(scored)}  ({skipped} skipped inside grace period)")
    print(f"alerts in window   {len(alerts)}")
    print(f"match tolerance    +/- {args.tolerance_sec:.0f}s on (card_id, rule)")

    print_table(
        "PRIMARY  -- (card_id, rule) within tolerance", match_tolerant(scored, alerts, tolerance)
    )
    print_table("STRICT   -- exact (txn_id, rule)", match_exact(scored, alerts))

    print(
        "\nNote: the two tables differ because a velocity burst is interleaved with\n"
        "the card's ordinary traffic, so the transaction that actually crosses the\n"
        "threshold is often a background one arriving mid-burst. The primary metric\n"
        "credits catching the anomaly; the strict one requires naming the exact\n"
        "transaction the simulator labelled.\n"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
