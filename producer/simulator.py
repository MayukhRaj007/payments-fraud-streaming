"""Transaction simulator: streams synthetic card payments into Kafka.

Background traffic runs at EVENTS_PER_SEC. A small fraction of events also kick
off a labelled anomaly (velocity burst / large amount / impossible travel) whose
transactions are *scheduled* into the near future and emitted when due.

Why a schedule rather than writing the anomaly out immediately with doctored
timestamps: event_time is stamped at emission, so event time only ever moves
forward with the wall clock. The Flink job tolerates 5s of lateness, and
backdated events would be dropped as late -- making the detector look broken
when it was simply never shown the data.

Ground truth for every injected anomaly is appended to LABELS_PATH as JSON
lines; scripts/evaluate.py scores the detector against that file.
"""

from __future__ import annotations

import heapq
import itertools
import json
import os
import random
import signal
import sys
import time
from datetime import UTC, datetime

from anomalies import ANOMALY_KINDS, ScheduledTxn, inject
from confluent_kafka import Producer
from profiles import build_cards, make_transaction


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def load_config() -> dict:
    """All tunables come from the environment; see .env.example."""
    return {
        "bootstrap": _env("KAFKA_BOOTSTRAP", "kafka:9092"),
        "topic": _env("TOPIC_TRANSACTIONS", "transactions"),
        "events_per_sec": float(_env("EVENTS_PER_SEC", "20")),
        "num_cards": int(_env("NUM_CARDS", "500")),
        "seed": int(_env("SEED", "42")),
        "run_seconds": float(_env("RUN_SECONDS", "0")),  # 0 = run forever
        "anomaly_rate": float(_env("ANOMALY_RATE", "0.02")),
        "labels_path": _env("LABELS_PATH", "/data/labels.jsonl"),
        # Rule parameters the injectors need in order to land either side of
        # the detector's thresholds.
        "velocity_max_txns": int(_env("VELOCITY_MAX_TXNS", "5")),
        "velocity_burst_spread_sec": float(_env("VELOCITY_BURST_SPREAD_SEC", "8")),
        "amount_threshold_cad": float(_env("AMOUNT_THRESHOLD_CAD", "5000")),
        "travel_gap_sec": float(_env("TRAVEL_GAP_SEC", "90")),
    }


def iso_now() -> str:
    """Event time in UTC with millisecond precision, matching the Flink parser."""
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def log(msg: str) -> None:
    print(f"[producer] {msg}", flush=True)


class Simulator:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.rng = random.Random(cfg["seed"])
        self.cards = build_cards(self.rng, cfg["num_cards"])
        self.producer = Producer(
            {
                "bootstrap.servers": cfg["bootstrap"],
                "linger.ms": 20,  # small batching window; keeps latency low
                "acks": "all",
                "enable.idempotence": True,  # no duplicates from internal retries
                "client.id": "txn-simulator",
            }
        )
        # Min-heap of (due_monotonic, tiebreak, ScheduledTxn). The tiebreak
        # counter keeps ordering stable for identical due times, since
        # ScheduledTxn itself is not comparable.
        self.pending: list[tuple[float, int, ScheduledTxn]] = []
        self.counter = itertools.count()
        self.stats = {"emitted": 0, "labels": 0, "delivery_errors": 0}
        self.injected = dict.fromkeys(ANOMALY_KINDS, 0)
        self.running = True
        self.labels_file = None  # opened by __enter__
        # The seed is fixed, so every restart replays the *same* txn_ids. Stamping
        # each label with the run it came from lets evaluate.py score a single run
        # instead of silently double-counting ground truth across restarts.
        self.run_id = iso_now()

    # The labels file stays open for the life of the run, so the simulator owns
    # it as a context manager rather than leaking a handle from __init__.
    def __enter__(self) -> Simulator:
        # Append, line-buffered: labels survive a hard kill, and the host can
        # tail the file while the simulation is still running.
        self.labels_file = open(self.cfg["labels_path"], "a", buffering=1)
        return self

    def __exit__(self, *_exc) -> None:
        log("flushing...")
        self.producer.flush(10)
        if self.labels_file is not None:
            self.labels_file.close()
        log(f"done: {self.stats} injected={self.injected}")

    # -- emission ----------------------------------------------------------
    def _on_delivery(self, err, _msg):
        if err is not None:
            self.stats["delivery_errors"] += 1
            log(f"delivery failed: {err}")

    def emit(self, sched: ScheduledTxn) -> None:
        """Stamp event time, publish keyed by card_id, record any label."""
        txn = dict(sched.txn)
        txn["event_time"] = iso_now()
        self.producer.produce(
            topic=self.cfg["topic"],
            # Keying by card_id puts a card's whole history in one partition, so
            # per-card state in Flink stays correct and ordered.
            key=txn["card_id"].encode(),
            value=json.dumps(txn).encode(),
            on_delivery=self._on_delivery,
        )
        self.stats["emitted"] += 1
        if sched.label is not None:
            label = dict(sched.label)
            label["event_time"] = txn["event_time"]
            label["run_id"] = self.run_id
            self.labels_file.write(json.dumps(label) + "\n")
            self.stats["labels"] += 1
        # Serve delivery callbacks without blocking.
        self.producer.poll(0)

    def schedule(self, items: list[ScheduledTxn], now: float) -> None:
        for item in items:
            heapq.heappush(self.pending, (now + item.delay_sec, next(self.counter), item))

    def maybe_inject(self, now: float) -> None:
        """With probability anomaly_rate, start one labelled anomaly."""
        if self.rng.random() >= self.cfg["anomaly_rate"]:
            return
        kind = self.rng.choice(ANOMALY_KINDS)
        card = self.rng.choice(self.cards)
        self.schedule(inject(self.rng, card, kind, self.cfg), now)
        self.injected[kind] += 1

    # -- main loop ---------------------------------------------------------
    def stop(self, *_):
        log("shutdown requested")
        self.running = False

    def run(self) -> None:
        cfg = self.cfg
        interval = 1.0 / cfg["events_per_sec"]
        start = time.monotonic()
        next_bg = start
        next_report = start + 5.0
        deadline = start + cfg["run_seconds"] if cfg["run_seconds"] > 0 else float("inf")

        log(
            f"run_id={self.run_id} "
            f"seed={cfg['seed']} cards={len(self.cards)} rate={cfg['events_per_sec']}/s "
            f"anomaly_rate={cfg['anomaly_rate']} topic={cfg['topic']} "
            f"labels={cfg['labels_path']}"
        )

        while self.running and time.monotonic() < deadline:
            now = time.monotonic()

            # 1. Anything scheduled that is now due (anomaly continuations).
            while self.pending and self.pending[0][0] <= now:
                self.emit(heapq.heappop(self.pending)[2])

            # 2. Background traffic, paced to the target rate.
            if now >= next_bg:
                card = self.rng.choice(self.cards)
                self.emit(ScheduledTxn(delay_sec=0.0, txn=make_transaction(self.rng, card)))
                self.maybe_inject(now)
                next_bg += interval
                # If we fell badly behind (long GC, paused container), resync
                # instead of trying to flush a backlog all at once.
                if next_bg < now - 1.0:
                    next_bg = now + interval
                continue

            if now >= next_report:
                log(
                    f"emitted={self.stats['emitted']} labels={self.stats['labels']} "
                    f"injected={self.injected} pending={len(self.pending)} "
                    f"errors={self.stats['delivery_errors']}"
                )
                next_report = now + 5.0

            # Sleep until the next due thing, capped so reports stay timely.
            wake = min(next_bg, self.pending[0][0] if self.pending else float("inf"), next_report)
            time.sleep(max(0.0, min(wake - time.monotonic(), 0.25)))


def main() -> int:
    cfg = load_config()
    with Simulator(cfg) as sim:
        # Compose sends SIGTERM on `down`; handle it so the final labels and
        # buffered records are flushed instead of lost.
        signal.signal(signal.SIGTERM, sim.stop)
        signal.signal(signal.SIGINT, sim.stop)
        sim.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
