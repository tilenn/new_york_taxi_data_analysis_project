#!/usr/bin/env python3
"""
Kafka consumer for Task 6 (streaming):

Subscribes to Yellow and FHVHV topics, parses JSON events, and maintains rolling
descriptive statistics (mean, stddev) for selected attributes by:
  - pickup borough (pu_borough)
  - top-N pickup locations (pu_locid), determined dynamically by observed counts

Attributes tracked (from the unified producer schema):
  - distance_miles
  - fare_usd
  - tip_usd

Event-time windowing: a fixed-length sliding window (default 15 minutes) based on pu_dt.

Usage example:
  python notebooks/T6/consumer.py \
    --bootstrap localhost:10000,localhost:10001 \
    --group t6-consumer-1 \
    --topics yellow_trips_2024 fhvhv_trips_2024 \
    --window-minutes 15 \
    --report-interval-seconds 10 \
    --top-n 10

Requires: confluent-kafka
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import signal
import time
from collections import Counter, defaultdict, deque
from dataclasses import dataclass
from typing import Deque

ATTRIBUTES = ("distance_miles", "fare_usd", "tip_usd")


def _parse_event_timestamp_ms(value: str | None) -> int | None:
    if not value:
        return None
    try:
        return int(dt.datetime.fromisoformat(value).timestamp() * 1000)
    except Exception:
        return None


def _coerce_float(x) -> float | None:
    if x is None:
        return None
    try:
        # drop NaN
        if isinstance(x, float) and x != x:
            return None
        return float(x)
    except Exception:
        return None


@dataclass
class RollingStats:
    # Maintain rolling sums for mean/std over a time window
    window_ms: int
    # deque of (event_time_ms, values_dict)
    buffer: Deque[tuple[int, dict]]
    # sums and squared sums per attribute
    sum_values: dict[str, float]
    sum_squares: dict[str, float]
    count: int

    def __init__(self, window_ms: int):
        self.window_ms = window_ms
        self.buffer = deque()
        self.sum_values = defaultdict(float)
        self.sum_squares = defaultdict(float)
        self.count = 0

    def add(self, event_time_ms: int, values: dict[str, float | None]):
        clean = {k: v for k, v in values.items() if v is not None}
        if not clean:
            return
        self.buffer.append((event_time_ms, clean))
        for k, v in clean.items():
            fv = float(v)
            self.sum_values[k] += fv
            self.sum_squares[k] += fv * fv
        self.count += 1
        self._evict(event_time_ms)

    def _evict(self, now_ms: int):
        min_time = now_ms - self.window_ms
        while self.buffer and self.buffer[0][0] < min_time:
            _, vals = self.buffer.popleft()
            for k, v in vals.items():
                fv = float(v)
                self.sum_values[k] -= fv
                self.sum_squares[k] -= fv * fv
            self.count -= 1
        # clamp numerical drift
        for k in list(self.sum_values.keys()):
            if abs(self.sum_values[k]) < 1e-12:
                self.sum_values[k] = 0.0
            if abs(self.sum_squares[k]) < 1e-12:
                self.sum_squares[k] = 0.0
        if self.count < 0:
            self.count = 0

    def snapshot(self) -> dict[str, dict[str, float]]:
        # return mean/std for current window
        out: dict[str, dict[str, float]] = {}
        n = len(self.buffer)
        if n == 0:
            for a in ATTRIBUTES:
                out[a] = {"mean": float("nan"), "std": float("nan"), "n": 0}
            return out
        for a in ATTRIBUTES:
            s = self.sum_values.get(a, 0.0)
            ss = self.sum_squares.get(a, 0.0)
            mean = s / n if n > 0 else float("nan")
            var = max(ss / n - mean * mean, 0.0) if n > 1 else 0.0
            out[a] = {"mean": mean, "std": var**0.5, "n": n}
        return out


class StatsManager:
    def __init__(self, window_minutes: int, top_n: int):
        self.window_ms = window_minutes * 60_000
        self.top_n = top_n
        self.by_borough: dict[str, RollingStats] = defaultdict(
            lambda: RollingStats(self.window_ms)
        )
        self.by_locid: dict[int, RollingStats] = {}
        self.locid_counts: Counter[int] = Counter()

    def ingest(self, event: dict):
        pu_dt_iso = event.get("pu_dt")
        ts_ms = _parse_event_timestamp_ms(pu_dt_iso)
        if ts_ms is None:
            return

        values = {
            "distance_miles": _coerce_float(event.get("distance_miles")),
            "fare_usd": _coerce_float(event.get("fare_usd")),
            "tip_usd": _coerce_float(event.get("tip_usd")),
        }

        # Borough-level stats
        pu_borough = event.get("pu_borough")
        if isinstance(pu_borough, str) and pu_borough:
            self.by_borough[pu_borough].add(ts_ms, values)

        # Top-N locatin ids
        pu_locid = event.get("pu_locid")
        if isinstance(pu_locid, int):
            self.locid_counts[pu_locid] += 1
            # Track stats for current top-N locids
            for locid in self._current_top_locids():
                if locid not in self.by_locid:
                    self.by_locid[locid] = RollingStats(self.window_ms)
            if pu_locid in self.by_locid:
                self.by_locid[pu_locid].add(ts_ms, values)

    def _current_top_locids(self) -> list[int]:
        return [loc for loc, _ in self.locid_counts.most_common(self.top_n)]

    def report(self) -> dict:
        return {
            "boroughs": {k: v.snapshot() for k, v in self.by_borough.items()},
            "top_locids": {
                locid: self.by_locid[locid].snapshot()
                for locid in self._current_top_locids()
                if locid in self.by_locid
            },
        }


def _print_report(rep: dict, limit_boroughs: int | None = None):
    now = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n[{now}] Rolling window stats")
    print("By borough:")
    items = list(rep.get("boroughs", {}).items())
    if limit_boroughs is not None:
        items = items[:limit_boroughs]
    for borough, stats in items:
        lines = [
            f"  {borough:<14} n={int(stats['distance_miles']['n']):>7}"
            f" | dist μ={stats['distance_miles']['mean']:.3f} σ={stats['distance_miles']['std']:.3f}"
            f" | fare μ={stats['fare_usd']['mean']:.2f} σ={stats['fare_usd']['std']:.2f}"
            f" | tip μ={stats['tip_usd']['mean']:.2f} σ={stats['tip_usd']['std']:.2f}"
        ]
        print("\n".join(lines))
    print("Top-N pickup locids:")
    for locid, stats in rep.get("top_locids", {}).items():
        print(
            f"  {locid:<6} n={int(stats['distance_miles']['n']):>7}"
            f" | dist μ={stats['distance_miles']['mean']:.3f} σ={stats['distance_miles']['std']:.3f}"
            f" | fare μ={stats['fare_usd']['mean']:.2f} σ={stats['fare_usd']['std']:.2f}"
            f" | tip μ={stats['tip_usd']['mean']:.2f} σ={stats['tip_usd']['std']:.2f}"
        )


def main() -> None:
    ap = argparse.ArgumentParser(description="T6 Kafka consumer for rolling stats")
    ap.add_argument(
        "--bootstrap",
        type=str,
        default="localhost:10000,localhost:10001",
        help="Kafka bootstrap servers",
    )
    ap.add_argument(
        "--group", type=str, default="t6-consumer", help="Consumer group id"
    )
    ap.add_argument(
        "--topics",
        type=str,
        nargs="+",
        required=True,
        help="Topics to subscribe to (e.g., yellow_trips_2024 fhvhv_trips_2024)",
    )
    ap.add_argument("--window-minutes", type=int, default=15)
    ap.add_argument("--report-interval-seconds", type=int, default=10)
    ap.add_argument("--top-n", type=int, default=10)
    ap.add_argument("--max-messages", type=int, default=0, help="0 = run forever")
    args = ap.parse_args()

    try:
        from confluent_kafka import Consumer, KafkaError
    except Exception as exc:
        raise RuntimeError(
            "confluent-kafka is required. Install with `uv add confluent-kafka` or `pip install confluent-kafka`."
        ) from exc

    conf = {
        "bootstrap.servers": args.bootstrap,
        "group.id": args.group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
        "partition.assignment.strategy": "cooperative-sticky",
    }
    consumer = Consumer(conf)
    consumer.subscribe(args.topics)

    stats = StatsManager(window_minutes=args.window_minutes, top_n=args.top_n)

    should_stop = False

    def _signal_handler(signum, frame):
        nonlocal should_stop
        should_stop = True

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    last_report = time.time()
    seen = 0

    try:
        while not should_stop and (args.max_messages <= 0 or seen < args.max_messages):
            msg = consumer.poll(0.2)
            if msg is None:
                if time.time() - last_report >= args.report_interval_seconds:
                    _print_report(stats.report())
                    last_report = time.time()
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:  # type: ignore[attr-defined]
                    continue
                print(f"Kafka error: {msg.error()}")
                continue
            try:
                event = json.loads(msg.value().decode("utf-8"))
                # Normalize pu_locid to int when possible
                if (
                    isinstance(event.get("pu_locid"), float)
                    and event["pu_locid"] == event["pu_locid"]
                ):
                    event["pu_locid"] = int(event["pu_locid"])  # type: ignore[assignment]
                stats.ingest(event)
                seen += 1
            except Exception as exc:
                print(f"skip bad message: {exc}")

            if time.time() - last_report >= args.report_interval_seconds:
                _print_report(stats.report())
                last_report = time.time()
    finally:
        try:
            consumer.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
