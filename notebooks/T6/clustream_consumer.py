#!/usr/bin/env python3
"""
CluStream-style Kafka consumer for Task 6

Consumes Yellow and FHVHV topics (JSON messages produced by notebooks/T6/producer.py),
builds streaming micro-clusters with exponential decay, and periodically generates
macro-clusters via a lightweight in-process KMeans on micro-cluster centroids.

Features used (robust, dependency-light):
  - Time: hour-of-day (sin, cos), day-of-week (sin, cos)
  - Trip: log1p(distance_miles), log1p(fare_usd), log1p(tip_usd), log1p(duration_minutes)
  - Borough one-hot: Manhattan, Brooklyn, Queens, Bronx, Staten Island, EWR

Notes:
  - No sklearn dependency; implements a tiny weighted KMeans with k-means++ init.
  - Exponential decay uses a half-life parameter (recent trips weigh more).
  - Optionally publishes macro-cluster summaries to an output Kafka topic.

Usage example:
  python notebooks/T6/clustream_consumer.py \
    --bootstrap localhost:10000,localhost:10001 \
    --group t6-clu-1 \
    --topics yellow_trips_2024 fhvhv_trips_2024 \
    --max-micro 200 --k 10 --tau 2.5 --half-life-minutes 360 \
    --report-interval-seconds 15
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import random
import signal
import time
from dataclasses import dataclass
from typing import List, Optional, Sequence, Tuple

# -------------------------
# Utilities and feature engineering
# -------------------------

BOROUGH_INDEX = {
    "Manhattan": 0,
    "Brooklyn": 1,
    "Queens": 2,
    "Bronx": 3,
    "Staten Island": 4,
    "EWR": 5,  # Newark Airport appears in TLC zones
}


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
        if isinstance(x, float) and x != x:  # NaN
            return None
        return float(x)
    except Exception:
        return None


def _log1p_safe(x: Optional[float]) -> float:
    if x is None or not math.isfinite(x) or x <= 0:
        return 0.0
    return math.log1p(float(x))


def _clip(x: float, low: float, high: float) -> float:
    return low if x < low else high if x > high else x


def _build_features(event: dict) -> List[float]:
    """
    Build a 14-D feature vector per event with robust scaling.
    Order:
      [0] sin_hour, [1] cos_hour, [2] sin_dow, [3] cos_dow,
      [4] log1p(distance)/5, [5] log1p(fare)/6, [6] log1p(tip)/5, [7] log1p(duration)/6,
      [8..13] borough one-hot (Manhattan, Brooklyn, Queens, Bronx, Staten Island, EWR)
    """
    pu_dt_iso = event.get("pu_dt")
    try:
        if isinstance(pu_dt_iso, str):
            t = dt.datetime.fromisoformat(pu_dt_iso)
        else:
            t = dt.datetime.utcnow()
    except Exception:
        t = dt.datetime.utcnow()

    hour = t.hour + t.minute / 60.0
    theta_h = 2.0 * math.pi * (hour / 24.0)
    sin_h = math.sin(theta_h)
    cos_h = math.cos(theta_h)

    dow = t.weekday()  # 0=Mon
    theta_d = 2.0 * math.pi * (dow / 7.0)
    sin_d = math.sin(theta_d)
    cos_d = math.cos(theta_d)

    v_dist = _log1p_safe(_coerce_float(event.get("distance_miles"))) / 5.0
    v_fare = _log1p_safe(_coerce_float(event.get("fare_usd"))) / 6.0
    v_tip = _log1p_safe(_coerce_float(event.get("tip_usd"))) / 5.0
    v_dur = _log1p_safe(_coerce_float(event.get("duration_minutes"))) / 6.0

    borough = event.get("pu_borough")
    one_hot = [0.0] * len(BOROUGH_INDEX)
    if isinstance(borough, str) and borough in BOROUGH_INDEX:
        one_hot[BOROUGH_INDEX[borough]] = 1.0

    return [sin_h, cos_h, sin_d, cos_d, v_dist, v_fare, v_tip, v_dur, *one_hot]


# -------------------------
# Micro-clusters with exponential decay
# -------------------------


def _decay_factor(delta_ms: int, half_life_ms: float) -> float:
    if delta_ms <= 0:
        return 1.0
    return 0.5 ** (float(delta_ms) / float(half_life_ms))


@dataclass
class MicroCluster:
    weight: float
    ls: List[float]
    ss: List[float]
    last_update_ms: int

    def apply_decay(self, now_ms: int, half_life_ms: float) -> None:
        f = _decay_factor(now_ms - self.last_update_ms, half_life_ms)
        if f != 1.0:
            self.weight *= f
            for i in range(len(self.ls)):
                self.ls[i] *= f
                self.ss[i] *= f
            self.last_update_ms = now_ms

    def add_point(self, x: Sequence[float], now_ms: int) -> None:
        self.weight += 1.0
        for i, v in enumerate(x):
            self.ls[i] += v
            self.ss[i] += v * v
        self.last_update_ms = now_ms

    def centroid(self) -> List[float]:
        if self.weight <= 0.0:
            return [0.0 for _ in self.ls]
        return [v / self.weight for v in self.ls]

    def rms_radius(self) -> float:
        # sqrt( E[||x||^2] - ||E[x]||^2 )
        if self.weight <= 0.0:
            return 0.0
        sum_ss = sum(self.ss)
        sum_ls_sq = sum(v * v for v in self.ls)
        msq = sum_ss / self.weight
        mean_norm_sq = sum_ls_sq / (self.weight * self.weight)
        val = msq - mean_norm_sq
        return math.sqrt(val) if val > 0.0 else 0.0


class MicroClusterManager:
    def __init__(
        self,
        *,
        dim: int,
        max_micro: int = 200,
        tau: float = 2.5,
        half_life_minutes: float = 360.0,
    ):
        self.dim = dim
        self.max_micro = max_micro
        self.tau = tau
        self.half_life_ms = float(half_life_minutes) * 60_000.0
        self.clusters: List[MicroCluster] = []

    def _euclidean(self, a: Sequence[float], b: Sequence[float]) -> float:
        s = 0.0
        for i in range(self.dim):
            d = a[i] - b[i]
            s += d * d
        return math.sqrt(s)

    def _find_nearest(self, x: Sequence[float]) -> Tuple[int, float]:
        best_idx = -1
        best_dist = float("inf")
        for i, mc in enumerate(self.clusters):
            c = mc.centroid()
            d = self._euclidean(x, c)
            if d < best_dist:
                best_dist = d
                best_idx = i
        return best_idx, best_dist

    def _evict_if_needed(self) -> None:
        if len(self.clusters) <= self.max_micro:
            return
        # Remove weakest (lowest weight) micro-cluster
        weakest_idx = min(
            range(len(self.clusters)), key=lambda i: self.clusters[i].weight
        )
        self.clusters.pop(weakest_idx)

    def decay_all(self, now_ms: int) -> None:
        for mc in self.clusters:
            mc.apply_decay(now_ms, self.half_life_ms)
        # Drop clusters that decayed to negligible weight
        self.clusters = [mc for mc in self.clusters if mc.weight > 1e-6]

    def update(self, x: Sequence[float], now_ms: int) -> None:
        # Decay existing clusters to current time for accurate distances
        self.decay_all(now_ms)

        if not self.clusters:
            self.clusters.append(
                MicroCluster(
                    weight=1.0, ls=list(x), ss=[v * v for v in x], last_update_ms=now_ms
                )
            )
            return

        idx, dist = self._find_nearest(x)
        if idx >= 0 and dist <= self.tau:
            self.clusters[idx].add_point(x, now_ms)
        else:
            self.clusters.append(
                MicroCluster(
                    weight=1.0, ls=list(x), ss=[v * v for v in x], last_update_ms=now_ms
                )
            )
            self._evict_if_needed()

    def get_weighted_centroids(self) -> Tuple[List[List[float]], List[float]]:
        xs: List[List[float]] = []
        ws: List[float] = []
        for mc in self.clusters:
            if mc.weight > 0.0:
                xs.append(mc.centroid())
                ws.append(mc.weight)
        return xs, ws


# -------------------------
# Lightweight weighted KMeans (k-means++ init)
# -------------------------


def _weighted_choice(weights: Sequence[float]) -> int:
    total = sum(weights)
    if total <= 0.0:
        return random.randrange(len(weights))
    r = random.random() * total
    acc = 0.0
    for i, w in enumerate(weights):
        acc += w
        if r <= acc:
            return i
    return len(weights) - 1


def _sq_euclid(a: Sequence[float], b: Sequence[float]) -> float:
    s = 0.0
    for i in range(len(a)):
        d = a[i] - b[i]
        s += d * d
    return s


def kmeans_weighted(
    data: List[List[float]],
    weights: List[float],
    k: int,
    *,
    max_iter: int = 15,
    seed: Optional[int] = 42,
) -> Tuple[List[List[float]], List[int]]:
    if seed is not None:
        random.seed(seed)

    n = len(data)
    if n == 0:
        return [], []
    if k <= 0:
        return [], []
    k = min(k, n)

    # k-means++ initialization (weighted by point weights)
    centers: List[List[float]] = []
    first_idx = _weighted_choice(weights)
    centers.append(list(data[first_idx]))

    # Distances to nearest center
    d2 = [float("inf")] * n
    for _ in range(1, k):
        for i in range(n):
            d = _sq_euclid(data[i], centers[-1])
            if d < d2[i]:
                d2[i] = d
        # Choose next proportional to w * d^2
        probs = [weights[i] * d2[i] for i in range(n)]
        next_idx = _weighted_choice(probs)
        centers.append(list(data[next_idx]))

    # Lloyd iterations
    assignments = [0] * n
    for _ in range(max_iter):
        changed = False
        # Assign step
        for i in range(n):
            best_j = 0
            best_d = float("inf")
            for j in range(len(centers)):
                d = _sq_euclid(data[i], centers[j])
                if d < best_d:
                    best_d = d
                    best_j = j
            if assignments[i] != best_j:
                assignments[i] = best_j
                changed = True

        # Update step (weighted means)
        dim = len(data[0])
        new_centers = [[0.0] * dim for _ in range(len(centers))]
        sums_w = [0.0] * len(centers)
        for i in range(n):
            j = assignments[i]
            w = weights[i]
            sums_w[j] += w
            for d in range(dim):
                new_centers[j][d] += w * data[i][d]
        for j in range(len(centers)):
            if sums_w[j] > 0.0:
                for d in range(dim):
                    new_centers[j][d] /= sums_w[j]
            else:
                # Re-seed empty cluster randomly
                new_centers[j] = list(data[random.randrange(n)])
        centers = new_centers
        if not changed:
            break
    return centers, assignments


# -------------------------
# Kafka consumer orchestration
# -------------------------


def _print_macro_report(
    *,
    now: float,
    manager: MicroClusterManager,
    macro_centers: List[List[float]],
    macro_assignments: List[int],
    weights: List[float],
) -> None:
    ts = dt.datetime.fromtimestamp(now).strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n[{ts}] CluStream report")
    print(f"Active micro-clusters: {len(manager.clusters)} | k={len(macro_centers)}")
    # Aggregate weights per macro cluster
    k = len(macro_centers)
    agg_w = [0.0] * k
    for i, a in enumerate(macro_assignments):
        if 0 <= a < k:
            agg_w[a] += weights[i]
    for j in range(k):
        center = macro_centers[j]
        # brief: show time and trip features only
        row = (
            f"  C{j:<2} w={agg_w[j]:>10.2f} | "
            f"hour(sin)={center[0]: .3f} hour(cos)={center[1]: .3f} "
            f"dist={center[4]: .3f} fare={center[5]: .3f} tip={center[6]: .3f} dur={center[7]: .3f}"
        )
        print(row)


def _build_cluster_summary(
    macro_centers: List[List[float]], agg_weights: List[float]
) -> dict:
    return {
        "k": len(macro_centers),
        "clusters": [
            {
                "id": i,
                "weight": agg_weights[i],
                "center": macro_centers[i],
            }
            for i in range(len(macro_centers))
        ],
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="CluStream-style Kafka consumer")
    ap.add_argument(
        "--bootstrap",
        type=str,
        default="localhost:10000,localhost:10001",
        help="Kafka bootstrap servers",
    )
    ap.add_argument(
        "--group", type=str, default="t6-clustream", help="Consumer group id"
    )
    ap.add_argument(
        "--topics",
        type=str,
        nargs="+",
        required=True,
        help="Topics to subscribe to (e.g., yellow_trips_2024 fhvhv_trips_2024)",
    )
    ap.add_argument("--max-messages", type=int, default=0, help="0 = run forever")
    ap.add_argument("--report-interval-seconds", type=int, default=15)
    ap.add_argument("--max-micro", type=int, default=200)
    ap.add_argument("--k", type=int, default=10)
    ap.add_argument(
        "--tau", type=float, default=2.5, help="Assignment distance threshold"
    )
    ap.add_argument("--half-life-minutes", type=float, default=360.0)
    ap.add_argument(
        "--output-topic",
        type=str,
        default=None,
        help="Optional topic to publish macro-cluster summaries",
    )
    args = ap.parse_args()

    try:
        from confluent_kafka import Consumer, KafkaError, Producer
    except Exception as exc:  # pragma: no cover
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

    producer: Optional[Producer] = None
    if args.output_topic:
        producer = Producer(
            {"bootstrap.servers": args.bootstrap, "compression.type": "zstd"}
        )

    dim = 8 + len(BOROUGH_INDEX)
    manager = MicroClusterManager(
        dim=dim,
        max_micro=args.max_micro,
        tau=args.tau,
        half_life_minutes=args.half_life_minutes,
    )

    should_stop = False

    def _signal_handler(signum, frame):  # noqa: ARG001
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
                # periodic report
                now = time.time()
                if now - last_report >= args.report_interval_seconds:
                    xs, ws = manager.get_weighted_centroids()
                    if xs:
                        centers, assigns = kmeans_weighted(xs, ws, args.k)
                        _print_macro_report(
                            now=now,
                            manager=manager,
                            macro_centers=centers,
                            macro_assignments=assigns,
                            weights=ws,
                        )
                        if producer and args.output_topic:
                            # aggregate weights per macro id and publish
                            k = len(centers)
                            agg_w = [0.0] * k
                            for i, a in enumerate(assigns):
                                if 0 <= a < k:
                                    agg_w[a] += ws[i]
                            payload = _build_cluster_summary(centers, agg_w)
                            producer.produce(
                                args.output_topic,
                                key=str(int(now)),
                                value=json.dumps(payload, separators=(",", ":")),
                                timestamp=int(now * 1000),
                            )
                            producer.poll(0)
                    last_report = now
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:  # type: ignore[attr-defined]
                    continue
                print(f"Kafka error: {msg.error()}")
                continue

            try:
                event = json.loads(msg.value().decode("utf-8"))
            except Exception as exc:  # pragma: no cover
                print(f"skip bad message: {exc}")
                continue

            ts_ms = _parse_event_timestamp_ms(event.get("pu_dt"))
            if ts_ms is None:
                # fallback to wall time
                ts_ms = int(time.time() * 1000)

            x = _build_features(event)
            manager.update(x, ts_ms)
            seen += 1

            now = time.time()
            if now - last_report >= args.report_interval_seconds:
                xs, ws = manager.get_weighted_centroids()
                if xs:
                    centers, assigns = kmeans_weighted(xs, ws, args.k)
                    _print_macro_report(
                        now=now,
                        manager=manager,
                        macro_centers=centers,
                        macro_assignments=assigns,
                        weights=ws,
                    )
                    if producer and args.output_topic:
                        k = len(centers)
                        agg_w = [0.0] * k
                        for i, a in enumerate(assigns):
                            if 0 <= a < k:
                                agg_w[a] += ws[i]
                        payload = _build_cluster_summary(centers, agg_w)
                        producer.produce(
                            args.output_topic,
                            key=str(int(now)),
                            value=json.dumps(payload, separators=(",", ":")),
                            timestamp=int(now * 1000),
                        )
                        producer.poll(0)
                last_report = now
    finally:
        try:
            consumer.close()
        except Exception:
            pass


if __name__ == "__main__":  # pragma: no cover
    main()
