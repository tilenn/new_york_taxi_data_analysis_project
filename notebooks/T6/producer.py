#!/usr/bin/env python3
"""
Kafka producer for Task 6 (streaming) that publishes NYC TLC Yellow and FHVHV trips
for a single year as JSON messages to two topics, ordered by pickup time.

Key features:
- Reads 12 monthly Parquet files via DuckDB with ORDER BY pickup timestamp
- Normalizes fields into a unified schema across datasets
- Enriches pickup/dropoff boroughs via the taxi zones shapefile
- Publishes to separate topics (yellow and fhvhv) with Kafka message timestamp = pickup time

Usage examples:
  # Produce both datasets sequentially (default):
  python notebooks/T6/producer.py \
    --year 2024 \
    --ext-src /Users/tilen/pers/project/ext/src \
    --bootstrap localhost:10000,localhost:10001 \
    --topic-yellow yellow_trips_2024 \
    --topic-fhvhv fhvhv_trips_2024

  # Produce only Yellow (run in one terminal):
  python notebooks/T6/producer.py --dataset yellow --year 2024 \
    --ext-src /Users/tilen/pers/project/ext/src --bootstrap localhost:10000,localhost:10001 \
    --topic-yellow yellow_trips_2024

  # Produce only FHVHV (run in another terminal):
  python notebooks/T6/producer.py --dataset fhvhv --year 2024 \
    --ext-src /Users/tilen/pers/project/ext/src --bootstrap localhost:10000,localhost:10001 \
    --topic-fhvhv fhvhv_trips_2024

  # Optionally pre-create topics to avoid consumer 'unknown topic' warnings:
  python notebooks/T6/producer.py --create-topics --year 2024 --ext-src /Users/tilen/pers/project/ext/src \
    --bootstrap localhost:10000,localhost:10001 --topic-yellow yellow_trips_2024 --topic-fhvhv fhvhv_trips_2024

Note: Requires a Kafka client library. This implementation uses confluent-kafka.
Install with: uv add confluent-kafka  (or pip install confluent-kafka)
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path
from typing import Iterable

import duckdb


def _ensure_project_on_sys_path():
    """Attempt to add project root to sys.path so `bdproject` can be imported."""
    here = Path(__file__).resolve()
    for parent in [here] + list(here.parents):
        if (parent / "src" / "bdproject").is_dir():
            sys.path.append(str(parent / "src"))
            return


_ensure_project_on_sys_path()

try:
    from bdproject.shapefile import get_gdf_shapefile
except Exception as exc:
    raise RuntimeError(
        "Unable to import bdproject. Ensure you run from project root or adjust PYTHONPATH."
    ) from exc


def _build_locid_to_borough() -> dict[int, str]:
    gdf = get_gdf_shapefile()
    m = (
        gdf[["OBJECTID", "borough"]]
        .astype({"OBJECTID": "int64"})
        .set_index("OBJECTID")["borough"]
        .to_dict()
    )
    return {int(k): str(v) for k, v in m.items()}


def _iter_duckdb_rows(conn: duckdb.DuckDBPyConnection, sql: str) -> Iterable[dict]:
    cur = conn.execute(sql)
    while True:
        df = cur.fetch_df_chunk()
        if df is None or len(df) == 0:
            break
        records = df.to_dict(orient="records")
        for r in records:
            yield r


def _sql_yellow(src_dir: Path, year: int) -> str:
    # Columns per original Yellow
    #  - tpep_pickup_datetime, tpep_dropoff_datetime
    #  - PULocationID, DOLocationID
    #  - trip_distance, fare_amount, tip_amount
    return f"""
        SELECT
            tpep_pickup_datetime  AS pu_dt,
            tpep_dropoff_datetime AS do_dt,
            PULocationID          AS pu_locid,
            DOLocationID          AS do_locid,
            CAST(trip_distance AS DOUBLE)       AS distance_miles,
            CAST(fare_amount AS DOUBLE)         AS fare_usd,
            CAST(tip_amount AS DOUBLE)          AS tip_usd
        FROM read_parquet('{(src_dir / "yellow" / f"yellow_tripdata_{year}-*.parquet").as_posix()}')
        WHERE tpep_pickup_datetime >= TIMESTAMP '{year}-01-01'
          AND tpep_pickup_datetime <  TIMESTAMP '{year + 1}-01-01'
        ORDER BY pu_dt
    """


def _sql_fhvhv(src_dir: Path, year: int) -> str:
    # Columns per original FHVHV
    #  - pickup_datetime, dropoff_datetime
    #  - PULocationID, DOLocationID
    #  - trip_miles, base_passenger_fare, tips
    return f"""
        SELECT
            pickup_datetime   AS pu_dt,
            dropoff_datetime  AS do_dt,
            PULocationID      AS pu_locid,
            DOLocationID      AS do_locid,
            CAST(trip_miles AS DOUBLE)             AS distance_miles,
            CAST(base_passenger_fare AS DOUBLE)    AS fare_usd,
            CAST(tips AS DOUBLE)                   AS tip_usd
        FROM read_parquet('{(src_dir / "fhvhv" / f"fhvhv_tripdata_{year}-*.parquet").as_posix()}')
        WHERE pickup_datetime >= TIMESTAMP '{year}-01-01'
          AND pickup_datetime <  TIMESTAMP '{year + 1}-01-01'
        ORDER BY pu_dt
    """


def _to_iso(o: object | None) -> str | None:
    if o is None:
        return None
    if isinstance(o, str):
        # Assume already ISO
        return o
    if isinstance(o, dt.datetime):
        return o.isoformat()
    # DuckDB may return pandas Timestamps
    try:
        return o.to_pydatetime().isoformat()  # type: ignore[attr-defined]
    except Exception:  # pragma: no cover - best effort
        return str(o)


def _safe_int(x: object | None) -> int | None:
    if x is None:
        return None
    try:
        # Handle NaN
        if isinstance(x, float) and x != x:
            return None
        return int(x)
    except Exception:
        return None


def _duration_minutes(pu_dt: object | None, do_dt: object | None) -> float | None:
    try:
        if pu_dt is None or do_dt is None:
            return None
        if isinstance(pu_dt, dt.datetime):
            a = pu_dt
        else:
            a = dt.datetime.fromisoformat(str(pu_dt))
        if isinstance(do_dt, dt.datetime):
            b = do_dt
        else:
            b = dt.datetime.fromisoformat(str(do_dt))
        delta = b - a
        return delta.total_seconds() / 60.0
    except Exception:
        return None


def _build_payload(row: dict, dataset: str, locid_to_borough: dict[int, str]) -> dict:
    pu_locid = _safe_int(row.get("pu_locid"))
    do_locid = _safe_int(row.get("do_locid"))
    pu_dt = row.get("pu_dt")
    do_dt = row.get("do_dt")

    payload = {
        "dataset": dataset,
        "pu_dt": _to_iso(pu_dt),
        "do_dt": _to_iso(do_dt),
        "pu_locid": pu_locid,
        "do_locid": do_locid,
        "pu_borough": locid_to_borough.get(pu_locid) if pu_locid is not None else None,
        "do_borough": locid_to_borough.get(do_locid) if do_locid is not None else None,
        "distance_miles": row.get("distance_miles"),
        "fare_usd": row.get("fare_usd"),
        "tip_usd": row.get("tip_usd"),
        "duration_minutes": _duration_minutes(pu_dt, do_dt),
    }
    return payload


def _event_timestamp_ms(pu_dt_iso: str | None) -> int | None:
    if not pu_dt_iso:
        return None
    try:
        t = dt.datetime.fromisoformat(pu_dt_iso)
        return int(t.timestamp() * 1000)
    except Exception:
        return None


def _produce_dataset(
    *,
    conn: duckdb.DuckDBPyConnection,
    sql: str,
    dataset: str,
    topic: str,
    producer,
    locid_to_borough: dict[int, str],
    flush_every: int,
) -> int:
    num = 0
    for row in _iter_duckdb_rows(conn, sql):
        payload = _build_payload(row, dataset, locid_to_borough)
        key = (
            str(payload["pu_locid"]) if payload.get("pu_locid") is not None else dataset
        )
        ts = _event_timestamp_ms(payload.get("pu_dt"))
        producer.produce(
            topic,
            key=key,
            value=json.dumps(payload, separators=(",", ":")),
            timestamp=ts,
        )
        num += 1
        if num % flush_every == 0:
            producer.poll(0)
            producer.flush()
    producer.flush()
    return num


def main() -> None:
    ap = argparse.ArgumentParser(description="T6 Kafka producer for Yellow + FHVHV")
    ap.add_argument("--year", type=int, default=2024, help="Calendar year >= 2024")
    ap.add_argument(
        "--ext-src",
        type=Path,
        default=Path("/Users/tilen/pers/project/ext/src"),
        help="Directory with original Parquet files",
    )
    ap.add_argument(
        "--bootstrap",
        type=str,
        default="localhost:10000,localhost:10001",
        help="Kafka bootstrap servers",
    )
    ap.add_argument(
        "--dataset",
        type=str,
        choices=["yellow", "fhvhv", "both"],
        default="both",
        help="Which dataset to produce",
    )
    ap.add_argument(
        "--topic-yellow",
        type=str,
        default=None,
        help="Topic name for yellow (default: yellow_trips_{year})",
    )
    ap.add_argument(
        "--topic-fhvhv",
        type=str,
        default=None,
        help="Topic name for fhvhv (default: fhvhv_trips_{year})",
    )
    ap.add_argument(
        "--create-topics",
        action="store_true",
        help="Pre-create topics before producing",
    )
    ap.add_argument(
        "--partitions",
        type=int,
        default=1,
        help="Partitions to create when --create-topics is used",
    )
    ap.add_argument(
        "--replication-factor",
        type=int,
        default=2,
        help="Replication factor when --create-topics is used",
    )
    ap.add_argument(
        "--batch-rows",
        type=int,
        default=50000,
        help="DuckDB chunk size when fetching rows",
    )
    ap.add_argument(
        "--flush-every",
        type=int,
        default=5000,
        help="Flush producer after this many messages",
    )
    ap.add_argument(
        "--compression",
        type=str,
        default="zstd",
        choices=["none", "gzip", "snappy", "lz4", "zstd"],
        help="Kafka compression.type",
    )
    args = ap.parse_args()

    year = args.year
    ext_src = args.ext_src
    topic_yellow = args.topic_yellow or f"yellow_trips_{year}"
    topic_fhvhv = args.topic_fhvhv or f"fhvhv_trips_{year}"

    # Kafka producer (confluent-kafka)
    try:
        from confluent_kafka import Producer
    except Exception as exc:  # pragma: no cover - dependency guidance
        raise RuntimeError(
            "confluent-kafka is required. Install with `uv add confluent-kafka` or `pip install confluent-kafka`."
        ) from exc

    producer = Producer(
        {
            "bootstrap.servers": args.bootstrap,
            "compression.type": args.compression,
            "linger.ms": 50,
            "acks": "all",
        }
    )

    if args.create_topics:
        try:
            from confluent_kafka.admin import AdminClient, NewTopic

            admin = AdminClient({"bootstrap.servers": args.bootstrap})
            topics_to_create = []
            if args.dataset in ("yellow", "both"):
                topics_to_create.append(
                    NewTopic(
                        topic_yellow,
                        num_partitions=args.partitions,
                        replication_factor=args.replication_factor,
                    )
                )
            if args.dataset in ("fhvhv", "both"):
                topics_to_create.append(
                    NewTopic(
                        topic_fhvhv,
                        num_partitions=args.partitions,
                        replication_factor=args.replication_factor,
                    )
                )
            if topics_to_create:
                fs = admin.create_topics(topics_to_create, request_timeout=10)
                for t, f in fs.items():
                    try:
                        f.result()
                    except Exception:
                        # Ignore errors like 'Topic already exists'
                        pass
        except Exception:
            # Best effort; continue even if admin client not available
            pass

    # Borough mapping
    locid_to_borough = _build_locid_to_borough()

    conn = duckdb.connect()
    sql_yellow = _sql_yellow(ext_src, year)
    sql_fhvhv = _sql_fhvhv(ext_src, year)

    total_yellow = 0
    total_fhvhv = 0

    # Produce per dataset to separate topics (keeps per-dataset time ordering)
    if args.dataset in ("yellow", "both"):
        total_yellow = _produce_dataset(
            conn=conn,
            sql=sql_yellow,
            dataset="yellow",
            topic=topic_yellow,
            producer=producer,
            locid_to_borough=locid_to_borough,
            flush_every=args.flush_every,
        )

    if args.dataset in ("fhvhv", "both"):
        total_fhvhv = _produce_dataset(
            conn=conn,
            sql=sql_fhvhv,
            dataset="fhvhv",
            topic=topic_fhvhv,
            producer=producer,
            locid_to_borough=locid_to_borough,
            flush_every=args.flush_every,
        )

    print(
        json.dumps(
            {
                "year": year,
                "topics": {"yellow": topic_yellow, "fhvhv": topic_fhvhv},
                "counts": {"yellow": total_yellow, "fhvhv": total_fhvhv},
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
