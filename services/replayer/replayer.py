"""Replays a historical Reddit comments dataset into Kafka, paced by the
comments' own timestamps (created_utc).

REPLAY_MODE=realtime     -> original gaps between comments (1x)
REPLAY_MODE=accelerated  -> gaps divided by SPEEDUP_FACTOR
"""
import csv
import json
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone

from confluent_kafka import Producer


def env(key, default=None):
    return os.environ.get(key, default)


BOOTSTRAP = env("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = env("KAFKA_TOPIC", "reddit-comments")
FORMAT = env("DATA_FORMAT", "sqlite").lower()
PATH = env("DATA_PATH", "/data/database.sqlite")
TABLE = env("SQLITE_TABLE", "May2015")
START_UTC = int(env("START_UTC", "0"))
END_UTC = int(env("END_UTC", "0"))
MAX_ROWS = int(env("MAX_ROWS", "0"))
MODE = env("REPLAY_MODE", "accelerated").lower()
SPEEDUP = 1.0 if MODE == "realtime" else float(env("SPEEDUP_FACTOR", "120"))

COL = {
    "id": env("COL_ID", "id"),
    "author": env("COL_AUTHOR", "author"),
    "body": env("COL_BODY", "body"),
    "subreddit": env("COL_SUBREDDIT", "subreddit"),
    "created_utc": env("COL_TS", "created_utc"),
    "ups": env("COL_UPS", "ups"),
    "downs": env("COL_DOWNS", "downs"),
    "score": env("COL_SCORE", "score"),
}


def log(msg):
    print(msg, flush=True)


# ---------- readers: yield dicts keyed by SOURCE column names ----------
def rows_sqlite():
    if not os.path.exists(PATH):
        sys.exit(f"[replayer] {PATH} not found. Put Kaggle's database.sqlite in ./data/")
    # immutable=1 lets us read from a read-only mount without journal files
    conn = sqlite3.connect(f"file:{PATH}?mode=ro&immutable=1", uri=True)
    conn.row_factory = sqlite3.Row
    cols = ", ".join(f'"{c}"' for c in COL.values())
    ts = f'"{COL["created_utc"]}"'
    where, params = [], []
    if START_UTC:
        where.append(f"{ts} >= ?")
        params.append(START_UTC)
    if END_UTC:
        where.append(f"{ts} < ?")
        params.append(END_UTC)
    sql = f'SELECT {cols} FROM "{TABLE}"'
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += f" ORDER BY {ts}"
    if MAX_ROWS:
        sql += f" LIMIT {MAX_ROWS}"
    log(f"[replayer] querying sqlite (a full-table scan can take several minutes)...\n  {sql}")
    cur = conn.execute(sql, params)
    for row in cur:
        yield dict(row)


def rows_csv():
    csv.field_size_limit(sys.maxsize)
    with open(PATH, newline="", encoding="utf-8") as f:
        yield from csv.DictReader(f)


def rows_jsonl():
    with open(PATH, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


READERS = {"sqlite": rows_sqlite, "csv": rows_csv, "jsonl": rows_jsonl}


def to_int(v, default=None):
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return default


def normalize(raw):
    """Map source columns -> canonical message. Returns None if unusable."""
    body = raw.get(COL["body"])
    sub = raw.get(COL["subreddit"])
    ts = to_int(raw.get(COL["created_utc"]))
    if not body or not sub or ts is None:
        return None
    if START_UTC and ts < START_UTC:
        return None
    if END_UTC and ts >= END_UTC:
        return None
    return {
        "id": raw.get(COL["id"]),
        "author": raw.get(COL["author"]),
        "body": body,
        "subreddit": sub,
        "created_utc": ts,
        "ups": to_int(raw.get(COL["ups"])),
        "downs": to_int(raw.get(COL["downs"])),
        "score": to_int(raw.get(COL["score"])),
    }


def main():
    if FORMAT not in READERS:
        sys.exit(f"[replayer] unknown DATA_FORMAT={FORMAT}")
    log(f"[replayer] mode={MODE} speedup={SPEEDUP}x format={FORMAT} topic={TOPIC}")

    producer = Producer({
        "bootstrap.servers": BOOTSTRAP,
        "linger.ms": 20,
        "batch.num.messages": 5000,
        "queue.buffering.max.messages": 200000,
    })
    errors = {"n": 0}

    def on_delivery(err, _msg):
        if err is not None:
            errors["n"] += 1

    sent = skipped = 0
    first_ts = wall0 = None
    last_log = time.monotonic()

    for raw in READERS[FORMAT]():
        if MAX_ROWS and sent >= MAX_ROWS:
            break
        rec = normalize(raw)
        if rec is None:
            skipped += 1
            continue

        ts = rec["created_utc"]
        if first_ts is None:
            first_ts, wall0 = ts, time.monotonic()
            log("[replayer] first comment at "
                f"{datetime.fromtimestamp(ts, timezone.utc):%Y-%m-%d %H:%M:%S} UTC")
        # pace by event time (out-of-order rows simply don't sleep)
        lag = wall0 + (ts - first_ts) / SPEEDUP - time.monotonic()
        if lag > 0.005:
            producer.poll(0)
            time.sleep(lag)

        payload = json.dumps(rec).encode("utf-8")
        while True:
            try:
                producer.produce(TOPIC, key=rec["subreddit"].encode("utf-8"),
                                 value=payload, on_delivery=on_delivery)
                break
            except BufferError:
                producer.poll(0.5)
        producer.poll(0)
        sent += 1

        now = time.monotonic()
        if now - last_log >= 5:
            log(f"[replayer] sent={sent:,} skipped={skipped:,} "
                f"sim_time={datetime.fromtimestamp(ts, timezone.utc):%Y-%m-%d %H:%M} UTC")
            last_log = now

    producer.flush()
    log(f"[replayer] DONE sent={sent:,} skipped={skipped:,} delivery_errors={errors['n']}")


if __name__ == "__main__":
    main()
