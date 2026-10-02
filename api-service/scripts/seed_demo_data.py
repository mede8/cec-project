#!/usr/bin/env python3
"""Fill Postgres with realistic demo experiments.

Lets you demo and test the API before the consumer is finished. Each
experiment warms up from room temperature towards the middle of its
threshold band (stabilisation), then holds there with sensor noise, a
slow drift, and a few deliberate excursions outside the band.

Usage (connects as the table OWNER, not the read-only api_reader):

    python scripts/seed_demo_data.py \
        --database-url postgresql://postgres:PASSWORD@localhost:5432/observability

    # more data, and wipe the tables first
    python scripts/seed_demo_data.py --database-url ... --experiments 5 \
        --duration 7200 --reset

Only needs: pip install "psycopg[binary]"
"""

import argparse
import math
import random
import sys
import time

import psycopg

STATUSES = ["running", "running", "terminated", "stabilizing", "configured"]


def generate(exp_index: int, start: float, duration: float, interval: float,
             rng: random.Random):
    lower = round(rng.uniform(18.0, 30.0), 1)
    upper = round(lower + rng.uniform(1.0, 3.0), 1)
    target = (lower + upper) / 2
    room = 21.0
    warmup = duration * 0.15  # first 15% is stabilisation

    # A few excursions: (start offset, length, size in degrees)
    excursions = [
        (rng.uniform(warmup, duration), rng.uniform(20, 90),
         rng.choice([-1, 1]) * rng.uniform(1.2, 2.5) * (upper - lower) / 2)
        for _ in range(rng.randint(2, 4))
    ]

    rows = []
    n = int(duration // interval)
    for i in range(n):
        t = i * interval
        if t < warmup:
            # Exponential approach from room temperature to the target.
            temp = target + (room - target) * math.exp(-5 * t / warmup)
        else:
            temp = target + 0.15 * (upper - lower) * math.sin(t / 600)
        temp += rng.gauss(0, 0.05 * (upper - lower))
        for ex_start, ex_len, ex_size in excursions:
            if ex_start <= t <= ex_start + ex_len:
                temp += ex_size
        rows.append((f"m{i:06d}", start + t, round(temp, 3)))

    status = STATUSES[exp_index % len(STATUSES)]
    started_at = start + warmup
    terminated_at = start + duration if status == "terminated" else None
    if status == "stabilizing":
        # Still warming up: only warm-up readings exist, not started yet.
        rows = [r for r in rows if r[1] < started_at]
        started_at = None
    elif status == "configured":
        rows, started_at = [], None  # configured, not even stabilising
    return lower, upper, status, started_at, terminated_at, rows


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--database-url", required=True)
    ap.add_argument("--experiments", type=int, default=3)
    ap.add_argument("--duration", type=float, default=3600,
                    help="seconds of data per experiment (default 3600)")
    ap.add_argument("--interval", type=float, default=5,
                    help="seconds between measurements (default 5)")
    ap.add_argument("--start", type=float, default=None,
                    help="epoch seconds of the first measurement (default: now - duration)")
    ap.add_argument("--seed", type=int, default=42, help="random seed (repeatable data)")
    ap.add_argument("--reset", action="store_true",
                    help="delete ALL rows from both tables first")
    args = ap.parse_args()

    rng = random.Random(args.seed)
    start = args.start if args.start is not None else time.time() - args.duration

    with psycopg.connect(args.database_url) as conn:
        if args.reset:
            conn.execute("TRUNCATE measurements, experiment_config")
        for i in range(args.experiments):
            exp_id = f"demo-{i + 1}"
            lower, upper, status, started_at, terminated_at, rows = generate(i, start, args.duration,
                                                  args.interval, rng)
            conn.execute(
                """
                INSERT INTO experiment_config (experiment_id, researcher, sensors,
                    lower_threshold, upper_threshold, status, started_at,
                    terminated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (experiment_id) DO UPDATE SET
                    lower_threshold = EXCLUDED.lower_threshold,
                    upper_threshold = EXCLUDED.upper_threshold,
                    status = EXCLUDED.status,
                    started_at = EXCLUDED.started_at,
                    terminated_at = EXCLUDED.terminated_at
                """,
                (exp_id, f"researcher{i + 1}@example.org",
                 [f"sensor-{i + 1}-{k}" for k in range(1, 4)], lower, upper, status,
                 started_at, terminated_at),
            )
            conn.execute("DELETE FROM measurements WHERE experiment_id = %s", (exp_id,))
            with conn.cursor().copy(
                "COPY measurements (experiment_id, measurement_id, ts, temperature) "
                "FROM STDIN"
            ) as copy:
                for mid, ts, temp in rows:
                    copy.write_row((exp_id, mid, ts, temp))
            out = sum(1 for _, ts, t in rows
                      if started_at is not None and ts >= started_at
                      and (t < lower or t > upper))
            print(f"{exp_id}: {status:<11} thresholds {lower}-{upper}  "
                  f"{len(rows):>5} measurements, {out} out of range")
    print(f"\nTime range: {start:.0f} .. {start + args.duration:.0f} (epoch seconds)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
