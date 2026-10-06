#!/usr/bin/env python3
"""Fill the database with realistic demo data in the team's schema.

Lets you demo the API before the consumer has ingested real data. It writes
rows the way the consumer does: one averaged reading per measurement, only
for the running phase, with out_of_range = not (lower <= avg <= upper).

    pip install "psycopg[binary]"
    python api/scripts/seed_demo_data.py \
        --database-url "postgresql://USER:PASSWORD@localhost:5432/DB"

Options: --experiments 5  --duration 3600  --interval 5  --start <epoch>
         --reset (delete ALL rows first)  --no-experiments-table (only write
         temperatures, which is what the current consumer does)
"""

import argparse
import math
import random
import sys
import time

import psycopg

PHASES = ["running", "running", "terminated", "stabilization", "configured"]


def generate(i, start, duration, interval, rng):
    lower = round(rng.uniform(18.0, 30.0), 1)
    upper = round(lower + rng.uniform(1.0, 3.0), 1)
    target, warmup = (lower + upper) / 2, duration * 0.15
    excursions = [(rng.uniform(warmup, duration), rng.uniform(20, 90),
                   rng.choice([-1, 1]) * rng.uniform(1.2, 2.5) * (upper - lower) / 2)
                  for _ in range(rng.randint(2, 4))]
    phase = PHASES[i % len(PHASES)]
    rows = []
    for n in range(int(duration // interval)):
        t = n * interval
        if t < warmup or phase in ("configured", "stabilization"):
            continue        # the consumer only saves readings while running
        temp = target + 0.15 * (upper - lower) * math.sin(t / 600) + rng.gauss(0, 0.05 * (upper - lower))
        for s, length, size in excursions:
            if s <= t <= s + length:
                temp += size
        temp = round(temp, 3)
        rows.append((f"m{n:06d}", start + t, temp, not (lower <= temp <= upper)))
    return lower, upper, phase, rows


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--database-url", required=True)
    ap.add_argument("--experiments", type=int, default=5)
    ap.add_argument("--duration", type=float, default=3600)
    ap.add_argument("--interval", type=float, default=5)
    ap.add_argument("--start", type=float, default=None)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--reset", action="store_true")
    ap.add_argument("--no-experiments-table", action="store_true")
    a = ap.parse_args()
    rng = random.Random(a.seed)
    start = a.start if a.start is not None else time.time() - a.duration

    with psycopg.connect(a.database_url) as conn:
        if a.reset:
            conn.execute("TRUNCATE temperatures, experiments")
        for i in range(a.experiments):
            exp = f"demo-{i + 1}"
            lower, upper, phase, rows = generate(i, start, a.duration, a.interval, rng)
            if not a.no_experiments_table:
                conn.execute(
                    """INSERT INTO experiments (experiment_id, researcher, sensors,
                           lower_threshold, upper_threshold, phase)
                       VALUES (%s, %s, %s, %s, %s, %s)
                       ON CONFLICT (experiment_id) DO UPDATE SET
                           lower_threshold = EXCLUDED.lower_threshold,
                           upper_threshold = EXCLUDED.upper_threshold, phase = EXCLUDED.phase""",
                    (exp, f"researcher{i + 1}", [f"sensor-{i + 1}-{k}" for k in (1, 2, 3)],
                     lower, upper, phase))
            conn.execute("DELETE FROM temperatures WHERE experiment_id = %s", (exp,))
            with conn.cursor().copy("COPY temperatures (experiment_id, measurement_id, ts, "
                                    "temperature, out_of_range) FROM STDIN") as cp:
                for mid, ts, temp, oor in rows:
                    cp.write_row((exp, mid, ts, temp, oor))
            print(f"{exp}: {phase:<13} thresholds {lower}-{upper}  {len(rows):>4} readings, "
                  f"{sum(r[3] for r in rows)} out of range")
    print(f"\nTime range: {start:.0f} .. {start + a.duration:.0f} (epoch seconds)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
