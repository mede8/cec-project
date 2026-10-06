#!/usr/bin/env python3
"""Load-test the API the way the course's http-load-generator does.

The official generator (EC-labs/cec-assignment, http-load-generator) sends
batches of 100-200 queries per second with up to 50 requests in flight,
half /temperature (window between two random measurements, rounded outward
to the millisecond) and half /temperature/out-of-range. It sorts each
response and compares the timestamps EXACTLY with its ground truth.

This script does the same, using the database as ground truth, and reports
throughput, latency percentiles and any mismatches.

    pip install "psycopg[binary]" httpx
    python api/scripts/loadtest.py --database-url "postgresql://USER:PW@localhost:5432/DB" \
        --api http://localhost:3003 --rate 200 --seconds 30 --in-flight 50
"""

import argparse
import asyncio
import bisect
import json
import math
import random
import statistics
import sys
import time
from collections import Counter, defaultdict

import httpx
import psycopg


def load_ground_truth(url):
    all_ts, oor_ts = defaultdict(list), defaultdict(list)
    with psycopg.connect(url) as conn:
        for exp, ts, oor in conn.execute(
                "SELECT experiment_id, ts, out_of_range FROM temperatures ORDER BY experiment_id, ts"):
            all_ts[exp].append(ts)
            if oor:
                oor_ts[exp].append(ts)
    return all_ts, oor_ts


def make_query(rng, all_ts):
    exp = rng.choice(list(all_ts))
    if rng.random() < 0.5:
        return ("oor", exp, None, None)
    ts = all_ts[exp]
    i, j = sorted((rng.randrange(len(ts)), rng.randrange(len(ts))))
    start = math.floor(ts[i] * 1000.0) / 1000.0          # same rounding as the grader
    end = math.ceil(ts[j] * 1000.0) / 1000.0
    return ("temp", exp, start, end)


def expected(q, all_ts, oor_ts):
    kind, exp, start, end = q
    if kind == "oor":
        return oor_ts.get(exp, [])
    ts = all_ts[exp]
    return ts[bisect.bisect_left(ts, start):bisect.bisect_right(ts, end)]


async def run(args):
    all_ts, oor_ts = load_ground_truth(args.database_url)
    if not all_ts:
        print("no readings in the database: seed it first")
        return 1
    rng = random.Random(args.seed)
    total = args.rate * args.seconds
    queries = [make_query(rng, all_ts) for _ in range(total)]
    sem = asyncio.Semaphore(args.in_flight)
    latencies, outcomes, examples = [], Counter(), []
    limits = httpx.Limits(max_connections=args.in_flight, max_keepalive_connections=args.in_flight)

    async with httpx.AsyncClient(base_url=args.api, limits=limits, timeout=30) as client:
        async def one(q):
            kind, exp, start, end = q
            async with sem:
                t0 = time.perf_counter()
                try:
                    if kind == "oor":
                        r = await client.get("/temperature/out-of-range", params={"experiment-id": exp})
                    else:
                        r = await client.get("/temperature", params={
                            "experiment-id": exp, "start-time": repr(start), "end-time": repr(end)})
                except httpx.HTTPError as e:
                    outcomes["server_error"] += 1
                    examples.append(f"{kind} {exp}: {e!r}")
                    return
                latencies.append(time.perf_counter() - t0)
            if r.status_code != 200:
                outcomes[f"http_{r.status_code}"] += 1
                examples.append(f"{kind} {exp}: HTTP {r.status_code} {r.text[:100]}")
                return
            try:
                got = sorted(m["timestamp"] for m in json.loads(r.text))
            except (ValueError, KeyError, TypeError):
                outcomes["deserialization_error"] += 1
                return
            if got != expected(q, all_ts, oor_ts):
                outcomes["validation_error"] += 1
                examples.append(f"{kind} {exp} [{start}, {end}]: got {len(got)} rows, "
                                f"expected {len(expected(q, all_ts, oor_ts))}")
            else:
                outcomes["ok"] += 1

        # Release one batch of `rate` queries per second, like the grader.
        tasks, t_start = [], time.perf_counter()
        for second in range(args.seconds):
            batch = queries[second * args.rate:(second + 1) * args.rate]
            tasks += [asyncio.create_task(one(q)) for q in batch]
            next_tick = t_start + second + 1
            await asyncio.sleep(max(0.0, next_tick - time.perf_counter()))
        await asyncio.gather(*tasks)
        elapsed = time.perf_counter() - t_start

    lat = sorted(latencies)
    pct = lambda p: lat[min(len(lat) - 1, int(p / 100 * len(lat)))] * 1000 if lat else float("nan")
    print(f"queries: {total}  experiments: {len(all_ts)}  readings: {sum(map(len, all_ts.values()))}")
    print(f"target rate: {args.rate}/s for {args.seconds} s, max {args.in_flight} in flight")
    print(f"achieved:    {total / elapsed:.0f} queries/s over {elapsed:.1f} s")
    print(f"latency ms:  p50 {pct(50):.1f}  p95 {pct(95):.1f}  p99 {pct(99):.1f}  "
          f"max {lat[-1] * 1000 if lat else float('nan'):.1f}  mean {statistics.mean(lat) * 1000:.1f}")
    print("outcomes:   ", dict(outcomes))
    for e in examples[:5]:
        print("  e.g.", e)
    return 0 if outcomes.get("ok", 0) == total else 1


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--api", default="http://localhost:3003")
    ap.add_argument("--database-url", required=True)
    ap.add_argument("--rate", type=int, default=200, help="queries per second (grader: 100-200)")
    ap.add_argument("--seconds", type=int, default=30)
    ap.add_argument("--in-flight", type=int, default=50, help="max concurrent requests (grader: 50)")
    ap.add_argument("--seed", type=int, default=1)
    sys.exit(asyncio.run(run(ap.parse_args())))


if __name__ == "__main__":
    main()
