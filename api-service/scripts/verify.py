#!/usr/bin/env python3
"""Check that a RUNNING API gives correct answers. Run it on your own machine.

What it does:
  1. Checks the API and the database are reachable.
  2. Inserts a small experiment whose correct answers are known in advance
     (warm-up readings, readings exactly on the thresholds, excursions),
     then checks every endpoint returns exactly those answers.
  3. Checks the error cases: 400, 404 and 422.
  4. Cross-checks EVERY experiment already in the database: it computes the
     out-of-range readings itself in Python, directly from the tables, and
     compares them with what the API returns. This checks the API against
     an independent calculation, not against itself.
  5. Deletes the test experiment again, even if a check fails.

Usage (after `docker compose up`):

    pip install "psycopg[binary]"
    python scripts/verify.py \
        --database-url "postgresql://postgres:<POSTGRES_PASSWORD>@localhost:5432/observability"

The database URL is the OWNER account (it inserts the test experiment).
Prints PASS/FAIL per check; the exit code is 0 only if everything passed.
"""

import argparse
import json
import sys
import urllib.error
import urllib.parse
import urllib.request
import uuid

import psycopg

results: list[tuple[bool, str]] = []


def check(ok: bool, label: str, detail: str = "") -> bool:
    results.append((ok, label))
    mark = "PASS" if ok else "FAIL"
    print(f"  [{mark}] {label}" + ("" if ok or not detail else f"\n         {detail}"))
    return ok


def get(api: str, path: str, params: dict | None = None, headers: dict | None = None):
    url = api + path + ("?" + urllib.parse.urlencode(params) if params else "")
    req = urllib.request.Request(url, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            body = r.read().decode()
            return r.status, (json.loads(body) if "json" in r.headers.get("Content-Type", "") else body), r.headers
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        try:
            body = json.loads(body)
        except ValueError:
            pass
        return e.code, body, e.headers


def approx(a, b, tol=1e-6):
    return a is not None and b is not None and abs(a - b) <= tol


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--api", default="http://localhost:8000")
    ap.add_argument("--database-url", required=True, help="owner account URL")
    args = ap.parse_args()
    api = args.api.rstrip("/")

    # ---- 1. reachability ---------------------------------------------------------
    print("\n1. Is everything running?")
    try:
        status, body, _ = get(api, "/health")
    except OSError as e:
        check(False, f"API reachable at {api}", str(e))
        print("\nStart it first: docker compose up -d   (then wait ~10 s)")
        return 1
    check(status == 200 and body == {"status": "ok"}, "API is up and can reach Postgres",
          f"got {status} {body}")
    check(get(api, "/health/live")[0] == 200, "liveness endpoint answers")
    try:
        conn = psycopg.connect(args.database_url, autocommit=True)
    except psycopg.Error as e:
        check(False, "database reachable with --database-url", str(e))
        print("\nIs Postgres's port published? See 'Quick start' in the README.")
        return 1
    check(True, "database reachable with --database-url")

    # ---- 2. known-answer experiment ---------------------------------------------------
    exp = f"verify-{uuid.uuid4().hex[:8]}"
    # thresholds 20..22, experiment starts at t=100
    rows = [
        ("w1", 10.0, 15.0),   # warm-up, below    -> NOT a violation
        ("w2", 50.0, 30.0),   # warm-up, above    -> NOT a violation
        ("m1", 100.0, 23.5),  # at start, above   -> violation
        ("m2", 110.0, 21.0),  # in range
        ("m3", 120.0, 20.0),  # exactly lower     -> in range
        ("m4", 130.0, 22.0),  # exactly upper     -> in range
        ("m5", 140.0, 19.2),  # below             -> violation
        ("m6", 150.0, 21.5),  # in range
    ]
    print(f"\n2. Known-answer checks (temporary experiment '{exp}')")
    try:
        conn.execute(
            "INSERT INTO experiment_config (experiment_id, researcher, sensors, "
            "lower_threshold, upper_threshold, status, started_at) "
            "VALUES (%s, 'verify@example.org', %s, 20.0, 22.0, 'running', 100.0)",
            (exp, ["a", "b"]),
        )
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO measurements (experiment_id, measurement_id, ts, temperature) "
                "VALUES (%s, %s, %s, %s)",
                [(exp, m, ts, t) for m, ts, t in rows],
            )

        s, b, _ = get(api, "/temperature",
                      {"experiment-id": exp, "start-time": 100, "end-time": 130})
        want = [{"timestamp": 100.0, "temperature": 23.5},
                {"timestamp": 110.0, "temperature": 21.0},
                {"timestamp": 120.0, "temperature": 20.0},
                {"timestamp": 130.0, "temperature": 22.0}]
        check(s == 200 and b == want,
              "time-range: returns exactly the readings in [100, 130], in order",
              f"got {s} {b}")

        s, b, _ = get(api, "/temperature",
                      {"experiment-id": exp, "start-time": 0, "end-time": 1000})
        check(s == 200 and len(b) == 8, "time-range: includes warm-up readings",
              f"got {len(b) if isinstance(b, list) else b}")

        s, b, _ = get(api, "/temperature/out-of-range", {"experiment-id": exp})
        want = [{"timestamp": 100.0, "temperature": 23.5},
                {"timestamp": 140.0, "temperature": 19.2}]
        check(s == 200 and b == want,
              "out-of-range: exactly the 2 violations (warm-up and on-threshold excluded)",
              f"got {s} {b}")

        s, b, _ = get(api, f"/experiments/{exp}/stats")
        ok = (s == 200 and b["count"] == 8 and b["min_temperature"] == 15.0
              and b["max_temperature"] == 30.0
              and approx(b["avg_temperature"], sum(r[2] for r in rows) / 8)
              and b["below_lower_count"] == 1 and b["above_upper_count"] == 1
              and b["out_of_range_count"] == 2)
        check(ok, "stats: count, min, max, average and violation counts correct",
              f"got {s} {b}")

        s, b, _ = get(api, "/temperature/series",
                      {"experiment-id": exp, "start-time": 100, "end-time": 150,
                       "interval": 25})
        # buckets [100,125): 23.5,21,20  [125,150): 22,19.2  [150,175): 21.5
        ok = (s == 200 and [x["bucket_start"] for x in b] == [100.0, 125.0, 150.0]
              and [x["count"] for x in b] == [3, 2, 1]
              and approx(b[0]["avg_temperature"], (23.5 + 21 + 20) / 3))
        check(ok, "series: bucket starts, counts and averages correct", f"got {s} {b}")

        s, b, _ = get(api, "/temperature",
                      {"experiment-id": exp, "start-time": 0, "end-time": 1000,
                       "limit": 3, "offset": 3})
        check(s == 200 and [x["timestamp"] for x in b] == [110.0, 120.0, 130.0],
              "paging: limit=3 offset=3 returns readings 4-6", f"got {s} {b}")

        s, b, _ = get(api, f"/experiments/{exp}")
        check(s == 200 and b["started_at"] == 100.0 and b["lower_threshold"] == 20.0,
              "experiment details returned", f"got {s} {b}")

        # ---- 3. error handling ------------------------------------------------------------
        print("\n3. Error handling")
        s, b, h = get(api, "/temperature/out-of-range", {"experiment-id": "no-such-exp"},
                      headers={"X-Request-ID": "verify-404"})
        check(s == 404, "unknown experiment -> 404", f"got {s} {b}")
        check(h.get("X-Request-ID") == "verify-404", "request ID echoed on errors")
        s, b, _ = get(api, "/temperature",
                      {"experiment-id": exp, "start-time": 200, "end-time": 100})
        check(s == 400, "start-time after end-time -> 400", f"got {s} {b}")
        s, b, _ = get(api, "/temperature", {"experiment-id": exp})
        check(s == 422, "missing start-time/end-time -> 422", f"got {s} {b}")
        s, b, _ = get(api, "/temperature",
                      {"experiment-id": exp, "start-time": "abc", "end-time": 1})
        check(s == 422, "non-numeric time -> 422", f"got {s} {b}")
        s, b, _ = get(api, "/metrics")
        check(s == 200 and "api_requests_total" in b, "metrics endpoint works")
    finally:
        conn.execute("DELETE FROM experiment_config WHERE experiment_id = %s", (exp,))

    s, _, _ = get(api, f"/experiments/{exp}")
    check(s == 404, "temporary experiment cleaned up")

    # ---- 4. independent cross-check of all real data ---------------------------------------
    print("\n4. Cross-check every experiment in the database against an independent calculation")
    experiments = conn.execute(
        "SELECT experiment_id, lower_threshold, upper_threshold, started_at "
        "FROM experiment_config ORDER BY experiment_id").fetchall()
    if not experiments:
        print("  (no experiments yet: run scripts/seed_demo_data.py, or let the "
              "consumer ingest some data, then run this again)")
    for exp_id, lower, upper, started in experiments:
        data = conn.execute(
            "SELECT ts, temperature FROM measurements WHERE experiment_id = %s "
            "ORDER BY ts, id", (exp_id,)).fetchall()
        expected = [{"timestamp": ts, "temperature": t} for ts, t in data
                    if started is not None and ts >= started and (t < lower or t > upper)]
        got, offset = [], 0
        while True:  # page through, so experiments of any size work
            s, page, _ = get(api, "/temperature/out-of-range",
                             {"experiment-id": exp_id, "limit": 5000, "offset": offset})
            if s != 200:
                break
            got += page
            if len(page) < 5000:
                break
            offset += 5000
        s_stats, stats, _ = get(api, f"/experiments/{exp_id}/stats")
        ok = (s == 200 and got == expected and s_stats == 200
              and stats["count"] == len(data)
              and stats["out_of_range_count"] == len(expected))
        check(ok, f"{exp_id}: {len(data)} readings, {len(expected)} out of range "
                  "(API matches independent calculation)",
              f"API gave {len(got)} rows, stats {stats.get('out_of_range_count') if isinstance(stats, dict) else stats}")

    conn.close()
    failed = [label for ok, label in results if not ok]
    print(f"\n{len(results) - len(failed)}/{len(results)} checks passed.")
    if failed:
        print("FAILED:\n  - " + "\n  - ".join(failed))
        return 1
    print("Everything works.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
