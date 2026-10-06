#!/usr/bin/env python3
"""Check that the RUNNING API gives correct answers. Run it on the machine
where `docker compose up` is running.

  1. API and database reachable.
  2. Inserts a temporary experiment with known answers into `temperatures`
     (like the consumer does) and checks every endpoint returns exactly them.
  3. Error handling (400 / 404 / 422).
  4. For EVERY experiment in the database: the API's answers must equal what
     is in the table. Also warns if a stored out_of_range flag disagrees with
     the thresholds in `experiments` (that would be a consumer problem).
  5. Removes the temporary experiment again, even if a check fails.

    pip install "psycopg[binary]"
    python api/scripts/verify.py --database-url "postgresql://USER:PASSWORD@localhost:5432/DB"

The database port must be reachable from where you run this: add
`ports: ["127.0.0.1:5432:5432"]` to the db service, or run it inside a container.
Exit code 0 only if every check passed (warnings do not fail the run).
"""

import argparse
import json
import sys
import urllib.error
import urllib.parse
import urllib.request
import uuid

import psycopg

results, warnings = [], []


def check(ok, label, detail=""):
    results.append((ok, label))
    print(f"  [{'PASS' if ok else 'FAIL'}] {label}" + ("" if ok or not detail else f"\n         {detail}"))
    return ok


def warn(label):
    warnings.append(label)
    print(f"  [WARN] {label}")


def get(api, path, params=None, headers=None):
    url = api + path + ("?" + urllib.parse.urlencode(params) if params else "")
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers=headers or {}), timeout=15) as r:
            return r.status, json.loads(r.read() or b"null"), r.headers
    except urllib.error.HTTPError as e:
        body = e.read()
        try:
            body = json.loads(body)
        except ValueError:
            pass
        return e.code, body, e.headers


def pairs(rows):
    return [(r["timestamp"], round(r["temperature"], 6)) for r in rows]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--api", default="http://localhost:3003")
    ap.add_argument("--database-url", required=True)
    a = ap.parse_args()
    api = a.api.rstrip("/")

    print("\n1. Is everything running?")
    try:
        s, b, _ = get(api, "/health")
    except OSError as e:
        check(False, f"API reachable at {api}", str(e))
        return 1
    check(s == 200 and b == {"status": "ok"}, "API is up and can reach the database", f"got {s} {b}")
    try:
        conn = psycopg.connect(a.database_url, autocommit=True)
    except psycopg.Error as e:
        check(False, "database reachable with --database-url", str(e))
        return 1
    check(True, "database reachable with --database-url")

    exp = f"verify-{uuid.uuid4().hex[:8]}"
    rows = [  # measurement_id, ts, temperature, out_of_range (as the consumer stores it)
        ("m1", 100.0, 21.0, False), ("m2", 110.0, 23.5, True), ("m3", 120.0, 20.0, False),
        ("m4", 130.0, 22.0, False), ("m5", 140.0, 19.2, True), ("m6", 150.0, 21.5, False)]
    print(f"\n2. Known-answer checks (temporary experiment '{exp}')")
    try:
        with conn.cursor() as cur:
            cur.executemany("INSERT INTO temperatures (experiment_id, measurement_id, ts, "
                            "temperature, out_of_range) VALUES (%s, %s, %s, %s, %s)",
                            [(exp, *r) for r in rows])
        s, b, _ = get(api, "/temperature", {"experiment-id": exp, "start-time": 110, "end-time": 130})
        check(s == 200 and pairs(b) == [(110.0, 23.5), (120.0, 20.0), (130.0, 22.0)],
              "time range: exactly the readings in [110, 130], oldest first", f"got {s} {b}")
        s, b, _ = get(api, "/temperature", {"experiment-id": exp, "start-time": "1970-01-01T00:01:40Z",
                                            "end-time": "1970-01-01T00:01:50Z"})
        check(s == 200 and pairs(b) == [(100.0, 21.0), (110.0, 23.5)],
              "time range: ISO-8601 times accepted", f"got {s} {b}")
        s, b, _ = get(api, "/temperature/out-of-range", {"experiment-id": exp})
        check(s == 200 and pairs(b) == [(110.0, 23.5), (140.0, 19.2)],
              "out-of-range: exactly the 2 flagged readings", f"got {s} {b}")
        s, b, _ = get(api, f"/experiments/{exp}/stats")
        check(s == 200 and b["count"] == 6 and b["out_of_range_count"] == 2
              and b["min_temperature"] == 19.2 and b["max_temperature"] == 23.5,
              "stats: count, min, max and out-of-range count", f"got {s} {b}")
        s, b, _ = get(api, "/temperature", {"experiment-id": exp, "start-time": 0, "end-time": 1000,
                                            "limit": 2, "offset": 2})
        check(s == 200 and [r["timestamp"] for r in b] == [120.0, 130.0], "paging: limit/offset")

        print("\n3. Error handling")
        s, b, _ = get(api, "/temperature/out-of-range", {"experiment-id": "no-such-experiment"})
        check(s == 200 and b == [], "unknown experiment -> 200 []", f"got {s} {b}")
        s, _, h = get(api, "/experiments/no-such-experiment", headers={"X-Request-ID": "verify-404"})
        check(s == 404 and h.get("X-Request-ID") == "verify-404", "unknown experiment details -> 404")
        s, _, _ = get(api, "/temperature", {"experiment-id": exp, "start-time": 200, "end-time": 100})
        check(s == 400, "start-time after end-time -> 400")
        s, _, _ = get(api, "/temperature", {"experiment-id": exp})
        check(s == 422, "missing times -> 422")
        s, _, _ = get(api, "/temperature", {"experiment-id": exp, "start-time": "abc", "end-time": 1})
        check(s == 422, "invalid time -> 422")
    finally:
        conn.execute("DELETE FROM temperatures WHERE experiment_id = %s", (exp,))
    s, b, _ = get(api, "/temperature", {"experiment-id": exp, "start-time": 0, "end-time": 1000})
    check(b == [], "temporary experiment removed")

    print("\n4. Every experiment in the database")
    ids = [r[0] for r in conn.execute("SELECT DISTINCT experiment_id FROM temperatures ORDER BY 1")]
    if not ids:
        print("  (no readings yet: run the consumer or scripts/seed_demo_data.py, then run this again)")
    for eid in ids:
        db_all = [(ts, round(t, 6)) for ts, t in conn.execute(
            "SELECT ts, temperature FROM temperatures WHERE experiment_id = %s "
            "ORDER BY ts, measurement_id", (eid,))]
        db_oor = [(ts, round(t, 6)) for ts, t in conn.execute(
            "SELECT ts, temperature FROM temperatures WHERE experiment_id = %s AND out_of_range "
            "ORDER BY ts, measurement_id", (eid,))]
        lo, hi = db_all[0][0], db_all[-1][0]
        s1, api_all, _ = get(api, "/temperature", {"experiment-id": eid, "start-time": lo, "end-time": hi})
        s2, api_oor, _ = get(api, "/temperature/out-of-range", {"experiment-id": eid})
        check(s1 == 200 and s2 == 200 and pairs(api_all) == db_all and pairs(api_oor) == db_oor,
              f"{eid}: {len(db_all)} readings, {len(db_oor)} out of range (API matches table)",
              f"API gave {len(api_all) if isinstance(api_all, list) else api_all} / "
              f"{len(api_oor) if isinstance(api_oor, list) else api_oor}")
        thr = conn.execute("SELECT lower_threshold, upper_threshold FROM experiments "
                           "WHERE experiment_id = %s", (eid,)).fetchone()
        if thr:
            lower, upper = thr
            # Thresholds are REAL (32-bit) in init.sql, so values within 1e-4 of a
            # threshold are ambiguous after rounding and are not judged.
            bad = conn.execute(
                "SELECT count(*) FROM temperatures WHERE experiment_id = %(e)s "
                "AND abs(temperature - %(lo)s) > 1e-4 AND abs(temperature - %(hi)s) > 1e-4 "
                "AND out_of_range <> NOT (temperature BETWEEN %(lo)s AND %(hi)s)",
                {"e": eid, "lo": lower, "hi": upper}).fetchone()[0]
            if bad:
                warn(f"{eid}: {bad} stored out_of_range flags disagree with thresholds "
                     f"{lower}-{upper} (consumer issue, not API)")

    conn.close()
    failed = [l for ok, l in results if not ok]
    print(f"\n{len(results) - len(failed)}/{len(results)} checks passed"
          + (f", {len(warnings)} warning(s)." if warnings else "."))
    if failed:
        print("FAILED:\n  - " + "\n  - ".join(failed))
        return 1
    print("Everything works.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
