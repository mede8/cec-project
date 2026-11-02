"""Per-time-window table of a k6 run, to see at which request rate the API breaks.

usage: python3 stress-test/k6_stages.py <k6.csv or k6.csv.gz> [window_s]

Make the input with  k6 run --out csv=<run>/k6.csv.gz ...  (gzip keeps it ~10x smaller;
an uncompressed 1,000 requests/s run is over 1 GB).

Columns: UTC clock time (to line up with analyse.py), achieved requests/s, median / p95 / p99 /
max latency (ms), error %, how the failures split (status 0 = no HTTP answer at all: EOF,
connection reset/refused, timeout; 5xx = the API answered with an error, e.g. 503 when no
database connection was free), dropped iterations (requests k6 could not start because all
VUs were busy) and VUs in use.
The first window that breaks a threshold (p95 >= 300 ms, errors >= 1 %, any drops) is marked.
"""
import csv
import gzip
import sys
from collections import defaultdict
from datetime import datetime, timezone


P95_LIMIT_MS = 300
ERROR_LIMIT = 0.01


def pct(sorted_values, p):
    if not sorted_values:
        return float("nan")
    k = max(0, min(len(sorted_values) - 1, round(p / 100 * len(sorted_values)) - 1))
    return sorted_values[k]


def main(path, window):
    opener = gzip.open if path.endswith(".gz") else open
    dur = defaultdict(list)
    failed = defaultdict(int)
    dropped = defaultdict(float)
    vus = defaultdict(float)
    no_answer = defaultdict(int)     # status 0
    server_err = defaultdict(int)    # status 5xx
    t0 = None
    with opener(path, "rt", newline="") as fh:
        for row in csv.DictReader(fh):
            metric = row["metric_name"]
            if metric not in ("http_req_duration", "http_req_failed", "dropped_iterations", "vus"):
                continue
            t = int(float(row["timestamp"]))
            t0 = t if t0 is None else min(t0, t)
            if row.get("name") == "setup":
                continue
            b = t // window
            v = float(row["metric_value"])
            if metric == "http_req_duration":
                dur[b].append(v)
                status = row.get("status") or "0"
                if status == "0":
                    no_answer[b] += 1
                elif status.startswith("5"):
                    server_err[b] += 1
            elif metric == "http_req_failed":
                failed[b] += v
            elif metric == "dropped_iterations":
                dropped[b] += v
            else:
                vus[b] = max(vus[b], v)

    if not dur:
        sys.exit("no http_req_duration rows found - is this a k6 --out csv file?")
    first_bin = min(dur)
    print(f"{'UTC':>8} {'t (s)':>6} {'req/s':>7} {'median':>7} {'p95':>7} {'p99':>7} {'max':>7} "
          f"{'err %':>6} {'no-ans':>6} {'5xx':>5} {'dropped':>7} {'VUs':>5}")
    broken = False
    for b in sorted(set(dur) | set(dropped)):
        d = sorted(dur.get(b, []))
        n = len(d)
        err = failed[b] / n if n else 0.0
        p95 = pct(d, 95)
        mark = ""
        if not broken and (p95 >= P95_LIMIT_MS or err >= ERROR_LIMIT or dropped[b] > 0):
            broken = True
            mark = "  <- first window over the limits"
        utc = datetime.fromtimestamp(b * window, timezone.utc).strftime("%H:%M:%S")
        print(f"{utc:>8} {(b - first_bin) * window:>6} {n / window:>7.0f} {pct(d, 50):>7.1f} "
              f"{p95:>7.1f} {pct(d, 99):>7.1f} {(d[-1] if d else float('nan')):>7.1f} "
              f"{100 * err:>6.2f} {no_answer[b]:>6} {server_err[b]:>5} "
              f"{dropped[b]:>7.0f} {vus[b]:>5.0f}{mark}")
    total = sum(len(v) for v in dur.values())
    all_d = sorted(x for v in dur.values() for x in v)
    print(f"\ntotal {total} requests, median {pct(all_d, 50):.1f} ms, p95 {pct(all_d, 95):.1f} ms, "
          f"errors {sum(failed.values()):.0f} (no answer {sum(no_answer.values())}, "
          f"5xx {sum(server_err.values())}), dropped {sum(dropped.values()):.0f}"
          + ("" if broken else "  - no window over the limits"))


if __name__ == "__main__":
    if len(sys.argv) not in (2, 3):
        sys.exit(__doc__)
    main(sys.argv[1], int(sys.argv[2]) if len(sys.argv) == 3 else 30)
