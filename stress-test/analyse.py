"""Summarises one run recorded by monitor.sh.

usage: python3 stress-test/analyse.py stress-test/results/<run-name> [window_s]

Reads stats.csv (container CPU/memory), vmstat.txt (host CPU), consumer.log
(lag and notification lines), api.log and run_info.txt. Prints an overall summary,
then a table per time window (default 30 s).
Writes the same text to <run-folder>/summary.txt.

Parts of this script were created with AI assistance (Copilot).
"""
import csv
import re
import statistics
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path


# targets
LAG_TARGET_S = 0.08
NOTIFY_LIMIT_MS = 10_000

# consumer log lines
TIME = r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d+"
LAG_NEW = re.compile(TIME + r".*\blag avg=([\d.]+)s p95=([\d.]+)s max=([\d.]+)s n=(\d+)")
LAG_OLD = re.compile(TIME + r".*\blag ([\d.]+)s")
NOTIFY = re.compile(TIME + r".*\bnotify (\S+) .*-> (\d{3}) latency=(\S+?)ms post=([\d.]+)ms")
NOTIFY_RETRY = re.compile(r"notify attempt \d+ failed")
NOTIFY_GAVE_UP = re.compile(r"giving up on notification")


def epoch(text: str) -> float:
    return datetime.strptime(text, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).timestamp()


def clock(t: float) -> str:
    return datetime.fromtimestamp(t, timezone.utc).strftime("%H:%M:%S")


def pct(values: list, p: float) -> float:
    """p-th percentile, nearest-rank method."""
    if not values:
        return float("nan")
    s = sorted(values)
    return s[max(0, min(len(s) - 1, round(p / 100 * len(s)) - 1))]


def describe(values: list, unit: str = "") -> str:
    if not values:
        return "no data"
    return (f"n={len(values)}  max={max(values):.2f}{unit}  p95={pct(values, 95):.2f}{unit}  "
            f"avg={statistics.mean(values):.2f}{unit}")


def read(folder: Path, name: str) -> str:
    f = folder / name
    return f.read_text(errors="replace") if f.exists() else ""


def short(container: str) -> str:
    """cec-project-consumer-1 -> consumer"""
    return re.sub(r"^.*?-(consumer|api|db)-\d+$", r"\1", container)


def main(folder: Path, window: int) -> None:
    out = [f"Run: {folder.name}"]
    for name in ("start_time", "end_time"):
        if (folder / name).exists():
            out.append(f"{name}: {read(folder, name).strip()}")
    missing = [n for n in ("stats.csv", "consumer.log", "vmstat.txt") if not (folder / n).exists()]
    if missing:
        out.append(f"WARNING: missing files {missing} - was monitor.sh stopped with Ctrl+C?")

    # window -> metric -> values, filled by every section below
    win = defaultdict(lambda: defaultdict(list))

    # container CPU/memory
    cpu, mem = defaultdict(list), defaultdict(list)
    bad_rows = 0
    if (folder / "stats.csv").exists():
        with (folder / "stats.csv").open() as fh:
            for row in csv.DictReader(fh):
                try:
                    t, c, m = int(row["time"]), float(row["cpu_pct"]), float(row["mem_pct"])
                except (ValueError, KeyError, TypeError):
                    bad_rows += 1
                    continue
                name = short(row["container"])
                cpu[name].append(c)
                mem[name].append(m)
                win[t // window][f"cpu {name}"].append(c)
    out.append("\nContainer CPU % (100% = one full core; above 100% = several cores):")
    for c in sorted(cpu):
        out.append(f"  {c:28s} {describe(cpu[c], '%')}   mem max={max(mem[c]):.1f}%")
    if bad_rows:
        out.append(f"  ({bad_rows} unreadable sample lines skipped)")

    # host CPU
    busy, steal, iowait = [], [], []
    header, skip_first = None, True
    for line in read(folder, "vmstat.txt").splitlines():
        parts = line.split()
        if len(parts) < 2:
            continue
        if "us" in parts and "id" in parts:
            header = parts[1:]
            continue
        if header is None or not parts[1].isdigit():
            continue
        if skip_first:
            skip_first = False
            continue
        v = dict(zip(header, map(int, parts[1:])))
        t = int(parts[0])
        busy.append(100 - v["id"])
        steal.append(v.get("st", 0))
        iowait.append(v.get("wa", 0))
        win[t // window]["host busy"].append(100 - v["id"])
        win[t // window]["host steal"].append(v.get("st", 0))
    out.append("\nHost CPU % of ALL cores (vmstat):")
    out.append(f"  busy (100 - idle)            {describe(busy, '%')}")
    out.append(f"  steal (taken by AWS)         {describe(steal, '%')}")
    out.append(f"  iowait (waiting for disk)    {describe(iowait, '%')}")

    # consumer lag
    text = read(folder, "consumer.log")
    lag_avg, lag_n, lag_p95, lag_max = [], [], [], []
    for m in LAG_NEW.finditer(text):
        t = epoch(m[1])
        a, p95, mx, n = float(m[2]), float(m[3]), float(m[4]), int(m[5])
        lag_avg.append(a)
        lag_p95.append(p95)
        lag_max.append(mx)
        lag_n.append(n)
        win[t // window]["lag avg"].append(a)
        win[t // window]["lag max"].append(mx)
    old = [float(m[2]) for m in LAG_OLD.finditer(text)]
    out.append("\nConsumer lag (now - producer timestamp, per sensor event):")
    if lag_n:
        overall = sum(a * n for a, n in zip(lag_avg, lag_n)) / sum(lag_n)
        verdict = "meets" if overall <= LAG_TARGET_S else "MISSES"
        out.append(f"  events measured         {sum(lag_n)} in {len(lag_n)} log lines")
        out.append(f"  overall average         {overall:.3f}s  ({verdict} the {LAG_TARGET_S}s target)")
        out.append(f"  per-line average        {describe(lag_avg, 's')}")
        out.append(f"  per-line p95            {describe(lag_p95, 's')}")
        out.append(f"  worst single event      {max(lag_max):.3f}s")
    elif old:
        out.append(f"  (old 'lag X.XXs' format) {describe(old, 's')}")
    else:
        out.append("  no lag lines found - is the producer running, and is the topic in .env the producer's topic?")

    # notifications
    statuses, types = defaultdict(int), defaultdict(int)
    latency, post = [], []
    for m in NOTIFY.finditer(text):
        t = epoch(m[1])
        types[m[2]] += 1
        statuses[m[3]] += 1
        post.append(float(m[5]))
        if m[4] != "-":
            latency.append(float(m[4]))
            win[t // window]["notify ms"].append(float(m[4]))
    out.append("\nNotifications:")
    out.append(f"  delivered by HTTP status {dict(statuses) or 'none found'}   by type {dict(types)}")
    out.append(f"  retried attempts        {len(NOTIFY_RETRY.findall(text))}")
    out.append(f"  given up (lost)         {len(NOTIFY_GAVE_UP.findall(text))}")
    out.append(f"  end-to-end latency      {describe(latency, 'ms')}  (producer timestamp -> 2xx)")
    out.append(f"  HTTP POST only          {describe(post, 'ms')}")
    if latency:
        out.append(f"  over the 10 s limit     {sum(v > NOTIFY_LIMIT_MS for v in latency)}")

    # errors in the log
    api = read(folder, "api.log")
    out.append("\nLog problems:")
    errors = len(re.findall(r"\b(?:ERROR|Traceback)\b", text))
    out.append(f"  consumer ERROR/Traceback lines           {errors}")
    for label, pattern in [("API slow requests", r"slow request"),
                           ("API 5xx (failed request)", r"failed request"),
                           ("API database unavailable / pool timeout", r"database unavailable"),
                           ("API query cancelled (statement_timeout)", r"query cancelled"),
                           ("API ERROR/Traceback lines", r"\b(?:ERROR|Traceback)\b"),
                           ("API worker (re)starts during the run", r"Application startup complete")]:
        out.append(f"  {label:40s} {len(re.findall(pattern, api))}")
    after = read(folder, "run_info.txt").split("after the run:")
    if len(after) == 2:
        out.append("\nContainers after the run (from run_info.txt):" + after[1].rstrip())

    # per-window summary table
    cols = ([f"cpu {c}" for c in ("api", "db", "consumer") if c in cpu]
            + ["host busy", "host steal", "lag avg", "lag max", "notify ms"])
    out.append(f"\nPer {window} s window (UTC clock; CPU/lag = mean in window, lag max/notify = max):")
    out.append("  time      " + "".join(f"{c:>13s}" for c in cols))
    for b in sorted(win):
        cells = []
        for c in cols:
            v = win[b].get(c)
            if not v:
                cells.append(f"{'-':>13s}")
            elif c in ("lag max", "notify ms"):
                cells.append(f"{max(v):>13.2f}")
            else:
                cells.append(f"{statistics.mean(v):>13.2f}")
        out.append(f"  {clock(b * window)}  " + "".join(cells))

    summary = "\n".join(out)
    print(summary)
    (folder / "summary.txt").write_text(summary + "\n")


if __name__ == "__main__":
    if len(sys.argv) not in (2, 3):
        sys.exit(__doc__)
    main(Path(sys.argv[1]), int(sys.argv[2]) if len(sys.argv) == 3 else 30)
