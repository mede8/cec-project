"""Summarises one recorded test run. It reads what the monitor.sh
saved in stats.csv for easier analysis of the stress-testing.

usage: python3 stress-test/analyse.py stress-test/results/<run-name>
"""
import csv
import re
import statistics
import sys
from collections import defaultdict
from pathlib import Path


def pct(values: list, p) -> list:
    """the p-th percentile of a list, simple nearest-rank method.
    
    :param values: retrieved list of csv values.
    :param p: the 
    """
    if not values:
        return float("nan")
    s = sorted(values)
    k = max(0, min(len(s) - 1, round(p / 100 * len(s)) - 1))
    return s[k]


def describe(values: list, unit: str = "") -> str:
    """Describes the message to be printed from the csv.
    
    :param values: retrieved csv values.
    :param unit: measure unit depending on the message type (e.g., CPU - %, lag - s)
    :returns: the message containing the length, max, p95, and average.
    """
    if not values:
        return "no data"
    return (f"n={len(values)}  max={max(values):.2f}{unit} "
            f"p95={pct(values, 95):.2f}{unit}  avg={statistics.mean(values):.2f}{unit}")


def main(folder: Path) -> None:
    """Main function to analyse the CSV file.
    
    :param folder: path to the generated file.
    """
    lines = [f"Run: {folder.name}"]
    for name in ("start_time", "end_time"):
        f = folder / name
        if f.exists():
            lines.append(f"{name}: {f.read_text().strip()}")

    # CPU
    cpu, mem = defaultdict(list), defaultdict(list)
    stats = folder / "stats.csv"
    if stats.exists():
        with stats.open() as fh:
            for row in csv.DictReader(fh):
                try:
                    c, m = float(row["cpu_pct"]), float(row["mean_pct"])
                except (ValueError, KeyError, TypeError):
                    # skip bad sample lines
                    continue
                cpu[row["container"]].append(c)
                mem[row["container"]].append(m)
    lines.append("\nCPU % per container (100% = one full core):")
    for c in sorted(cpu):
        lines.append(f" {c:40s} {describe(cpu[c], '%')}")
    lines.append("\nMemory % per container:")
    for c in sorted(mem):
        lines.append(f" {c:40s} max={max(mem[c]):.2f}%")

    # consumer log: lag and notifications latency
    log = folder / "consumer.log"
    text = log.read_text(errors="replace") if log.exists() else ""

    lags = [float(m) for m in re.findall(r"lag ([\d.]+)s", text)]
    lines.append(f"\nConsumer lag: {describe(lags, 's')}")

    notif = re.findall(r"-> (\d{3})(?:\s+([\d.]+)\b)?", text)
    statuses = defaultdict(int)
    latencies = []
    for status, latency in notif:
        statuses[status] += 1
        if status == "200" and latency:
            latencies.append(float(latency))
    lines.append(f"\nNotifications by HTTP status: {dict(statuses) or 'none found'}")
    lines.append(f"Notification latency: {describe(latencies, 'ms')}")
    if latencies:
        late = sum(1 for v in latencies if v > 10_000)
        lines.append(f"  over the 10 s requirement: {late}")

    errors = len(re.findall(r"\b(ERROR|Traceback)\b", text))
    lines.append(f"\nError/Traceback lines in consumer log: {errors}")

    summary = "\n".join(lines)
    print(summary)
    (folder / "summary.txt").write_text(summary + "\n")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("usage: python3 stress-test/analyse.py stress-test/results/<run-name>")
    main(Path(sys.argv[1]))
