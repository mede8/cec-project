#!/bin/bash
# records container CPU/memory usage during stress-testing
# results go to stress-test/results/<run name>

set -e
cd "$(dirname "$0")/.."

run="${1:?usage: $0 <run-name>}"
out="stress-test/results/$run"
mkdir -p "$out"

start=$(date -u +%Y-%m-%dT%H:%M:%SZ)
echo "$start" > "$out/start_time"
echo "time,container,cpu_pct,mem_pct" > "$out/stats.csv"

finish() {
    echo
    echo "stopping - saving logs to $out ..."
    date -u +%Y-%m-%dT%H:%M:%SZ > "$out/end_time"
    docker compose logs --no-color --since "$start" consumer > "$out/consumer.log" 2>&1 || true
    docker compose logs --no-color --since "$start" api > "$out/api.log" 2>&1 || true
    docker logs --since "$start" notifications-service > "$out/notifications.log" 2>&1 || true
    echo "done. analyse with: python3 stress-test/analyse.py $out"
    exit 0
}
trap finish INT TERM

echo "recording to $out - press control + C when the test is over"
while true; do
    ts=$(date +%s)
    # one line per running container
    docker stats --no-stream --format '{{.Name}},{{.CPUPerc}},{{.MemPerc}}' | tr -d '%' | sed "s/^/$ts,/" >> "$out/stats.csv"
done
