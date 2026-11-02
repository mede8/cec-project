#!/bin/bash
# records container CPU/memory usage during stress-testing
# results go to stress-test/results/<run name>
# start it in a tmux session, after docker compose up -d --build

cd "$(dirname "$0")/.." || exit 1

run="${1:?usage: $0 <run-name>}"
out="stress-test/results/$run"
if [ -e "$out/stats.csv" ]; then
    echo "$out already exists. Please choose a different run name."
    exit 1
fi
mkdir -p "$out"

start=$(date -u +%Y-%m-%dT%H:%M:%SZ)
echo "$start" > "$out/start_time"

# what was tested in this run
{
    echo "start (UTC): $start"
    echo "git: $(git rev-parse --abbrev-ref HEAD) $(git rev-parse --short HEAD), $(git status --porcelain | wc -l) uncommitted file(s)"
    echo "host: $(uname -m), $(nproc) cores, $(free -m | awk '/^Mem:/{print $2}') MB RAM"
    tok=$(curl -s -m 1 -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token)
    echo "instance: $(curl -s -m 1 -H "X-aws-ec2-metadata-token: $tok" http://169.254.169.254/latest/meta-data/instance-type || echo unknown)"
    echo "settings in .env (secrets are not copied):"
    grep -E '^(TOPIC|GROUP_ID|OFFSET_RESET|NOTIFY_URL|NOTIFY_WORKERS|API_WORKERS|API_POOL_SIZE|FETCH_WAIT_MAX_MS|SLOW_REQUEST_MS)=' .env | sed 's/^/  /'
    echo "api container: WEB_CONCURRENCY (uvicorn workers)=$(docker compose exec -T api printenv WEB_CONCURRENCY) DB_POOL_SIZE=$(docker compose exec -T api printenv DB_POOL_SIZE)"
    echo "containers:"
    docker ps --format '  {{.Names}}  {{.Image}}  {{.Status}}'
} > "$out/run_info.txt"
cat "$out/run_info.txt"

# background recorders
pids=()
docker compose logs -f --no-color --since "$start" consumer > "$out/consumer.log" 2>&1 & pids+=($!)
docker compose logs -f --no-color --since "$start" api > "$out/api.log" 2>&1 & pids+=($!)
docker logs -f --since "$start" notifications-service > "$out/notifications.log" 2>&1 & pids+=($!)
vmstat -n 1 | while read -r line; do printf '%(%s)T %s\n' -1 "$line"; done > "$out/vmstat.txt" & pids+=($!)

finished=0
finish() {
    [ "$finished" = 1] && return
    finished=1
    echo
    echo "stopping - saving to $out ..."
    date -u +%Y-%m-%dT%H:%M:%SZ > "$out/end_time"
    kill "${pids[@]}" 2>/dev/null
    wait 2>/dev/null
    docker logs --since "$start" experiment-producer > "$out/producer.log" 2>&1
    {
        echo "after the run:"
        docker ps -a --format '  {{.Names}} {{.Status}}'
        for c in $(docker ps -aq); do
            docker inspect -f '  {{.Name}} restarts={{.RestartCount}} oom_killed={{.State.OOMKilled}}' "$c"
        done
     } >> "$out/run_info.txt"
    python3 stress-test/analyse.py "$out"
    exit 0
}
trap finish INT TERM HUP

echo "recording to $out - press control + C when the test is over"
echo "time,container,cpu_pct,mem_pct" > "$out/stats.csv"
while true; do
    ts=$(date +%s)
    # one line per running container
    docker stats --no-stream --format '{{.Name}},{{.CPUPerc}},{{.MemPerc}}' | tr -d '%' | sed "s/^/$ts,/" >> "$out/stats.csv"
    printf '.'
done
