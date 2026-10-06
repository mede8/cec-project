# REST API

Reads the temperatures the consumer stores in Postgres and serves them on
**port 3003**. It only reads: every database session is read-only.

```
api/
├── Dockerfile            python:3.11-slim, non-root, port 3003
├── requirements.txt
├── app/
│   ├── main.py           FastAPI app: endpoints, input checks, errors
│   └── db.py             all SQL (reads temperatures / experiments)
├── tests/                pytest against a real Postgres built from db/init.sql
│   ├── test_api.py
│   └── test_with_consumer.py   runs the consumer's own state.py + db.py
└── scripts/
    ├── verify.py         checks a running API end to end
    └── seed_demo_data.py demo data in our schema
```

## Endpoints

| Endpoint | Parameters | Returns |
|---|---|---|
| `GET /temperature` | `experiment-id`, `start-time`, `end-time` (+ optional `limit`, `offset`) | readings with start ≤ timestamp ≤ end, oldest first |
| `GET /temperature/out-of-range` | `experiment-id` (+ optional `start-time`, `end-time`, `limit`, `offset`) | readings the consumer flagged `out_of_range` |
| `GET /temperature/series` | `experiment-id`, `start-time`, `end-time`, `interval` | avg/min/max per time bucket (charts) |
| `GET /experiments` | optional `phase` | all experiments |
| `GET /experiments/{id}` | | details (null fields if the consumer stored none) |
| `GET /experiments/{id}/stats` | optional `start-time`, `end-time` | count, min, max, avg, out-of-range count |
| `GET /health` / `GET /health/live` | | 503 if the DB is down / process alive |
| `GET /docs` | | interactive documentation |

Both temperature endpoints return
`[{"timestamp": 1790000110.0, "temperature": 23.5}, ...]`.

- **Times** can be epoch seconds (`1790000000`) or ISO-8601
  (`2026-10-05T12:00:00Z`); responses use epoch seconds (what `ts` stores).
- **Unknown experiment** → `200 []` on the temperature endpoints (it may simply
  have no readings yet), `404` on `/experiments/{id}`.
- **Errors:** `400` start after end, `422` missing/invalid parameter,
  `503` database unavailable or query too slow. Every response has `X-Request-ID`.

## How the API depends on the consumer

The API reads `temperatures` (written by `consumer/db.py`) and trusts its
`out_of_range` flag, so the API always agrees with the notifications the
consumer sends. Readings exist only for the **running** phase, because that is
all the consumer saves.

The `experiments` table is optional: **the consumer currently never writes
to it**, so the API also lists experiments that only appear in
`temperatures` (with null details). If the consumer starts filling
`experiments`, details and the `phase` filter work automatically.

## Run

```bash
docker compose up --build -d                # from the repo root, needs .env
curl "http://localhost:3003/temperature/out-of-range?experiment-id=<id>"
```

## Test

```bash
cd api
pip install -r requirements-dev.txt
# an EMPTY throwaway database; its tables are dropped and recreated
TEST_DATABASE_URL=postgresql://USER:PASSWORD@localhost:5432/api_test pytest
```

`test_with_consumer.py` feeds Kafka-style events through the consumer's real
`state.py` and `db.py` and checks the API's answers. One test is marked
`xfail`: it documents a known consumer issue (a duplicate OutOfRange
notification when Kafka redelivers an old measurement). It starts passing
when that is fixed; then remove the marker.

Check a running stack (Postgres must be reachable, e.g.
`ports: ["127.0.0.1:5432:5432"]` on the `db` service):

```bash
python api/scripts/verify.py --database-url "postgresql://USER:PASSWORD@localhost:5432/DB"
```

## Run on EC2

```bash
# Amazon Linux 2023 (Ubuntu: apt install docker.io docker-compose-v2)
sudo dnf install -y docker git && sudo systemctl enable --now docker
sudo usermod -aG docker ec2-user && newgrp docker
# compose plugin, if `docker compose version` fails:
sudo mkdir -p /usr/local/lib/docker/cli-plugins
sudo curl -SL https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64 \
     -o /usr/local/lib/docker/cli-plugins/docker-compose && sudo chmod +x /usr/local/lib/docker/cli-plugins/docker-compose

cd cec-project-main
printf 'POSTGRES_USER=teamuser\nPOSTGRES_PASSWORD=change-me\nPOSTGRES_DB=observability\n' > .env
docker compose up -d --build
curl localhost:3003/health                      # {"status":"ok"}
```

Open TCP 3003 in the instance's security group (only to the load generator's
IP if possible). Keep 5432 closed.

One uvicorn worker is the default. On a 2-vCPU instance, 2 or 4 workers were
*slower* in our load test (Postgres needs the CPU), so only add
`WEB_CONCURRENCY: "2"` (uvicorn reads it) to the api service's `environment` on bigger instances.

Measured on 2 vCPUs, 60 experiments / 108,120 readings, grader-like load
(`scripts/loadtest.py`, 200 queries/s, 50 in flight): 6000/6000 correct,
p50 135 ms, p95 515 ms, p99 814 ms.
