# Temperature Observability Service: REST API

The query side of the service. The consumer turns Kafka events into rows
in Postgres; this API lets researchers query those rows. It only reads
from the database, and logs in with an account that cannot write.

For how it works, the design decisions, and likely exam questions, see
**[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)**.

```
api/
  app/main.py              wiring: pool, middleware, routers, error handlers
  app/config.py            settings from environment variables
  app/routes/              temperature.py, experiments.py, health.py
  app/db.py                all SQL
  app/models.py            response schemas (also generate /docs)
  app/middleware.py        request IDs, access logs, metrics
  tests/                   51 tests against a real Postgres
  Dockerfile, requirements.txt
db/
  01-init.sql              tables shared with the consumer
  02-api-readonly-role.sh  creates the read-only api_reader login
scripts/seed_demo_data.py  realistic demo data (demo without the consumer)
docs/ARCHITECTURE.md       architecture, design decisions, exam Q&A
docker-compose.yml         postgres + api (consumer block ready to fill in)
```

## Quick start

```bash
cp .env.example .env              # then change both passwords
docker compose up --build -d

# Optional: demo data, so the API works before the consumer is done
pip install "psycopg[binary]"
python scripts/seed_demo_data.py \
    --database-url "postgresql://postgres:<POSTGRES_PASSWORD>@localhost:5432/observability" \
    --experiments 5 --start 1790000000

curl "http://localhost:8000/experiments"
curl "http://localhost:8000/temperature/out-of-range?experiment-id=demo-1"
```

Interactive documentation, where every endpoint can be tried in the
browser: **http://localhost:8000/docs**. It's a useful tool in the live demo.

> The scripts in `db/` only run when the Postgres volume is **empty**. After
> changing them, reset with `docker compose down -v` (this deletes the data).
> Postgres is published on `127.0.0.1:5432` only, so the scripts can
> connect from your machine but other computers can't.

## Running without local Python (Docker only)

Not everyone has Python installed. The API image already contains
`psycopg`, so the two helper scripts can run inside Docker. Run these from
the project folder, with the stack already started
(`docker compose up --build -d`).

**Windows (PowerShell):**

```powershell
copy .env.example .env            # then edit .env and change both passwords

# demo data
docker run --rm --network api-service_default -v "${PWD}\scripts:/scripts" api-service-api python /scripts/seed_demo_data.py --database-url "postgresql://postgres:<POSTGRES_PASSWORD>@postgres:5432/observability" --experiments 5 --start 1790000000

# check everything
docker run --rm --network api-service_default -v "${PWD}\scripts:/scripts" api-service-api python /scripts/verify.py --api http://api:8000 --database-url "postgresql://postgres:<POSTGRES_PASSWORD>@postgres:5432/observability"

curl.exe http://localhost:8000/experiments
```

**Linux / Ubuntu (reads the passwords from `.env`):**

```bash
set -a; . ./.env; set +a

docker run --rm --network api-service_default -v "$PWD/scripts:/scripts" api-service-api python /scripts/seed_demo_data.py --database-url "postgresql://postgres:${POSTGRES_PASSWORD}@postgres:5432/observability" --experiments 5 --start 1790000000

docker run --rm --network api-service_default -v "$PWD/scripts:/scripts" api-service-api python /scripts/verify.py --api http://api:8000 --database-url "postgresql://postgres:${POSTGRES_PASSWORD}@postgres:5432/observability"
```

Notes:

- Inside Docker the database host is `postgres` and the API host is `api`,
  not `localhost`.
- In PowerShell use `curl.exe` (plain `curl` is a different command).
- Use passwords made of letters and numbers only. Symbols such as `@`, `:`,
  `/` and `#` break the connection URL.
- Paste one command at a time. Two commands pasted onto one line fail.

## Running on an Ubuntu VM (AWS)

Tested on Ubuntu 24.04 with Docker Engine and the Compose plugin.

1. **Install Docker** (once per VM):
   ```bash
   curl -fsSL https://get.docker.com -o get-docker.sh
   sudo sh get-docker.sh
   sudo usermod -aG docker $USER      # then log out and back in
   docker run --rm hello-world
   ```
2. **Check the ports are free.** The VM may be shared. This service uses
   `8000` (API) and `5432` (Postgres, local to the VM only):
   ```bash
   ss -tlnp | grep -E ':8000|:5432'   # no output means both are free
   ```
3. **Get the code onto the VM.** Prefer `git clone`:
   ```bash
   git clone <repository-url> api-service && cd api-service
   cp .env.example .env && nano .env  # change both passwords
   chmod 600 .env
   ```
4. **Start it and check it:**
   ```bash
   docker compose up --build -d
   docker compose ps                  # both services: healthy
   curl http://localhost:8000/health  # {"status":"ok"}
   ```
5. **Demo data and checks:** use the Linux commands in the previous section.
6. **See `/docs` from your own computer** with an SSH tunnel, which works
   without opening port 8000 in the AWS security group:
   ```bash
   ssh -i <your-key> -L 8000:localhost:8000 ubuntu@<vm-address>
   ```
   Leave it open and browse to http://localhost:8000/docs.

**Do not** open port `5432` to the internet. If you do open port `8000`,
allow only your own IP address.

## Troubleshooting

| Symptom | Likely cause and fix |
|---|---|
| `failed to connect to the docker API ... npipe` (Windows) | Docker Desktop is not running. Open it, wait until the engine is running, then try `docker info`. |
| `Bad permissions ... UNPROTECTED PRIVATE KEY FILE` (Windows SSH) | Run `icacls .\<key> /inheritance:r`, then `icacls .\<key> /grant:r "${env:USERNAME}:(R)"`. |
| Postgres restarts in a loop with `Permission denied` on `/docker-entrypoint-initdb.d/` | Files copied from Windows with `scp` have folders readable only by the owner. Run `chmod -R a+rX db`, then `docker compose down -v` and `docker compose up -d`. |
| API restarts in a loop with `cannot import name 'experiments' from 'app.routes'` | Same permissions problem, in `api/app`. Run `find . -type d -exec chmod 755 {} +`, then `docker compose build --no-cache api` and `docker compose up -d --force-recreate api`. A normal `docker compose up --build` reuses the cached image and keeps the problem. |
| `password authentication failed for user "postgres"` | The password in the command differs from the one the database was created with. Postgres reads `.env` only on first start: fix `.env`, then `docker compose down -v` and `docker compose up -d` (deletes the data). |
| `curl: (56) Recv failure` right after starting | The API is still starting. Wait 10 to 15 seconds and check `docker compose ps` for `healthy`. |
| `/experiments` returns `[]` | The API works but the database is empty. Seed demo data or start the consumer. |

## Never commit credentials

Do **not** put these in the repository: `.env` (real passwords, already in
`.gitignore`), SSH private keys (`*_rsa`, `*.pem`), Kafka keystores and
truststores (`*.pkcs12`), tokens, and the course-issued credentials folder.
Only `.env.example`, which holds placeholders, belongs in Git.

## How to check it works

Three levels, each catching different problems:

**1. Unit tests: is the code correct?** 51 tests against a real Postgres
(see [Tests](#tests)). Run these after every code change.

**2. `scripts/verify.py`: does the deployed system give correct answers?**
Run it against the running stack. It inserts a small experiment with
known answers (warm-up readings, values exactly on the thresholds,
excursions), checks every endpoint and error case, then cross-checks
every experiment in the database against its own independent Python
calculation, and finally cleans up after itself.

```bash
docker compose up --build -d
pip install "psycopg[binary]"
python scripts/verify.py \
    --database-url "postgresql://postgres:<POSTGRES_PASSWORD>@localhost:5432/observability"
# ... 22/22 checks passed.  Everything works.
```

Exit code 0 means everything passed. The script is proven to catch bugs:
counting a reading exactly on the lower threshold as a violation makes
it fail with exit code 1.

**3. With real data: does it work with the consumer?** Once the consumer
has ingested a real experiment, run `verify.py` again. Section 4 then
checks the API's answers on *real* ingested data. If a count disagrees,
the problem is in what the consumer wrote (for example a missing
`started_at`, or per-sensor rows instead of averages).

### Before the demo

- [ ] `docker compose down -v && docker compose up --build -d` on the demo laptop
- [ ] `verify.py` passes (22/22)
- [ ] The consumer runs and a real experiment appears in `GET /experiments`
- [ ] `verify.py` again: the real experiment passes the cross-check
- [ ] http://localhost:8000/docs opens in the browser
- [ ] The endpoint names and parameters match the assignment brief exactly

## Endpoints

All timestamps are Unix epoch seconds, the same unit as the Kafka events.

### Required by the assignment

| Endpoint | Parameters | Returns |
|---|---|---|
| `GET /temperature` | `experiment-id`, `start-time`, `end-time`; optional `limit`, `offset` | Averaged readings with `start-time <= timestamp <= end-time`, oldest first |
| `GET /temperature/out-of-range` | `experiment-id`; optional `start-time`, `end-time`, `limit`, `offset` | Readings outside the thresholds after the experiment started |

Both return:

```json
[{"timestamp": 1790000410.0, "temperature": 27.93},
 {"timestamp": 1790000415.0, "temperature": 28.02}]
```

**Out-of-range rules:** below `lower_threshold` or above `upper_threshold`.
A value exactly on a threshold is in range. Readings from before
`started_at`, the warm-up during stabilisation, are never violations.

### Additional

| Endpoint | Purpose |
|---|---|
| `GET /temperature/series?experiment-id=&start-time=&end-time=&interval=60` | Average, minimum and maximum per time bucket, for charts |
| `GET /experiments?status=running` | List experiments, optionally filtered by status |
| `GET /experiments/{id}` | Thresholds, sensors, status, start and end times |
| `GET /experiments/{id}/stats?start-time=&end-time=` | Count, min, max, average, and below/above/out-of-range counts |
| `GET /health` | Ready: `503` if the database is unreachable |
| `GET /health/live` | Alive: never touches the database (used by Docker) |
| `GET /metrics` | Prometheus metrics |

### Errors

Every error is `{"detail": "..."}` and carries an `X-Request-ID` header.

| Status | When |
|---|---|
| 400 | `start-time` after `end-time`, `limit` above the maximum, or too many series buckets |
| 404 | Unknown experiment |
| 422 | Missing parameter, wrong type, or unknown status |
| 503 | Database unreachable, or a query exceeded the time limit |

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | built from `POSTGRES_*` | Full connection string, overrides the rest |
| `POSTGRES_HOST` / `_PORT` / `_DB` / `_USER` / `_PASSWORD` | `postgres` / `5432` / `observability` / `postgres` / `postgres` | Connection parts |
| `DB_POOL_SIZE` | `10` | Maximum open connections |
| `DB_CONNECT_TIMEOUT` | `30` | Seconds to wait for Postgres at startup |
| `DB_REQUEST_TIMEOUT` | `5` | Seconds a request waits for a connection before `503` |
| `DB_STATEMENT_TIMEOUT_MS` | `5000` | Postgres cancels queries longer than this |
| `API_MAX_PAGE_SIZE` | `10000` | Maximum rows or buckets per response |
| `CORS_ORIGINS` | none | Comma-separated browser origins allowed to call the API |
| `LOG_LEVEL` | `INFO` | Log verbosity |

## Contract with the consumer (please read)

The API reads the tables in `db/01-init.sql`. The consumer should:

| Kafka event | Write |
|---|---|
| experiment configured | `INSERT INTO experiment_config` with thresholds and sensors |
| stabilization started | `status = 'stabilizing'` |
| experiment started | `status = 'running'`, **`started_at` = event timestamp** |
| temperature measured | Once all sensors have reported a measurement id: insert the **average** into `measurements` with `ON CONFLICT (experiment_id, measurement_id) DO NOTHING` |
| experiment terminated | `status = 'terminated'`, `terminated_at` = event timestamp |

`started_at` matters: without it, the out-of-range endpoint returns
nothing, because a not-yet-started experiment has no violations. If you
rename a table or column, tell the API owner; only `api/app/db.py` needs
to change.

## Tests

The tests use a real, **throwaway** Postgres database. **They drop and
recreate the tables in it.**

```bash
cd api
pip install -r requirements-dev.txt
TEST_DATABASE_URL=postgresql://postgres:<password>@localhost:5432/observability_test pytest
```

Create the test database first:
`docker compose exec postgres createdb -U postgres observability_test`
