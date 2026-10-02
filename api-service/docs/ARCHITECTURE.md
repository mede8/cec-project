# REST API: Architecture and Design Decisions

This document explains how the query side of the Temperature Observability
Service is built, and why. It is written to support the architecture
assessment: each decision lists the alternative that was rejected.

## 1. Where the API sits

```
 sensors ──┐
           ├──► Kafka topic <team_id>.experiment.temperature
 sensor    │            │
 mgmt svc ─┘            ▼
              ┌────────────────────┐   POST out-of-range   ┌──────────────┐
              │ Consumer           │──────────────────────►│ Notifications│
              │ ingest + notify    │                       │ service      │
              └─────────┬──────────┘                       └──────────────┘
                        │ INSERT (owner account)
                        ▼
              ┌────────────────────┐
              │ Postgres           │  shared state
              │ experiment_config  │
              │ measurements       │
              └─────────┬──────────┘
                        │ SELECT only (api_reader account)
                        ▼
              ┌────────────────────┐   GET /temperature ...  ┌──────────────┐
              │ REST API (this)    │◄────────────────────────│ Researchers  │
              └────────────────────┘                         └──────────────┘
```

The system follows a **write-path / read-path split**. The consumer owns
the write path (Kafka to Postgres, plus notifications); the API owns the
read path (Postgres to researchers). They share nothing but the database
schema, so:

- a slow or crashed consumer does not stop researchers from querying data
  that is already stored, and a flood of queries cannot slow down ingestion
  except through the database;
- each side can be developed, deployed, restarted and scaled on its own;
- the only contract between the two is `db/01-init.sql`.

*Rejected alternative:* serving queries from the consumer process itself,
which keeps state in memory. That is simpler, but the data would be lost on
every restart, and queries would compete with Kafka polling in the same
process.

## 2. Code layout

| File | Responsibility |
|---|---|
| `app/main.py` | Builds the app: lifespan (connection pool), middleware, routers, error handlers |
| `app/config.py` | All settings, read from environment variables, each with a default |
| `app/routes/temperature.py` | `/temperature`, `/temperature/out-of-range`, `/temperature/series` |
| `app/routes/experiments.py` | `/experiments`, `/experiments/{id}`, `/experiments/{id}/stats` |
| `app/routes/health.py` | `/health`, `/health/live`, `/metrics` |
| `app/deps.py` | Shared dependencies: pool, pagination, window validation, 404 |
| `app/db.py` | **All SQL.** The only file that knows table and column names |
| `app/models.py` | Response schemas: validate output and generate `/docs` |
| `app/middleware.py` | Request IDs, access logs, Prometheus metrics |

The layering is **routes → db → Postgres**. Routes validate input and
choose status codes; `db.py` builds queries; nothing else touches SQL. If
the consumer team renames a column, only `db.py` changes.

## 3. What happens on one request

`GET /temperature/out-of-range?experiment-id=exp-1`

1. **Middleware** takes the caller's `X-Request-ID` header, or generates
   one, and starts a timer.
2. **FastAPI** validates the query parameters against the declared types
   (missing parameter or wrong type → `422`).
3. **The route** checks the time window (`start > end` → `400`) and that
   the experiment exists (`404`).
4. **`db.py`** borrows a connection from the pool (waiting at most 5 s,
   otherwise `503`) and runs one parameterised `SELECT`. Postgres cancels
   it if it runs longer than 5 s (`503`).
5. **The response model** checks the output shape and serialises it to JSON.
6. **Middleware** adds `X-Request-ID` to the response, writes one log line,
   and updates the request counter and latency histogram.

## 4. Data model and the contract with the consumer

```sql
experiment_config(experiment_id PK, researcher, sensors[],
                  lower_threshold, upper_threshold, status,
                  started_at, terminated_at, created_at)
measurements(id PK, experiment_id FK, measurement_id, ts, temperature,
             UNIQUE (experiment_id, measurement_id))
INDEX measurements(experiment_id, ts)
```

What the consumer must do:

| Kafka event | Consumer writes |
|---|---|
| experiment configured | `INSERT experiment_config` (thresholds, sensors, `status='configured'`) |
| stabilization started | `status='stabilizing'` |
| experiment started | `status='running'`, **`started_at = event timestamp`** |
| temperature measured (one per sensor) | once every sensor has reported a measurement id: `INSERT measurements` with the **average**, `ON CONFLICT DO NOTHING` |
| experiment terminated | `status='terminated'`, `terminated_at` |

## 5. Design decisions

### 5.1 Out-of-range is computed when queried, not stored as a flag
The query joins measurements with the thresholds in `experiment_config`.

- **Why:** one source of truth. A stored `is_out_of_range` column can
  disagree with the thresholds, for example if the consumer has a bug, if
  a reading arrives before the configuration, or if thresholds are
  corrected later.
- **Cost:** a join on every query. The composite index keeps this cheap,
  because only one experiment's rows are ever scanned.
- **Consistency guarantee:** the list endpoint and the statistics endpoint
  use the **same SQL fragment** (`_VIOLATION` in `db.py`), so their counts
  cannot disagree. This is tested for every experiment.

### 5.2 Warm-up readings are not violations
During stabilisation, the temperature is still moving from room
temperature towards the target band, so almost every warm-up reading is
"out of range". Counting them would bury the real excursions: in the demo
data, 50 of 64 flagged readings were warm-up. Only readings with
`ts >= started_at` count. Until an experiment has started, it has no
violations. The time-range query still returns warm-up readings, because
researchers may want to see the warm-up curve.

A value **exactly on** a threshold counts as in range (`<` and `>`, not
`<=` and `>=`). This is tested explicitly.

### 5.3 Composite index `(experiment_id, ts)`
Every query filters by one experiment and then by a time range or sorts by
time. With the experiment id first, Postgres jumps straight to that
experiment's rows, which are already sorted by `ts`, so `BETWEEN` and
`ORDER BY ts` need no separate sort step. The reverse order `(ts,
experiment_id)` would force it to scan every experiment's rows in the
window.

### 5.4 Connection pool, sized and bounded
Opening a Postgres connection costs several milliseconds and a server
process, so the service keeps up to 10 connections open
(`psycopg_pool`). The endpoints are plain `def` functions; FastAPI runs
them in a thread pool, so a slow query blocks one worker thread, not the
event loop. Three time limits keep failures fast:

| Limit | Default | Protects against |
|---|---|---|
| Startup wait for Postgres | 30 s | Starting in a broken state: the container fails and compose restarts it |
| Wait for a free connection | 5 s | Requests piling up during an outage (returns `503` instead) |
| `statement_timeout` | 5 s | One huge query holding a connection and CPU |

### 5.5 Bounded responses
Every list endpoint has `limit` (capped at 10 000) and `offset`, so a
request can never ask for millions of rows. For charts,
`/temperature/series` aggregates in the database (`GROUP BY
floor(ts / interval)`) and returns one row per bucket, and it rejects
requests that would produce more than 10 000 buckets. Buckets are aligned
to multiples of the interval, so the same bucket always has the same start
time across requests.

*Trade-off:* offset paging is simple but gets slower on deep pages, and
can skip or repeat rows if data arrives between pages. Keyset paging
(`WHERE ts > last_ts`) fixes both and is the next step if datasets grow.

### 5.6 Epoch seconds everywhere
Timestamps are stored and returned as Unix epoch seconds, the unit the
Kafka events use. There is no conversion step anywhere, so there are no
time-zone bugs, and fractional seconds work.

### 5.7 Idempotent writes (consumer side, required by the API)
Kafka delivers at least once: after a consumer crash, messages since the
last committed offset are delivered again. The unique key
`(experiment_id, measurement_id)` together with `ON CONFLICT DO NOTHING`
makes a replayed insert harmless. Without it, the API would return
duplicate readings and inflate averages and counts.

## 6. Failure modes

| Failure | What the API does | Verified by |
|---|---|---|
| Unknown experiment | `404` with a message | tests |
| Bad or missing parameters | `422` (type) or `400` (logic) with a message | tests |
| Postgres down | `503 database unavailable` within about 5 s; `/health` → `503`; `/health/live` → `200` | manual outage test |
| Postgres comes back | Recovers on the next request, no restart needed | manual outage test |
| Query too slow | Cancelled by Postgres, `503` "try a smaller time window" | manual test on 1.5 M rows |
| Postgres down at startup | Process exits after 30 s; compose `restart` retries | by design |
| Consumer down | Queries keep working on the stored data | architecture (§1) |

Two health endpoints exist on purpose. Docker's `HEALTHCHECK` uses
`/health/live`, which does not touch the database: if it used `/health`,
a database outage would make Docker restart every API container, which
fixes nothing and adds load during recovery. `/health` (readiness)
reports whether queries can actually be answered.

## 7. Security

- **Least privilege:** the API logs in as `api_reader`, which holds only
  `SELECT` (tested: `INSERT` and `DELETE` are refused).
- **Defence in depth:** every API session also sets
  `default_transaction_read_only = on`, so even a misconfigured owner
  login could not write.
- **No SQL injection:** every value is passed as a query parameter
  (`%s`), never pasted into SQL text. The only formatted SQL fragments are
  fixed constants in `db.py`.
- **Input validation:** types, ranges and enums are checked before any
  query runs.
- **Secrets:** passwords come from `.env`, which is git-ignored; there are
  none in the repository.
- **Container:** runs as a non-root user.
- **CORS:** off by default; enable specific origins with `CORS_ORIGINS`.

## 8. Observability

- **`X-Request-ID`** on every response, including errors, and in every log
  line, so one request can be traced end to end.
- **Access log:** one line per request with method, path, status and
  duration.
- **Prometheus `/metrics`:** `api_requests_total{method,route,status}` and
  `api_request_duration_seconds` (histogram). Routes are labelled by
  pattern (`/experiments/{experiment_id}`), not by raw path, so the number
  of metric series stays bounded however many experiments exist.

## 9. Scaling

The API holds no state of its own; everything is in Postgres. So:

1. **More API replicas** behind a load balancer: no coordination needed.
   Watch the total connections (replicas × pool size) against Postgres's
   `max_connections`, or put PgBouncer in front.
2. **Read replicas:** point the API at a Postgres streaming replica. Data
   is then slightly stale (milliseconds), which is acceptable for
   observability queries, and the primary is left for the consumer's
   writes.
3. **Partition `measurements` by time** (for example monthly) when it gets
   large: old partitions can be dropped or archived cheaply.
4. **Pre-aggregation** (a materialised view of per-minute buckets) if
   `/series` over months becomes slow.

## 10. Known limitations

- Offset paging (see §5.5).
- `/temperature/series` omits empty buckets rather than returning nulls.
- The time-range query includes warm-up readings; a `phase` filter could
  be added if researchers want to exclude them.
- There is no authentication: any client that can reach port 8000 can
  query. That is acceptable inside the course network; a real deployment
  would add API keys or OAuth at a gateway.

## 11. Likely oral-exam questions

**Why does the API not read from Kafka directly?**
Kafka is a log, not a query engine: answering "readings between T1 and T2"
would mean replaying the topic on every request. The consumer turns the
event stream into indexed rows once, and queries hit the index.

**What happens if the consumer inserts the same measurement twice?**
The unique constraint on `(experiment_id, measurement_id)` together with
`ON CONFLICT DO NOTHING` makes the second insert a no-op, which is needed
because Kafka delivers at least once.

**Why compute out-of-range at query time?**
It gives a single source of truth: thresholds live in one place, and the
list and the statistics share one SQL definition (§5.1).

**Why do warm-up readings not count?**
They are expected to be outside the band while the system heats up. The
cut-off is `started_at`, taken from the "experiment started" event (§5.2).

**How does the service behave if Postgres dies during the demo?**
It returns `503 database unavailable` within about 5 s, the liveness check
stays green so Docker does not restart it, and it recovers by itself when
Postgres returns (§6).

**How would you scale to 100× the traffic?**
Add stateless API replicas, use read replicas for the queries, add
connection pooling (PgBouncer), then partitioning and pre-aggregation
(§9).

**Why are the endpoints `def` and not `async def`?**
psycopg's pool is synchronous here, and FastAPI runs `def` endpoints in a
thread pool, so blocking database calls never block the event loop. With
`async def` and a synchronous driver, one slow query would stall every
request.

**How do you prevent SQL injection?**
Parameterised queries everywhere; FastAPI rejects bad types before any SQL
runs; and the database account cannot write anyway (§7).

**Why is the index `(experiment_id, ts)` and not `(ts)`?**
Every query is for one experiment, so the experiment id must come first to
narrow the search; `ts` second gives rows already in time order (§5.3).

**What would you change for production?**
Authentication at a gateway, keyset pagination, read replicas, alerting on
the Prometheus metrics, and schema migrations (for example Alembic)
instead of an init script.
