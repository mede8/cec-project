# Temperature Observability Microservice

**Group 12:** Andrei Medesan, Diana Luna, Dami

The idea of this project is to provide a distributed service for an XYZ research center. The center wants temperature controlled experiments which can be configured, run concurrently and monitored in real time. In case a temperature goes out of range from the experiment's configuration or it stabilises, it sends a notification within 10 s. In addition, the microservice answers queries about an experiment's temperature in a time window, and about the moments it was out of range.

### Pipeline

```
producer -> Kafka -> consumer -> PostgreSQL <- API (FastAPI) <- researchers / k6 (own stress-test)
                        |
                        +-> notifications-service
```
The system has three containers, each with one job to adhere to separation of concerns principle. The `consumer` reads sensor events from Kafka, tracks each experiment, stores the temperature averages, and sends prepares the notification message to be sent to the researchers. The consumer container also connects to PostgresSQL to write the temperatures and experiments' configurations. The database (`db`) stores the averged temperatures. The last container in the system (`api`) answers researcher's queries by accessing the database. The consumer and the API do not interact with each other, the database is the only thing they share. 


### Repository Structure
```bash
temperature-observability-service/
├── docker-compose.yml
├── README.md
├── consumer/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── main.py          # connect Kafka + DB, run poll loop
│   ├── state.py         # per-experiment tracking, handles all events
│   ├── notifier.py      # notifies the notifications-service
│   └── db.py            # writes: insert measurement, and get/set experiment config
│
├── api/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app/
│       ├── main.py      # FastAPI app, defines endpoints
│       ├── db.py        # reads: out-of-range query, time-range query
│       ├── timing.py    # records the latencies of the API endpoints
│       └── cache.py     # creates the API cache 
├── db/
│    └── init.sql        # creates tables on first startup
│
├── stress-test/            
│   ├── api_load.js      # http load generator using k6
│   ├── k6_stages.py     # monitors api's result and creates an overall table
│   ├── analyse.py       # creates a summary from all logs
│   └── monitor.sh       # monitors CPU/memory when running stress-test 
│
├── producer/
│   ├── config_generator.py  # config to generate experiments
│   ├── experiment.sh        # run experiments locally for stress-test
│   └── notifications.sh     # run notifications locally for stress-test
│
└── prometheus/
    └── prometheus.yml   # prometheus config
```

## System Workings

### Consumer

The job of the consumer is to read incoming messages from the Kafka topic and decode them, while also keeping each experiment's state in memory through a dictionary. 

- `main.py`: This is the entry point file, which connects to Kafka and the database, then runs the poll loop to read messages, decode them with Avro and pass it to `state.py` for carrying out the actions it returns. Notifications are handed to a thread pool, so the loop keeps reading Kafka while a notification is being sent. 
- `state.py`: This is the per-experiment state machine, which keeps in memory a dictionary of experiments' configuration. It records and collects the sensor readings of each measurement until it can average them.
- `notifier.py`: This script sends a notification to notifications-service via POST as the service is a RESTful API. Notably, there are two types of notifications that are handled: *Stabilized* message is sent once when the averaged temperature is firstly in range; the second message, *OutOfRange* is sent when the temperature falls out of the pre-defined range.
- `db.py`: This script connects the consumer to the database and allows inserts of averaged temperatures. Kafka delivers each message at least once, so after a restart, a measurement can arrive twice, thus the primary key makes the second insert indempotent. 

### Database (`db/init.sql`)

The script creates two tables for the database, particularly, one row per averaged measurement with a primary key set to `(experiment_id, measurement_id)`; and a table with the experiment details. Regarding indexes, we created one on `(experiment_id, timestamp, measurement_id)`, which lets Postgres jump to one experiment's time window and read the rows already in the order the API returns them. The second index on rows flagged out of range helps the query to read only out-of-range temperatures faster. Every index also costs a little on each insert, which is why there are only two. 

### API

| Endpoint | Description |
|---|---|
| `GET /temperature?experiment-id=&start-time=&end-time=` | averaged temperatures in a time window |
| `GET /temperature/out-of-range?experiment-id=` | temperatures flagged out of range |
| `GET /temperature/series` | temperatures grouped into time buckets (charts) |
| `GET /experiments`, `/experiments/{id}` | list multiple or specific experiments |
| `GET /experiments/{id}/stats` | experiment's statistics: count, min, max, average, and out-of-range count |
| `GET /health` | API and database reachable (503 if the database is down) |
| `GET /health/live` | API process is up |

- `app/main.py`: The script introduces the FastAPI app which includes: the endpoints, input checks (`parse_time`, `check_window`, `check_id`), exception handlers (that return a 503 with the specific message or a 500 for database error), and the request middleware. The latter wraps around every request and, on the way in, it applies an admission control. If `MAX_IN_FLIGHT` requests are already being handled by this worker, the new one gets an immediate `503 busy`. Otherwise, it gives the request an ID and starts a timer. On the way out, it adds the ID to the response and logs the request if it was slow or failed.
- `app/db.py`: This allows the API to only read queries over an asynchronous connection pool.
- `app/timing.py`: This script collects timings and counters to log a summary every 5 seconds (request time, pool wait, query time, cache hits/misses, rejections).
- `app/cache.py`: This creates the in-memory cache per worker, with a short expiry time (`CACHE_TTL_S=1`). If many requests for the same answer arrive at once, only one queries the database and the others share its result. We cache the following information: `/temperature` is saved for closed windows (ending more than 2 seconds ago), whose data can no longer change; and `/temperature/out-of-range` answers.

### Stress Test

We created specific files and directories for stress-testing the microservice. The target is to run 100 experiments concurrently and reach 1000 requests per second with 80 parallel connections (VUs), while also reaching an average latency of about 50 ms. Also, the consumer's lag should be less than 2 s and it should notify the researcher in case of an out-of-range temperature under 10 s. 

The `/producer` directory contains: the Python script (`config_generator`) that creates JSON experiment configurations; and two shell files that create the experiment-producer and notifications-service containers from the course's assignment repository. These files allow the system to be tested on the consumer part. 

The `/stress-test` directory contains the necessary files for running the http load generator and extract the information from the system's logs.
- `api_load.js`: This is the k6 load test which sends a fixed number of requests per second whether or not earlier ones have finished, resembling independent users. It tests the system in steps, starting with 200 req/s and builds up to 1000 req/s by default.
    - with `-e MODE=hold` in the command, the system is tested with 1000 req/s for 3 minutes.
    - with `-e VERIFY=1`, the system is tested for 30 s to make sure everything works as intended. 
    - with `-e PING=1`, the system is tested on one endpoint `/health/live`. 
- `monitor.sh`: This records the containers' CPU/memory and the service logs during a run. 
- `analyse.py`: This summarises one monitored run by revealing the CPU per container, consumer lag, notification latency and log problems.
 

## Getting Started

*Requirements*: To run this service effectively, Docker and Docker Compose need to be installed. Additionally, this service does not implement or have Kafka brokers, so access to these is required. For the stress testing, we use [k6][https://k6.io] and Python 3.

```bash
cd cec-project
# create .env containing the Kafka broker and topic, notifications-service URL and token.
# Postgres credentials and the settings below
docker compose up -d --build
```

To check whether the service is running:
```bash
docker ps   # db and api should be "healthy"
curl http://localhost:3003/health
```

To run stress-testing on the service:
```bash
# execute all shell scripts
chmod +x stress-test/*.sh producer/*.sh

# build the containers for notifications-service
./producer/notifications.sh

# create experiment configuration
cd producer
python3 config_generator.py <number of experiments>
./experiment.sh
cd ..

# open the monitoring for logging the results
# this can be done before opening the experiment-producer in another terminal or using tmux
./stress-test/monitor.sh <name for the results folder>

# on a different machine, test the API with k6
# for just verifying whether the system works properly
k6 run -e VERIFY=1 stress-test/api_load.js

# for running the full stress-test on API
k6 run -e BASE_URL=http://<vm-ip>:3003 --summary-mode=full --out csv=<run-name>.csv.gz --summary-export=<run-name>.json stress-test/api_load.js

# to run the ping test for only one endpoint
k6 run -e BASE_URL=http://<vm-ip>:3003 -e PING=1 --out csv=<run-name>.csv.gz stress-test/api_load.js
```

### Configuration

| Environment Variable | Value | Description |
|---|---|---|
| `WEB_CONCURRENCY` | 2 | uvicorn worker processes. One Python process uses about one core, and the VM has 2. More workers would only take turns on the same cores. |
| `API_POOL_SIZE` | 5 | Database connections per worker (10 in total), so at most 10 queries run in Postgres at once. Extra requests wait in the API.|
| `DB_REQUEST_TIMEOUT` | 1 s | Maximum wait for a free connection, then 503. A request that waited 1 s has missed the 300 ms target anyway, so failing fast is better than holding resources. |
| `DB_STATEMENT_TIMEOUT_MS` | 5000 | Postgres cancels a query that runs longer, so one huge window cannot hold a connection forever. |
| `MAX_IN_FLIGHT` | 64 | Requests one worker handles at once before refusing. The value was chosen by Little's law (in flight = throughput $\times$ time per request) to adhere to the requirements. |
| `SLOW_MS` | 2000 | Requests slower than this are logged. |
| `CACHE_TTL_S` | 1 | How long a cached answer is reused. |
| `CACHE_MAX_ENTRIES` | 10000 | The cache is cleared when it reaches this size. |
| `cpu_shares` (compose) | consumer 4096, api 512 | Prioritises the consumer over API for CPU usage. |

## Lessons Learned

The core lesson of this project is that overload does not disappear when one place gets fixed, but it moves to the next queue. Every stage of the stress test followed the same pattern, namely, we found where requests piled up, bounded that queue, and the pile-up appeared one step further along. 

The first demo version passed the data consistency test and included synchronous endpoints with a thread pool, one uvicorn process, and a pool of 8 connections. During stress-testing we observed and modified the following:

| Stress Test # | What we measured | Why it happened | What we changed | Effect |
|---|---|---|---|---|
| 1 | API plateaued at ~280–290 successful req/s; failures were "no answer", the API logged nothing | One process used one core; the API was too busy to accept connections, so the OS dropped them creating client latency | 2 uvicorn workers | Both cores in use |
| 2 | 398 notifications took over 10 s (worst 69 s); consumer lag 12.5 s | More requests now reached the API and Postgres; the consumer stopped reading due to waiting for CPU | `cpu_shares` (consumer 4096, api 512); pool connections changed to 5 | no notifications over 10 s; lag 0.117 s; API cost ~10–15% of a core |
| 3 | 503s after exactly 5 s | With async endpoints, hundreds of requests queued for the pool | Async endpoints + async pool; pool timeout 5 s -> 1 s | Reached ~600 req/s |
| 4 | Above ~640 req/s, successful req/s collapsed to ~235 | Congestion collapse: CPU went into requests that then failed | Admission control (`MAX_IN_FLIGHT`) | No improvement alone: refusals took ~2 s, because the event loop itself was saturated, so requests queued before the app saw them |
| 5 | Ping test: 1,000 req/s at ~0.65 ms of CPU per request | The framework was not the limit; the endpoints' own work was (~1.1 ms per request) | In-memory cache with TTL | Successful req/s leveled off at ~780 instead of collapsing |

