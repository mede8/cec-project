# Temperature Observability Microservice

### Authors: Andrei, Diana, Dami

Repository Structure (tbd)

```bash
temperature-observability-service/
├── docker-compose.yml
├── consumer/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── main.py          # connect Kafka + DB, run poll loop
│   ├── state.py         # per-experiment tracking, sensor-averaging buffer
│   ├── notifier.py      # notifies the notifications-service
│   └── db.py            # writes: insert measurement, get/set experiment config
│
├── api/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app/
│       ├── main.py          # FastAPI app, defines endpoints
│       └── db.py            # reads: out-of-range query, time-range query
└── db/
    └── init.sql             # creates tables on first startup
```

### Improvements for Final Stage
    - Implement API Gateway to decrease response time: Kubernetes
    - Implement Load Balancer (?)
    - Save config of experiments in DB (?)
    - Prometheus (add it on main branch)
    - Multiple consumers
    - Research is multiple DB works (?)

    Stress test: measure first, then fix the bottleneck
    - Run the load generator and spot bottlenecks.
    - Set the producer’s config.json to 100-120 concurrent experiments. 
    - Run http-load-generator against our API, with its rate above 90 requests per second.
    - Make some important numbers visible
    - Consumer lag: log time.time() - event[“timestamp”] every few seconds. Target should be under ~2s and doesn’t grow.
    - Notification latency: we already log it and the target should be under 10s.
    - API response time: curl -w ‘%{time_total}’ or the load generator’s histogram. Target should be 0.1
    - CPU and memory per container: use prometheus. Target would be that no container is stuck at 100%.

    Likely bottlenecks:
    consumer: 
        * every commit has it own transaction, at 100 experiments x several sensors -> hundreds of commits per second. Fix: collect save items and write them in batches every ~200ms or every N rows (executemany() or psycopg2.extras.execute_values).
        * notifications: use a requests.Session so connections are reused. The connection pool should be at least as large as the number of workers (8-16).
        * running more consumers to split the topic’s partitions between them. That only works with your in-memory state if all events of one experiment land in the same partition, meaning the producer uses the experiment id as the message key. Check msg.key() and how many partitions the topic has. During a rebalance, partitions move between consumers and that state is lost, which is exactly why the “save configurations in the database” fix belongs in this phase.
        * Asynchronous messages?

    API:
        * More uvicorn workers. One process = one CPU core. Each worker has its own connection pool, but we should keep workers x pool size well below Postgres’s default limit of 100 connections.
        * API gateway