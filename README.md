# Temperature Observability Microservice

## Group 12: Andrei Medesan, Diana Luna, Dami

The idea of this project is to provide a distributed service for an XYZ research center. The center wants temperature controlled experiments which can be configured, run concurrently and monitored in real time. In case a temperature goes out of range from the experiment's configuration, it sends a notification in less than 10sec.

### Pipeline

```
producer -> Kafka -> consumer -> PostgreSQL <- API (FastAPI) <- researchers / k6 (own stress-test)
                        |
                        +-> notifications-service
```

### Repository Structure
```bash
temperature-observability-service/
├── docker-compose.yml
│
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
│       ├── db.py            # reads: out-of-range query, time-range query
│       ├── timing.py
│       └── cache.py  
├── db/
│    └── init.sql             # creates tables on first startup
│
├── stress-test/            
│   ├── api_load.js     # http load generator using k6
│   ├── k6_stages.py    # k6 stages to monitor api's results
│   ├── analyse.py      # summary of 1 run done by monitor
│   └── monitor.sh      # monitor CPU/memory when running stress-test 
│
├── producer/
│    ├── config_generator.py    # config to generate experiments
│    ├── experiment.sh          # run experiments locally for stress-test
│    └── notifications.sh       # run notifications locally for stress-test
│
└── prometheus/
    └── prometheus.yml      # prometheus config
```

### Getting Started

In Group 12 VM:
```bash
cd cec-project
docker compose up --build
```

### API

| Endpoint | Description |
|---|---|
| `GET /temperature?experiment-id=&start-time=&end-time=` | temperatures in a time window |
| `GET /temperature/out-of-range?experiment-id=` | temperatures flagged out of range |
| `GET /temperature/series` | temperatures grouped into time buckets (charts) |
| `GET /experiments`, `/experiments/{id}` | list multiple or specific experiment |
| `GET /experiments/{id}/stats` | experiment's stats: count, min, max, average, out-of-range count |
| `GET /health`, `/health/live` | api & DB check |

### Monitoring: Prometheus
Prometheus add metrics of CPU & memory for the microservice and it is linked with Grafana. 

### Stress Test
The target is to run 100 experiments concurrently and reach 1000 requests per second with 80 parallel connections (VUs) and reach an average latency about 50 ms. Also, for the consumer's lag should be less than 2s and it should notify in case of an out of range temperature under 10s. 

```bash
# run api_load.js to stress api with k6
k6 run -e BASE_URL=http://localhost:3003   --summary-trend-stats="avg,min,med,p(90),p(95),p(99),max"   --out csv=combined-k6.csv.gz   --summary-export=combined-summary.json --summary-mode=full  stress-test/api_load.js

# run api_load.js to stress api with k6 but just to check connections
k6 run -e BASE_URL=http://54.220.44.146:3003 -e PING=1 --out csv=ping-k6.csv.gz stress-test/api_load.js
```
