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


