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
