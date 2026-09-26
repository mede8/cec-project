CREATE TABLE experiments (
    experiment_id TEXT PRIMARY KEY,
    researcher TEXT NOT NULL,
    lower_threshold REAL NOT NULL,
    upper_threshold REAL NOT NULL,
    started BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE measurements (
    measurement_id SERIAL PRIMARY KEY,
    experiment_id TEXT NOT NULL REFERENCES experiments(experiment_id),
    timestamp DOUBLE PRECISION NOT NULL,
    temperature REAL NOT NULL,
    in_range BOOLEAN NOT NULL
);

CRATE INDEX idx_measurements_experiment_id ON measurements (experiment_id, timestamp);
