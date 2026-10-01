CREATE TABLE IF NOT EXISTS experiments (
    experiment_id TEXT PRIMARY KEY,
    researcher TEXT NOT NULL,
    sensors TEXT[] NOT NULL,
    lower_threshold REAL NOT NULL,
    upper_threshold REAL NOT NULL,
    phase TEXT NOT NULL
);

-- one row for average measurement
CREATE TABLE IF NOT EXISTS temperatures(
    experiment_id TEXT NOT NULL,
    measurement_id SERIAL PRIMARY KEY,
    ts DOUBLE PRECISION NOT NULL,
    temperature DOUBLE PRECISION NOT NULL,
    out_of_range BOOLEAN NOT NULL,
    PRIMARY KEY (experiment_id, measurement_id)
);

-- range scan by time, and filter on out_of_range
CREATE INDEX IF NOT EXISTS idx_temp_exp_ts ON temperatures (experiment_id, ts);
