-- Shared-state schema for the Temperature Observability Service.
-- The consumer WRITES these tables; the REST API only READS them.
-- Agree on this with the consumer + database owners before changing it.

-- One row per experiment, written when the consumer sees an
-- "experiment configured" event.
CREATE TABLE IF NOT EXISTS experiment_config (
    experiment_id     TEXT PRIMARY KEY,
    researcher        TEXT,
    sensors           TEXT[]           NOT NULL DEFAULT '{}',
    lower_threshold   DOUBLE PRECISION NOT NULL,
    upper_threshold   DOUBLE PRECISION NOT NULL,
    -- configured | stabilizing | running | terminated
    status            TEXT             NOT NULL DEFAULT 'configured',
    -- Epoch seconds of the "experiment started" event: the moment
    -- stabilisation ended. Readings before this are warm-up, not
    -- violations. NULL until the experiment has started.
    started_at        DOUBLE PRECISION,
    -- Epoch seconds of the "experiment terminated" event, NULL until then.
    terminated_at     DOUBLE PRECISION,
    created_at        TIMESTAMPTZ      NOT NULL DEFAULT now(),
    CHECK (lower_threshold <= upper_threshold)
);

-- One row per measurement: the temperature averaged over all of the
-- experiment's sensors for one measurement id.
CREATE TABLE IF NOT EXISTS measurements (
    id                BIGSERIAL        PRIMARY KEY,
    experiment_id     TEXT             NOT NULL
                      REFERENCES experiment_config (experiment_id)
                      ON DELETE CASCADE,
    measurement_id    TEXT             NOT NULL,
    -- Unix epoch seconds, same unit as the timestamps in the Kafka events.
    ts                DOUBLE PRECISION NOT NULL,
    temperature       DOUBLE PRECISION NOT NULL,
    -- Makes consumer inserts idempotent if Kafka redelivers a message:
    -- INSERT ... ON CONFLICT (experiment_id, measurement_id) DO NOTHING
    UNIQUE (experiment_id, measurement_id)
);

-- Both API queries filter by experiment and order by time.
CREATE INDEX IF NOT EXISTS measurements_experiment_ts_idx
    ON measurements (experiment_id, ts);
