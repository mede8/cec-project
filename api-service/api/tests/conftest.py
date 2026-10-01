"""Test fixtures: a real Postgres database with known sample data.

Point TEST_DATABASE_URL at an empty, throwaway database, e.g. the one
from docker compose:

    TEST_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/observability_test \
        pytest

WARNING: the tables in that database are dropped and recreated.
"""

import os
from pathlib import Path

import psycopg
import pytest
from fastapi.testclient import TestClient

INIT_SQL = Path(__file__).resolve().parents[2] / "db" / "01-init.sql"

# Experiment "exp-1": thresholds 25.0 .. 26.0, started at 1000
# Experiment "exp-2": thresholds 20.0 .. 30.0, a single in-range reading
# Experiment "exp-empty": configured, no measurements yet
# Experiment "exp-warmup": started at 1100; the 1050 reading is warm-up
# Experiment "exp-stabilizing": still stabilising, never started
EXPERIMENTS = [
    # id, researcher, sensors, lower, upper, status, started_at
    ("exp-1", "alice@example.org", ["s1", "s2"], 25.0, 26.0, "running", 1000.0),
    ("exp-2", "bob@example.org", ["s3"], 20.0, 30.0, "running", 1000.0),
    ("exp-empty", "carol@example.org", ["s4"], 10.0, 12.0, "configured", None),
    ("exp-warmup", "dan@example.org", ["s5"], 20.0, 22.0, "running", 1100.0),
    ("exp-stabilizing", "eve@example.org", ["s6"], 20.0, 22.0, "stabilizing", None),
]
MEASUREMENTS = [
    # experiment, measurement_id, ts, temperature
    ("exp-1", "m1", 1000.0, 25.5),   # in range
    ("exp-1", "m2", 1010.0, 26.4),   # above upper
    ("exp-1", "m3", 1020.0, 24.1),   # below lower
    ("exp-1", "m4", 1030.0, 26.0),   # exactly on upper -> in range
    ("exp-1", "m5", 1040.0, 25.0),   # exactly on lower -> in range
    ("exp-2", "m1", 1005.0, 21.0),   # other experiment, must never leak
    ("exp-warmup", "m1", 1050.0, 15.0),   # below, but before start: warm-up
    ("exp-warmup", "m2", 1100.0, 23.0),   # above, exactly at start: counts
    ("exp-warmup", "m3", 1150.0, 21.0),   # in range
    ("exp-stabilizing", "m1", 1070.0, 5.0),  # not started: never a violation
]


@pytest.fixture(scope="session")
def database_url() -> str:
    url = os.getenv("TEST_DATABASE_URL")
    if not url:
        pytest.skip("TEST_DATABASE_URL is not set")
    return url


@pytest.fixture(scope="session")
def seeded_db(database_url):
    with psycopg.connect(database_url, autocommit=True) as conn:
        conn.execute("DROP TABLE IF EXISTS measurements, experiment_config CASCADE")
        conn.execute(INIT_SQL.read_text())
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO experiment_config (experiment_id, researcher, "
                "sensors, lower_threshold, upper_threshold, status, started_at) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                EXPERIMENTS,
            )
            cur.executemany(
                "INSERT INTO measurements (experiment_id, measurement_id, ts, "
                "temperature) VALUES (%s, %s, %s, %s)",
                MEASUREMENTS,
            )
    return database_url


@pytest.fixture(scope="session")
def client(seeded_db):
    os.environ["DATABASE_URL"] = seeded_db
    from app.main import app

    with TestClient(app) as c:  # runs startup/shutdown (the pool)
        yield c
