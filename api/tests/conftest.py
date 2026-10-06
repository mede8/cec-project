"""Test fixtures: a real Postgres database built from the team's db/init.sql.

Point TEST_DATABASE_URL at an EMPTY, throwaway database, for example:

    docker compose exec db createdb -U <POSTGRES_USER> api_test
    TEST_DATABASE_URL=postgresql://<user>:<password>@localhost:5432/api_test pytest

WARNING: the tables in that database are dropped and recreated.
"""

import os
from pathlib import Path

import psycopg
import pytest
from fastapi.testclient import TestClient

REPO_ROOT = Path(__file__).resolve().parents[2]
INIT_SQL = REPO_ROOT / "db" / "init.sql"

# exp-1 has a row in `experiments` (thresholds 25.0 .. 26.0).
# exp-2 only has rows in `temperatures`, which is what happens with the
#       current consumer, since it never writes to `experiments`.
# exp-empty is configured but has no saved readings yet.
EXPERIMENTS = [
    ("exp-1", "alice", ["s1", "s2"], 25.0, 26.0, "running"),
    ("exp-empty", "carol", ["s4"], 10.0, 12.0, "configured"),
]
TEMPERATURES = [
    # experiment, measurement_id, ts, temperature, out_of_range (set by consumer)
    ("exp-1", "m1", 1000.0, 25.5, False),
    ("exp-1", "m2", 1010.0, 26.4, True),
    ("exp-1", "m3", 1020.0, 24.1, True),
    ("exp-1", "m4", 1030.0, 26.0, False),   # exactly on the upper threshold
    ("exp-1", "m5", 1040.0, 25.0, False),   # exactly on the lower threshold
    ("exp-2", "m1", 1005.0, 21.0, False),
    ("exp-2", "m2", 1006.0, 35.0, True),
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
        conn.execute("DROP TABLE IF EXISTS temperatures, experiments CASCADE")
        conn.execute(INIT_SQL.read_text())
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO experiments (experiment_id, researcher, sensors, "
                "lower_threshold, upper_threshold, phase) VALUES (%s, %s, %s, %s, %s, %s)",
                EXPERIMENTS)
            cur.executemany(
                "INSERT INTO temperatures (experiment_id, measurement_id, ts, temperature, "
                "out_of_range) VALUES (%s, %s, %s, %s, %s)",
                TEMPERATURES)
    return database_url


@pytest.fixture(scope="session")
def client(seeded_db):
    os.environ["DATABASE_URL"] = seeded_db
    from app.main import app

    with TestClient(app) as c:   # runs startup/shutdown (the connection pool)
        yield c
