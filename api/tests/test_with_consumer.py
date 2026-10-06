"""Integration test: the consumer's real code writes, the API reads.

Feeds Kafka-style events (as the consumer receives them after Avro decoding)
through consumer/state.py and saves with consumer/db.py, exactly as
consumer/main.py does, then checks what the API returns. Notifications are
recorded instead of being sent over the network.

Needs psycopg2 (the consumer's driver): pip install -r requirements-dev.txt
"""

import importlib
import sys

import pytest

from conftest import REPO_ROOT

EXP = "exp-int"


@pytest.fixture(scope="module")
def consumer(seeded_db):
    pytest.importorskip("psycopg2", reason="the consumer's driver psycopg2 is not installed")
    sys.path.insert(0, str(REPO_ROOT / "consumer"))
    try:
        cdb = importlib.import_module("db")
        state = importlib.import_module("state")
    finally:
        sys.path.pop(0)
    state.experiments.clear()
    conn = cdb.connect(seeded_db, retries=1)
    sent = []

    def feed(record_name, event):
        nonlocal conn
        for action, payload in state.handle_event(record_name, event):   # same as main.py
            if action == "save":
                conn = cdb.save_measurement(conn, **payload)
            elif action == "notify":
                sent.append(payload)

    yield feed, sent
    conn.close()


def reading(mid, ts, sensor, temp):
    return {"experiment": EXP, "measurement_id": mid, "timestamp": ts,
            "measurement_hash": f"hash-{mid}", "sensor": sensor, "temperature": temp}


def run_experiment(feed):
    feed("experiment_configured", {
        "experiment": EXP, "researcher": "dana", "sensors": ["a", "b"],
        "temperature_range": {"lower_threshold": 20.0, "upper_threshold": 22.0}})
    feed("stabilization_started", {"experiment": EXP})
    # warm-up reading (avg 15.5): during stabilization the consumer does not save
    feed("sensor_temperature_measured", reading("m0", 90.0, "a", 15.0))
    feed("sensor_temperature_measured", reading("m0", 90.0, "b", 16.0))
    feed("experiment_started", {"experiment": EXP})
    for mid, ts, ta, tb in [
        ("m1", 100.0, 21.0, 21.4),   # avg 21.2  in range
        ("m2", 110.0, 23.0, 24.0),   # avg 23.5  above upper  -> OutOfRange notification
        ("m3", 120.0, 19.0, 19.2),   # avg 19.1  below lower  (still out: no 2nd notification)
        ("m4", 130.0, 22.0, 22.0),   # avg 22.0  exactly on the threshold: in range
    ]:
        feed("sensor_temperature_measured", reading(mid, ts, "a", ta))
        feed("sensor_temperature_measured", reading(mid, ts, "b", tb))
    # Kafka redelivers m2's readings: must not create a duplicate row
    feed("sensor_temperature_measured", reading("m2", 110.0, "a", 23.0))
    feed("sensor_temperature_measured", reading("m2", 110.0, "b", 24.0))
    feed("experiment_terminated", {"experiment": EXP})


@pytest.fixture(scope="module")
def ran(consumer):
    feed, sent = consumer
    run_experiment(feed)
    return sent


def test_api_returns_what_the_consumer_saved(client, ran):
    r = client.get("/temperature", params={"experiment-id": EXP, "start-time": 0, "end-time": 1000})
    assert r.status_code == 200
    got = [(m["timestamp"], round(m["temperature"], 6)) for m in r.json()]
    # m0 (stabilization) not saved; the redelivered m2 not duplicated
    assert got == [(100.0, 21.2), (110.0, 23.5), (120.0, 19.1), (130.0, 22.0)]


def test_out_of_range_matches_consumer(client, ran):
    r = client.get("/temperature/out-of-range", params={"experiment-id": EXP})
    assert [(m["timestamp"], round(m["temperature"], 6)) for m in r.json()] == [
        (110.0, 23.5), (120.0, 19.1)]


@pytest.mark.xfail(strict=True, reason=(
    "Known consumer issue: when Kafka redelivers an old out-of-range measurement "
    "after newer in-range ones, state.py sends a second OutOfRange notification "
    "for the same measurement_id. strict=True: once fixed, this test passes and "
    "the marker must be removed."))
def test_consumer_notified_once_for_the_excursion(ran):
    assert [(n["notification_type"], n["measurement_id"]) for n in ran] == [("OutOfRange", "m2")]


def test_stats_and_listing_work_without_experiments_row(client, ran):
    s = client.get(f"/experiments/{EXP}/stats").json()
    assert s["count"] == 4 and s["out_of_range_count"] == 2
    ids = [e["experiment_id"] for e in client.get("/experiments").json()]
    assert EXP in ids
