def test_health(client):
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


# ---- GET /temperature (time-range query) ----------------------------------

def test_time_range_returns_window_in_order(client):
    r = client.get(
        "/temperature",
        params={"experiment-id": "exp-1", "start-time": 1005, "end-time": 1030},
    )
    assert r.status_code == 200
    assert r.json() == [
        {"timestamp": 1010.0, "temperature": 26.4},
        {"timestamp": 1020.0, "temperature": 24.1},
        {"timestamp": 1030.0, "temperature": 26.0},
    ]


def test_time_range_bounds_are_inclusive(client):
    r = client.get(
        "/temperature",
        params={"experiment-id": "exp-1", "start-time": 1000, "end-time": 1000},
    )
    assert r.json() == [{"timestamp": 1000.0, "temperature": 25.5}]


def test_time_range_accepts_fractional_timestamps(client):
    r = client.get(
        "/temperature",
        params={"experiment-id": "exp-1", "start-time": 1039.5, "end-time": 1040.5},
    )
    assert r.json() == [{"timestamp": 1040.0, "temperature": 25.0}]


def test_time_range_does_not_leak_other_experiments(client):
    r = client.get(
        "/temperature",
        params={"experiment-id": "exp-2", "start-time": 0, "end-time": 5000},
    )
    assert r.json() == [{"timestamp": 1005.0, "temperature": 21.0}]


def test_time_range_empty_window(client):
    r = client.get(
        "/temperature",
        params={"experiment-id": "exp-1", "start-time": 2000, "end-time": 3000},
    )
    assert r.status_code == 200
    assert r.json() == []


def test_time_range_start_after_end_is_400(client):
    r = client.get(
        "/temperature",
        params={"experiment-id": "exp-1", "start-time": 1030, "end-time": 1000},
    )
    assert r.status_code == 400


def test_time_range_unknown_experiment_is_404(client):
    r = client.get(
        "/temperature",
        params={"experiment-id": "nope", "start-time": 0, "end-time": 1},
    )
    assert r.status_code == 404


def test_time_range_missing_param_is_422(client):
    r = client.get("/temperature", params={"experiment-id": "exp-1"})
    assert r.status_code == 422


def test_time_range_non_numeric_time_is_422(client):
    r = client.get(
        "/temperature",
        params={"experiment-id": "exp-1", "start-time": "yesterday", "end-time": 1},
    )
    assert r.status_code == 422


# ---- GET /temperature/out-of-range ----------------------------------------

def test_out_of_range_returns_only_violations(client):
    r = client.get("/temperature/out-of-range", params={"experiment-id": "exp-1"})
    assert r.status_code == 200
    # 26.0 and 25.0 sit exactly on the thresholds, so they are in range.
    assert r.json() == [
        {"timestamp": 1010.0, "temperature": 26.4},
        {"timestamp": 1020.0, "temperature": 24.1},
    ]


def test_out_of_range_none_for_in_range_experiment(client):
    r = client.get("/temperature/out-of-range", params={"experiment-id": "exp-2"})
    assert r.json() == []


def test_out_of_range_experiment_without_measurements(client):
    r = client.get(
        "/temperature/out-of-range", params={"experiment-id": "exp-empty"}
    )
    assert r.status_code == 200
    assert r.json() == []


def test_out_of_range_unknown_experiment_is_404(client):
    r = client.get("/temperature/out-of-range", params={"experiment-id": "nope"})
    assert r.status_code == 404


def test_out_of_range_missing_param_is_422(client):
    assert client.get("/temperature/out-of-range").status_code == 422


# ---- GET /experiments/{id} -------------------------------------------------

def test_get_experiment(client):
    r = client.get("/experiments/exp-1")
    assert r.status_code == 200
    assert r.json() == {
        "experiment_id": "exp-1",
        "researcher": "alice@example.org",
        "sensors": ["s1", "s2"],
        "lower_threshold": 25.0,
        "upper_threshold": 26.0,
        "status": "running",
        "started_at": 1000.0,
        "terminated_at": None,
    }


def test_get_unknown_experiment_is_404(client):
    assert client.get("/experiments/nope").status_code == 404
