import pytest

T = "/temperature"
OOR = "/temperature/out-of-range"


def get(client, path, **params):
    return client.get(path, params=params)


# ---- GET /temperature (required) -------------------------------------------

def test_time_range_window_in_order(client):
    r = get(client, T, **{"experiment-id": "exp-1", "start-time": 1005, "end-time": 1030})
    assert r.status_code == 200
    assert r.json() == [
        {"timestamp": 1010.0, "temperature": 26.4},
        {"timestamp": 1020.0, "temperature": 24.1},
        {"timestamp": 1030.0, "temperature": 26.0},
    ]


def test_time_range_bounds_inclusive(client):
    r = get(client, T, **{"experiment-id": "exp-1", "start-time": 1000, "end-time": 1000})
    assert r.json() == [{"timestamp": 1000.0, "temperature": 25.5}]


def test_time_range_fractional_times(client):
    r = get(client, T, **{"experiment-id": "exp-1", "start-time": 1039.5, "end-time": 1040.5})
    assert r.json() == [{"timestamp": 1040.0, "temperature": 25.0}]


def test_time_range_accepts_iso_times(client):
    # 1970-01-01T00:16:40Z == 1000 s, 00:17:10Z == 1030 s
    r = get(client, T, **{"experiment-id": "exp-1",
                          "start-time": "1970-01-01T00:16:40Z", "end-time": "1970-01-01T00:17:10Z"})
    assert [m["timestamp"] for m in r.json()] == [1000.0, 1010.0, 1020.0, 1030.0]


def test_time_range_experiment_without_experiments_row(client):
    # exp-2 exists only in `temperatures` (the consumer does not fill `experiments`)
    r = get(client, T, **{"experiment-id": "exp-2", "start-time": 0, "end-time": 5000})
    assert r.json() == [{"timestamp": 1005.0, "temperature": 21.0},
                        {"timestamp": 1006.0, "temperature": 35.0}]


def test_time_range_unknown_experiment_is_empty_list(client):
    r = get(client, T, **{"experiment-id": "nope", "start-time": 0, "end-time": 5000})
    assert r.status_code == 200 and r.json() == []


def test_time_range_empty_window(client):
    r = get(client, T, **{"experiment-id": "exp-1", "start-time": 2000, "end-time": 3000})
    assert r.status_code == 200 and r.json() == []


def test_time_range_start_after_end_is_400(client):
    r = get(client, T, **{"experiment-id": "exp-1", "start-time": 1030, "end-time": 1000})
    assert r.status_code == 400


@pytest.mark.parametrize("params", [
    {"experiment-id": "exp-1"},                                      # no times
    {"start-time": 0, "end-time": 1},                                # no experiment
    {"experiment-id": "exp-1", "start-time": "yesterday", "end-time": 1},
])
def test_time_range_bad_input_is_422(client, params):
    assert get(client, T, **params).status_code == 422


def test_paging(client):
    p = {"experiment-id": "exp-1", "start-time": 0, "end-time": 5000}
    assert [m["timestamp"] for m in get(client, T, **p, limit=2).json()] == [1000.0, 1010.0]
    assert [m["timestamp"] for m in get(client, T, **p, limit=2, offset=2).json()] == [1020.0, 1030.0]
    assert get(client, T, **p, limit=10_000_000).status_code == 422
    assert get(client, T, **p, limit=0).status_code == 422


# ---- GET /temperature/out-of-range (required) --------------------------------

def test_out_of_range_uses_consumer_flag(client):
    r = get(client, OOR, **{"experiment-id": "exp-1"})
    assert r.status_code == 200
    # 26.0 and 25.0 sit exactly on the thresholds: the consumer saved them as in range
    assert r.json() == [{"timestamp": 1010.0, "temperature": 26.4},
                        {"timestamp": 1020.0, "temperature": 24.1}]


def test_out_of_range_experiment_without_experiments_row(client):
    assert get(client, OOR, **{"experiment-id": "exp-2"}).json() == [
        {"timestamp": 1006.0, "temperature": 35.0}]


def test_out_of_range_with_window(client):
    r = get(client, OOR, **{"experiment-id": "exp-1", "start-time": 1015})
    assert r.json() == [{"timestamp": 1020.0, "temperature": 24.1}]


def test_out_of_range_unknown_or_empty_is_empty_list(client):
    for exp in ("nope", "exp-empty"):
        r = get(client, OOR, **{"experiment-id": exp})
        assert r.status_code == 200 and r.json() == []


def test_out_of_range_missing_experiment_is_422(client):
    assert client.get(OOR).status_code == 422


# ---- extras -------------------------------------------------------------------

def test_series(client):
    r = get(client, "/temperature/series",
            **{"experiment-id": "exp-1", "start-time": 1000, "end-time": 1040, "interval": 20})
    b = r.json()
    assert [x["bucket_start"] for x in b] == [1000.0, 1020.0, 1040.0]
    assert [x["count"] for x in b] == [2, 2, 1]
    assert [x["out_of_range_count"] for x in b] == [1, 1, 0]
    assert b[0]["avg_temperature"] == pytest.approx(25.95)


def test_series_too_many_buckets_is_400(client):
    r = get(client, "/temperature/series",
            **{"experiment-id": "exp-1", "start-time": 0, "end-time": 1e9, "interval": 1})
    assert r.status_code == 400


def test_stats(client):
    s = client.get("/experiments/exp-1/stats").json()
    assert s["count"] == 5
    assert (s["min_temperature"], s["max_temperature"]) == (24.1, 26.4)
    assert s["avg_temperature"] == pytest.approx((25.5 + 26.4 + 24.1 + 26.0 + 25.0) / 5)
    assert (s["first_timestamp"], s["last_timestamp"]) == (1000.0, 1040.0)
    assert s["out_of_range_count"] == 2 and s["out_of_range_ratio"] == pytest.approx(0.4)


@pytest.mark.parametrize("exp", ["exp-1", "exp-2", "exp-empty"])
def test_stats_agree_with_out_of_range_endpoint(client, exp):
    stats = client.get(f"/experiments/{exp}/stats").json()
    assert stats["out_of_range_count"] == len(get(client, OOR, **{"experiment-id": exp}).json())


def test_stats_unknown_is_404(client):
    assert client.get("/experiments/nope/stats").status_code == 404


def test_list_experiments_includes_ones_only_in_temperatures(client):
    ids = [e["experiment_id"] for e in client.get("/experiments").json()]
    assert ids == ["exp-1", "exp-2", "exp-empty"]


def test_list_experiments_by_phase(client):
    assert [e["experiment_id"] for e in client.get("/experiments?phase=running").json()] == ["exp-1"]
    assert client.get("/experiments?phase=exploded").status_code == 422


def test_get_experiment(client):
    assert client.get("/experiments/exp-1").json() == {
        "experiment_id": "exp-1", "researcher": "alice", "sensors": ["s1", "s2"],
        "lower_threshold": 25.0, "upper_threshold": 26.0, "phase": "running"}
    # only in temperatures: known, but without details
    assert client.get("/experiments/exp-2").json()["phase"] is None
    assert client.get("/experiments/nope").status_code == 404


# ---- operations ---------------------------------------------------------------

def test_health(client):
    assert client.get("/health").json() == {"status": "ok"}
    assert client.get("/health/live").json() == {"status": "ok"}


def test_request_id(client):
    assert len(client.get("/health").headers["X-Request-ID"]) == 32
    r = client.get("/experiments/nope", headers={"X-Request-ID": "demo-1"})
    assert r.status_code == 404 and r.headers["X-Request-ID"] == "demo-1"


def test_sessions_are_read_only(client):
    with client.app.state.pool.connection() as conn:
        assert conn.execute("SHOW default_transaction_read_only").fetchone() == {
            "default_transaction_read_only": "on"}


def test_openapi_lists_endpoints(client):
    paths = client.get("/openapi.json").json()["paths"]
    for p in [T, OOR, "/temperature/series", "/experiments", "/experiments/{experiment_id}",
              "/experiments/{experiment_id}/stats", "/health", "/health/live"]:
        assert p in paths


# ---- hardening (found in the final review) ---------------------------------------

@pytest.mark.parametrize("start,end", [("nan", "10"), ("-inf", "10"), ("0", "inf"), ("0", "1e400")])
def test_non_finite_times_are_422(client, start, end):
    r = get(client, T, **{"experiment-id": "exp-1", "start-time": start, "end-time": end})
    assert r.status_code == 422


def test_non_finite_interval_is_422(client):
    r = get(client, "/temperature/series",
            **{"experiment-id": "exp-1", "start-time": 0, "end-time": 10, "interval": "inf"})
    assert r.status_code == 422


def test_huge_offset_is_422_not_500(client):
    r = get(client, T, **{"experiment-id": "exp-1", "start-time": 0, "end-time": 1,
                          "offset": 10**20})
    assert r.status_code == 422


@pytest.mark.parametrize("path", [
    "/temperature?experiment-id=a%00b&start-time=0&end-time=1",
    "/temperature/out-of-range?experiment-id=a%00b",
    "/experiments/a%00b",
    "/experiments/a%00b/stats",
])
def test_nul_in_experiment_id_is_422_not_500(client, path):
    assert client.get(path).status_code == 422


def test_unexpected_database_error_is_json_500(client, monkeypatch):
    import psycopg
    from app import db

    def boom(*args, **kwargs):
        raise psycopg.DataError("simulated")
    monkeypatch.setattr(db, "out_of_range_temperatures", boom)
    r = client.get(OOR, params={"experiment-id": "exp-1"}, headers={"X-Request-ID": "err-500"})
    assert r.status_code == 500
    assert r.json() == {"detail": "internal database error"}
    assert r.headers["X-Request-ID"] == "err-500"


# ---- what the course's load generator checks ----------------------------------------
# It compares the COMPLETE response list, matching readings by exact timestamp.

def test_no_truncation_of_large_results(client, seeded_db):
    import psycopg
    n = 12_500                       # more than the old 10,000 default cap
    with psycopg.connect(seeded_db, autocommit=True) as conn:
        with conn.cursor().copy("COPY temperatures (experiment_id, measurement_id, ts, "
                                "temperature, out_of_range) FROM STDIN") as cp:
            for i in range(n):
                cp.write_row(("exp-big", f"m{i:05d}", 10_000.0 + i, 30.0, True))
    r = get(client, T, **{"experiment-id": "exp-big", "start-time": 0, "end-time": 1e9})
    assert len(r.json()) == n
    assert len(get(client, OOR, **{"experiment-id": "exp-big"}).json()) == n


def test_timestamps_round_trip_exactly(client, seeded_db):
    import psycopg
    stamps = [1691419390.9467194, 1691419391.9467194, 1691404551.541, 1790000000.123456789]
    with psycopg.connect(seeded_db, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.executemany("INSERT INTO temperatures VALUES (%s, %s, %s, %s, %s)",
                            [("exp-exact", f"m{i}", ts, 25.4, True) for i, ts in enumerate(stamps)])
    ordered = sorted(stamps)                   # the API returns oldest first
    got = [m["timestamp"] for m in get(client, OOR, **{"experiment-id": "exp-exact"}).json()]
    assert got == ordered                      # bit-for-bit equal, as the grader compares
    # a window whose bounds equal stored timestamps includes both ends
    r = get(client, T, **{"experiment-id": "exp-exact",
                          "start-time": repr(ordered[1]), "end-time": repr(ordered[2])})
    assert [m["timestamp"] for m in r.json()] == ordered[1:3]
