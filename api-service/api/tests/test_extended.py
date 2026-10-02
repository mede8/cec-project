"""Tests for the features beyond the two required endpoints."""

import pytest


# ---- pagination ---------------------------------------------------------------

def test_time_range_limit_and_offset(client):
    params = {"experiment-id": "exp-1", "start-time": 0, "end-time": 5000}
    first = client.get("/temperature", params={**params, "limit": 2}).json()
    second = client.get("/temperature", params={**params, "limit": 2, "offset": 2}).json()
    assert [m["timestamp"] for m in first] == [1000.0, 1010.0]
    assert [m["timestamp"] for m in second] == [1020.0, 1030.0]


def test_limit_above_server_maximum_is_400(client):
    r = client.get(
        "/temperature",
        params={"experiment-id": "exp-1", "start-time": 0, "end-time": 1,
                "limit": 10_000_000},
    )
    assert r.status_code == 400


@pytest.mark.parametrize("bad", [{"limit": 0}, {"offset": -1}])
def test_invalid_paging_is_422(client, bad):
    r = client.get(
        "/temperature",
        params={"experiment-id": "exp-1", "start-time": 0, "end-time": 1, **bad},
    )
    assert r.status_code == 422


# ---- out-of-range with a window ---------------------------------------------------

def test_out_of_range_with_window(client):
    r = client.get(
        "/temperature/out-of-range",
        params={"experiment-id": "exp-1", "start-time": 1015, "end-time": 5000},
    )
    assert r.json() == [{"timestamp": 1020.0, "temperature": 24.1}]


def test_out_of_range_open_ended_window(client):
    r = client.get(
        "/temperature/out-of-range",
        params={"experiment-id": "exp-1", "end-time": 1015},
    )
    assert r.json() == [{"timestamp": 1010.0, "temperature": 26.4}]


def test_out_of_range_reversed_window_is_400(client):
    r = client.get(
        "/temperature/out-of-range",
        params={"experiment-id": "exp-1", "start-time": 9, "end-time": 1},
    )
    assert r.status_code == 400


# ---- warm-up readings are not violations ----------------------------------------

def test_out_of_range_ignores_warmup_readings(client):
    r = client.get("/temperature/out-of-range",
                   params={"experiment-id": "exp-warmup"})
    # 15.0 at 1050 is before started_at=1100; 23.0 at exactly 1100 counts.
    assert r.json() == [{"timestamp": 1100.0, "temperature": 23.0}]


def test_out_of_range_empty_before_experiment_starts(client):
    r = client.get("/temperature/out-of-range",
                   params={"experiment-id": "exp-stabilizing"})
    assert r.json() == []


def test_time_range_still_includes_warmup_readings(client):
    r = client.get("/temperature",
                   params={"experiment-id": "exp-warmup", "start-time": 0,
                           "end-time": 5000})
    assert len(r.json()) == 3


def test_stats_warmup_counted_in_totals_not_in_violations(client):
    s = client.get("/experiments/exp-warmup/stats").json()
    assert s["count"] == 3
    assert s["min_temperature"] == 15.0
    assert s["below_lower_count"] == 0
    assert s["above_upper_count"] == 1


# ---- stats ------------------------------------------------------------------------

def test_stats_whole_experiment(client):
    r = client.get("/experiments/exp-1/stats")
    assert r.status_code == 200
    s = r.json()
    assert s["count"] == 5
    assert s["min_temperature"] == 24.1
    assert s["max_temperature"] == 26.4
    assert s["avg_temperature"] == pytest.approx((25.5 + 26.4 + 24.1 + 26.0 + 25.0) / 5)
    assert s["first_timestamp"] == 1000.0
    assert s["last_timestamp"] == 1040.0
    assert s["below_lower_count"] == 1
    assert s["above_upper_count"] == 1
    assert s["out_of_range_count"] == 2
    assert s["out_of_range_ratio"] == pytest.approx(0.4)


@pytest.mark.parametrize("exp", ["exp-1", "exp-2", "exp-empty", "exp-warmup",
                                 "exp-stabilizing"])
def test_stats_agree_with_out_of_range_endpoint(client, exp):
    stats = client.get(f"/experiments/{exp}/stats").json()
    rows = client.get("/temperature/out-of-range",
                      params={"experiment-id": exp}).json()
    assert stats["out_of_range_count"] == len(rows)


def test_stats_with_window(client):
    s = client.get("/experiments/exp-1/stats",
                   params={"start-time": 1025, "end-time": 1045}).json()
    assert s["count"] == 2
    assert s["out_of_range_count"] == 0


def test_stats_experiment_without_measurements(client):
    s = client.get("/experiments/exp-empty/stats").json()
    assert s["count"] == 0
    assert s["min_temperature"] is None
    assert s["out_of_range_count"] == 0
    assert s["out_of_range_ratio"] == 0.0


def test_stats_unknown_experiment_is_404(client):
    assert client.get("/experiments/nope/stats").status_code == 404


# ---- series -------------------------------------------------------------------------

def test_series_buckets(client):
    r = client.get(
        "/temperature/series",
        params={"experiment-id": "exp-1", "start-time": 1000, "end-time": 1040,
                "interval": 20},
    )
    assert r.status_code == 200
    buckets = r.json()
    # [1000,1020): 25.5, 26.4   [1020,1040): 24.1, 26.0   [1040,1060): 25.0
    assert [b["bucket_start"] for b in buckets] == [1000.0, 1020.0, 1040.0]
    assert [b["count"] for b in buckets] == [2, 2, 1]
    assert buckets[0]["avg_temperature"] == pytest.approx(25.95)
    assert buckets[0]["min_temperature"] == 25.5
    assert buckets[0]["max_temperature"] == 26.4


def test_series_counts_add_up(client):
    buckets = client.get(
        "/temperature/series",
        params={"experiment-id": "exp-1", "start-time": 0, "end-time": 5000,
                "interval": 7},
    ).json()
    assert sum(b["count"] for b in buckets) == 5


def test_series_too_many_buckets_is_400(client):
    r = client.get(
        "/temperature/series",
        params={"experiment-id": "exp-1", "start-time": 0, "end-time": 1e9,
                "interval": 1},
    )
    assert r.status_code == 400


def test_series_zero_interval_is_422(client):
    r = client.get(
        "/temperature/series",
        params={"experiment-id": "exp-1", "start-time": 0, "end-time": 10,
                "interval": 0},
    )
    assert r.status_code == 422


# ---- experiment list ---------------------------------------------------------------

def test_list_experiments(client):
    ids = [e["experiment_id"] for e in client.get("/experiments").json()]
    assert sorted(ids) == ["exp-1", "exp-2", "exp-empty", "exp-stabilizing",
                           "exp-warmup"]


def test_list_experiments_by_status(client):
    r = client.get("/experiments", params={"status": "configured"})
    assert [e["experiment_id"] for e in r.json()] == ["exp-empty"]
    r = client.get("/experiments", params={"status": "stabilizing"})
    assert [e["experiment_id"] for e in r.json()] == ["exp-stabilizing"]


def test_list_experiments_unknown_status_is_422(client):
    assert client.get("/experiments", params={"status": "exploded"}).status_code == 422


# ---- operations ---------------------------------------------------------------------

def test_liveness(client):
    assert client.get("/health/live").json() == {"status": "ok"}


def test_request_id_generated(client):
    r = client.get("/health")
    assert len(r.headers["X-Request-ID"]) == 32


def test_request_id_propagated(client):
    r = client.get("/health", headers={"X-Request-ID": "demo-123"})
    assert r.headers["X-Request-ID"] == "demo-123"


def test_request_id_on_errors(client):
    r = client.get("/experiments/nope", headers={"X-Request-ID": "err-1"})
    assert r.status_code == 404
    assert r.headers["X-Request-ID"] == "err-1"


def test_metrics_count_requests_by_route(client):
    client.get("/experiments/exp-1")
    body = client.get("/metrics").text
    assert 'api_requests_total{method="GET",route="/experiments/{experiment_id}",status="200"}' in body
    assert "api_request_duration_seconds_bucket" in body


def test_sessions_are_read_only(client):
    pool = client.app.state.pool
    with pool.connection() as conn:
        assert conn.execute("SHOW default_transaction_read_only").fetchone() == {
            "default_transaction_read_only": "on"
        }
        assert conn.execute("SHOW statement_timeout").fetchone() == {
            "statement_timeout": "5s"
        }


def test_openapi_documents_all_endpoints(client):
    paths = client.get("/openapi.json").json()["paths"]
    for p in ["/temperature", "/temperature/out-of-range", "/temperature/series",
              "/experiments", "/experiments/{experiment_id}",
              "/experiments/{experiment_id}/stats", "/health", "/health/live"]:
        assert p in paths
