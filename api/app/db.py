"""Read-only database access for the REST API.

The consumer writes the tables in db/init.sql; the API only reads them:

  temperatures  one row per averaged measurement, saved by the consumer
                while an experiment is running, with the out_of_range flag
                the consumer computed from the experiment's thresholds
  experiments   experiment details (thresholds, phase). Optional: the API
                works even if the consumer never fills this table.

Every query is parameterised (%s), so user input never becomes SQL text.
All SQL lives in this file: if a table or column is renamed, only this file
needs to change.
"""

import os
from typing import Optional

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool


def database_url() -> str:
    """DATABASE_URL (as set in docker-compose.yml), else the POSTGRES_* parts."""
    url = os.getenv("DATABASE_URL")
    if url:
        return url
    return "postgresql://{u}:{p}@{h}:{port}/{db}".format(
        u=os.getenv("POSTGRES_USER", "postgres"),
        p=os.getenv("POSTGRES_PASSWORD", "postgres"),
        h=os.getenv("POSTGRES_HOST", "db"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        db=os.getenv("POSTGRES_DB", "postgres"),
    )


def _configure(conn) -> None:
    # The API never writes. Making every session read-only means even a bug
    # in the API cannot change the consumer's data.
    conn.execute("SET default_transaction_read_only = on")


def create_pool() -> ConnectionPool:
    pool = ConnectionPool(
        conninfo=database_url(),
        min_size=1,
        max_size=int(os.getenv("DB_POOL_SIZE", "10")),
        # A request waits at most this long for a connection, then gets 503.
        timeout=float(os.getenv("DB_REQUEST_TIMEOUT", "5")),
        kwargs={
            "row_factory": dict_row,
            "autocommit": True,
            # Postgres cancels any single query that runs longer than this.
            "options": f"-c statement_timeout={int(os.getenv('DB_STATEMENT_TIMEOUT_MS', '5000'))}",
        },
        configure=_configure,
        open=True,
    )
    # Fail fast at startup if the database is unreachable; compose restarts us.
    pool.wait(timeout=float(os.getenv("DB_CONNECT_TIMEOUT", "30")))
    return pool


def ping(pool: ConnectionPool) -> None:
    with pool.connection() as conn:
        conn.execute("SELECT 1")


# ---------------------------------------------------------------- temperatures

# The two required endpoints can return thousands of rows and are queried
# 100-200 times per second by the course's load generator. Postgres builds
# the JSON array itself (json_agg), so Python only passes the text through.
# float8 values are written in their shortest exact form, so timestamps
# round-trip bit-for-bit, which is what the grader compares.

def _json_points(where: str) -> str:
    return f"""
        SELECT coalesce(
                 json_agg(json_build_object('timestamp', ts, 'temperature', temperature)
                          ORDER BY ts, measurement_id),
                 '[]')::text AS body
        FROM (
            SELECT ts, temperature, measurement_id
            FROM temperatures
            WHERE {where}
            ORDER BY ts, measurement_id
            LIMIT %(limit)s OFFSET %(offset)s
        ) AS page
    """


_TEMPERATURE_JSON = _json_points("experiment_id = %(exp)s AND ts BETWEEN %(start)s AND %(end)s")
_OUT_OF_RANGE_JSON = _json_points(
    "experiment_id = %(exp)s AND out_of_range"
    " AND (%(start)s::float8 IS NULL OR ts >= %(start)s)"
    " AND (%(end)s::float8   IS NULL OR ts <= %(end)s)")


def temperatures_in_range(pool, experiment_id: str, start: float, end: float,
                          limit: Optional[int], offset: int) -> str:
    """JSON array of readings with start <= ts <= end, oldest first.
    limit=None returns every row (LIMIT NULL means no limit in Postgres)."""
    with pool.connection() as conn:
        return conn.execute(_TEMPERATURE_JSON, {"exp": experiment_id, "start": start, "end": end,
                                                "limit": limit, "offset": offset}).fetchone()["body"]


def out_of_range_temperatures(pool, experiment_id: str, start: Optional[float],
                              end: Optional[float], limit: Optional[int], offset: int) -> str:
    """JSON array of the readings the consumer flagged as out of range, oldest first.

    The consumer sets out_of_range when it saves a reading, using the
    experiment's thresholds (a value exactly on a threshold is in range).
    The API trusts that flag instead of recomputing it, so the API and the
    notifications the consumer sends always agree.
    """
    with pool.connection() as conn:
        return conn.execute(_OUT_OF_RANGE_JSON, {"exp": experiment_id, "start": start, "end": end,
                                                 "limit": limit, "offset": offset}).fetchone()["body"]


def temperature_series(pool, experiment_id: str, start: float, end: float,
                       interval: float) -> list[dict]:
    """Readings grouped into fixed-width time buckets (for charts).

    Buckets are aligned to multiples of `interval`, so a bucket always has
    the same start time across requests. Empty buckets are left out.
    """
    with pool.connection() as conn:
        return conn.execute(
            """
            SELECT floor(ts / %(iv)s) * %(iv)s     AS bucket_start,
                   count(*)                         AS count,
                   avg(temperature)                 AS avg_temperature,
                   min(temperature)                 AS min_temperature,
                   max(temperature)                 AS max_temperature,
                   count(*) FILTER (WHERE out_of_range) AS out_of_range_count
            FROM temperatures
            WHERE experiment_id = %(exp)s AND ts BETWEEN %(start)s AND %(end)s
            GROUP BY bucket_start
            ORDER BY bucket_start
            """,
            {"exp": experiment_id, "start": start, "end": end, "iv": interval},
        ).fetchall()


# ---------------------------------------------------------------- experiments

_EXPERIMENT_COLUMNS = """experiment_id, researcher, sensors,
                         lower_threshold, upper_threshold, phase"""


def experiment_known(pool, experiment_id: str) -> bool:
    """True if the experiment is in either table."""
    with pool.connection() as conn:
        row = conn.execute(
            """
            SELECT EXISTS (SELECT 1 FROM experiments  WHERE experiment_id = %(e)s)
                OR EXISTS (SELECT 1 FROM temperatures WHERE experiment_id = %(e)s)
                AS known
            """,
            {"e": experiment_id},
        ).fetchone()
    return row["known"]


def get_experiment(pool, experiment_id: str) -> Optional[dict]:
    """Details from the experiments table, or a minimal record if the
    experiment only appears in temperatures."""
    with pool.connection() as conn:
        row = conn.execute(
            f"SELECT {_EXPERIMENT_COLUMNS} FROM experiments WHERE experiment_id = %s",
            (experiment_id,),
        ).fetchone()
    if row is not None:
        return row
    if experiment_known(pool, experiment_id):
        return {"experiment_id": experiment_id, "researcher": None, "sensors": None,
                "lower_threshold": None, "upper_threshold": None, "phase": None}
    return None


def list_experiments(pool, phase: Optional[str], limit: Optional[int], offset: int) -> list[dict]:
    """Experiments from the experiments table, plus any experiment that only
    has rows in temperatures (the consumer may not fill the experiments
    table). Filtering by phase only returns experiments with known details."""
    with pool.connection() as conn:
        return conn.execute(
            f"""
            SELECT * FROM (
                SELECT {_EXPERIMENT_COLUMNS} FROM experiments
                UNION ALL
                SELECT DISTINCT t.experiment_id, NULL, NULL::text[],
                       NULL::real, NULL::real, NULL
                FROM temperatures t
                WHERE NOT EXISTS (SELECT 1 FROM experiments e
                                  WHERE e.experiment_id = t.experiment_id)
            ) AS all_experiments
            WHERE %(phase)s::text IS NULL OR phase = %(phase)s
            ORDER BY experiment_id
            LIMIT %(limit)s OFFSET %(offset)s
            """,
            {"phase": phase, "limit": limit, "offset": offset},
        ).fetchall()


def experiment_stats(pool, experiment_id: str, start: Optional[float],
                     end: Optional[float]) -> dict:
    """Summary of an experiment's saved readings in one pass."""
    with pool.connection() as conn:
        row = conn.execute(
            """
            SELECT count(*)                            AS count,
                   min(temperature)                    AS min_temperature,
                   max(temperature)                    AS max_temperature,
                   avg(temperature)                    AS avg_temperature,
                   min(ts)                             AS first_timestamp,
                   max(ts)                             AS last_timestamp,
                   count(*) FILTER (WHERE out_of_range) AS out_of_range_count
            FROM temperatures
            WHERE experiment_id = %(exp)s
              AND (%(start)s::float8 IS NULL OR ts >= %(start)s)
              AND (%(end)s::float8   IS NULL OR ts <= %(end)s)
            """,
            {"exp": experiment_id, "start": start, "end": end},
        ).fetchone()
    row["experiment_id"] = experiment_id
    row["out_of_range_ratio"] = (row["out_of_range_count"] / row["count"]) if row["count"] else 0.0
    return row
