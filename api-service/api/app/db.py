"""Database access for the REST API.

The API only READS the shared state that the consumer writes (see
db/init.sql). All SQL lives in this file, so a change to the table
layout only needs changes here.

Every query is parameterised (%s placeholders), so user input is never
pasted into SQL text: no SQL injection is possible.
"""

from typing import Optional

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from .config import Settings


def create_pool(settings: Settings) -> ConnectionPool:
    pool = ConnectionPool(
        conninfo=settings.database_url,
        min_size=settings.pool_min_size,
        max_size=settings.pool_max_size,
        # Wait at most this long for a free connection, then fail (-> 503).
        timeout=settings.request_timeout_s,
        kwargs={
            "row_factory": dict_row,
            # Postgres itself cancels runaway queries.
            "options": f"-c statement_timeout={settings.statement_timeout_ms}",
            # Single SELECTs need no transaction; autocommit also means a
            # connection is never left "idle in transaction".
            "autocommit": True,
        },
        # The API never writes: every session is made read-only, a second
        # line of defence behind the read-only database role.
        configure=_configure_connection,
        open=True,
    )
    # Fail fast at startup if Postgres is unreachable.
    pool.wait(timeout=settings.connect_timeout_s)
    return pool


def _configure_connection(conn) -> None:
    conn.execute("SET default_transaction_read_only = on")


# ---- experiments ------------------------------------------------------------

def experiment_exists(pool: ConnectionPool, experiment_id: str) -> bool:
    with pool.connection() as conn:
        row = conn.execute(
            "SELECT 1 FROM experiment_config WHERE experiment_id = %s",
            (experiment_id,),
        ).fetchone()
    return row is not None


def get_experiment(pool: ConnectionPool, experiment_id: str) -> Optional[dict]:
    with pool.connection() as conn:
        return conn.execute(
            """
            SELECT experiment_id, researcher, sensors,
                   lower_threshold, upper_threshold, status,
                   started_at, terminated_at
            FROM experiment_config
            WHERE experiment_id = %s
            """,
            (experiment_id,),
        ).fetchone()


def list_experiments(
    pool: ConnectionPool, status: Optional[str], limit: int, offset: int
) -> list[dict]:
    with pool.connection() as conn:
        return conn.execute(
            """
            SELECT experiment_id, researcher, sensors,
                   lower_threshold, upper_threshold, status,
                   started_at, terminated_at
            FROM experiment_config
            WHERE %(status)s::text IS NULL OR status = %(status)s
            ORDER BY created_at, experiment_id
            LIMIT %(limit)s OFFSET %(offset)s
            """,
            {"status": status, "limit": limit, "offset": offset},
        ).fetchall()


# ---- temperatures -------------------------------------------------------------

# The single definition of "violation", shared by the out-of-range list and
# the statistics so they can never disagree. These are fixed SQL fragments
# (no user input), so formatting them into the query text is safe.
_AFTER_START = "(c.started_at IS NOT NULL AND m.ts >= c.started_at)"
_BELOW = f"({_AFTER_START} AND m.temperature < c.lower_threshold)"
_ABOVE = f"({_AFTER_START} AND m.temperature > c.upper_threshold)"
_VIOLATION = f"({_BELOW} OR {_ABOVE})"


def temperatures_in_range(
    pool: ConnectionPool,
    experiment_id: str,
    start_time: float,
    end_time: float,
    limit: int,
    offset: int,
) -> list[dict]:
    """Time-range query: measurements with start_time <= ts <= end_time."""
    with pool.connection() as conn:
        return conn.execute(
            """
            SELECT ts AS timestamp, temperature
            FROM measurements
            WHERE experiment_id = %s
              AND ts BETWEEN %s AND %s
            ORDER BY ts, id
            LIMIT %s OFFSET %s
            """,
            (experiment_id, start_time, end_time, limit, offset),
        ).fetchall()


def out_of_range_temperatures(
    pool: ConnectionPool,
    experiment_id: str,
    start_time: Optional[float],
    end_time: Optional[float],
    limit: int,
    offset: int,
) -> list[dict]:
    """Out-of-range query: measurements outside the experiment's thresholds.

    Thresholds are read from experiment_config at query time, so the
    answer always agrees with the configured limits. A value exactly on
    a threshold counts as in range. Only readings taken after the
    experiment started count: warm-up readings during stabilisation are
    expected to be outside the band and are not violations. The time
    window is optional.
    """
    with pool.connection() as conn:
        return conn.execute(
            f"""
            SELECT m.ts AS timestamp, m.temperature
            FROM measurements m
            JOIN experiment_config c USING (experiment_id)
            WHERE m.experiment_id = %(exp)s
              AND {_VIOLATION}
              AND (%(start)s::float8 IS NULL OR m.ts >= %(start)s)
              AND (%(end)s::float8   IS NULL OR m.ts <= %(end)s)
            ORDER BY m.ts, m.id
            LIMIT %(limit)s OFFSET %(offset)s
            """,
            {
                "exp": experiment_id,
                "start": start_time,
                "end": end_time,
                "limit": limit,
                "offset": offset,
            },
        ).fetchall()


def experiment_stats(
    pool: ConnectionPool,
    experiment_id: str,
    start_time: Optional[float],
    end_time: Optional[float],
) -> dict:
    """Summary statistics in one pass over the experiment's measurements.

    count/min/max/avg cover every reading in the window; the out-of-range
    counts use the same rule as out_of_range_temperatures(), so the two
    endpoints always agree.
    """
    with pool.connection() as conn:
        row = conn.execute(
            f"""
            SELECT
                count(m.id)                                        AS count,
                min(m.temperature)                                 AS min_temperature,
                max(m.temperature)                                 AS max_temperature,
                avg(m.temperature)                                 AS avg_temperature,
                min(m.ts)                                          AS first_timestamp,
                max(m.ts)                                          AS last_timestamp,
                count(m.id) FILTER (WHERE {_BELOW})                AS below_lower_count,
                count(m.id) FILTER (WHERE {_ABOVE})                AS above_upper_count
            FROM experiment_config c
            LEFT JOIN measurements m
                   ON m.experiment_id = c.experiment_id
                  AND (%(start)s::float8 IS NULL OR m.ts >= %(start)s)
                  AND (%(end)s::float8   IS NULL OR m.ts <= %(end)s)
            WHERE c.experiment_id = %(exp)s
            GROUP BY c.experiment_id
            """,
            {"exp": experiment_id, "start": start_time, "end": end_time},
        ).fetchone()
    out = row["below_lower_count"] + row["above_upper_count"]
    return {
        "experiment_id": experiment_id,
        **row,
        "out_of_range_count": out,
        "out_of_range_ratio": (out / row["count"]) if row["count"] else 0.0,
    }


def temperature_series(
    pool: ConnectionPool,
    experiment_id: str,
    start_time: float,
    end_time: float,
    interval_s: float,
) -> list[dict]:
    """Downsample measurements into fixed-width time buckets.

    Buckets are aligned to multiples of interval_s (e.g. whole minutes),
    so the same bucket always has the same start time across requests.
    Empty buckets are omitted.
    """
    with pool.connection() as conn:
        return conn.execute(
            """
            SELECT floor(ts / %(iv)s) * %(iv)s AS bucket_start,
                   count(*)                     AS count,
                   avg(temperature)             AS avg_temperature,
                   min(temperature)             AS min_temperature,
                   max(temperature)             AS max_temperature
            FROM measurements
            WHERE experiment_id = %(exp)s
              AND ts BETWEEN %(start)s AND %(end)s
            GROUP BY bucket_start
            ORDER BY bucket_start
            """,
            {"exp": experiment_id, "start": start_time, "end": end_time,
             "iv": interval_s},
        ).fetchall()


def ping(pool: ConnectionPool) -> None:
    with pool.connection() as conn:
        conn.execute("SELECT 1")
