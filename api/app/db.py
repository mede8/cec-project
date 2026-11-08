"""Read-only db access for rest API

the API only reads from 2 tables:
- temperatures: 1 row per avg measurement, with the out_of_range flag
                the consumer showed from experiments.
- experiments: experiment details (thresholds, phase), api
               works even if consumer never fills the table.

queries are parameterised (%s) => prevent sql injection
"""
import os
import time
import logging
from typing import Optional
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from . import timing


log = logging.getLogger("api.db")


def database_url() -> str:
    """Gets database URl from .env or builds it from postgres.

    :returns: str with db url
    """
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


async def _configure(conn) -> None:
    """Configure api connection to have read-only access.
    
    :param conn: db connection to configure
    """
    await conn.execute("SET default_transaction_read_only = on")


async def create_pool() -> AsyncConnectionPool:
    """Create and open pool but asynchronous. Called once per worker process at startup.

    :returns: the async connection pool for the worker process.
    """
    size = int(os.getenv("API_POOL_SIZE", "8"))
    pool = AsyncConnectionPool(
        conninfo=database_url(),
        min_size=size,
        max_size=size,
        # req waits this to connect, if not 503 error
        timeout=float(os.getenv("DB_REQUEST_TIMEOUT", "5")),
        kwargs={
            "row_factory": dict_row,
            "autocommit": True,
            # cancel query if too long to run
            "options": f"-c statement_timeout={int(os.getenv('DB_STATEMENT_TIMEOUT_MS', '5000'))}",
        },
        configure=_configure,
        open=False,
    )
    # open pool
    await pool.open()
    # if db is unreachable, fail at startup, docker compose restart
    await pool.wait(timeout=float(os.getenv("DB_CONNECT_TIMEOUT", "30")))

    return pool


async def ping(pool: AsyncConnectionPool) -> None:
    """Ping db asynchronously to check if it is reachable.
    
    :param pool: async connection pool
    """
    async with pool.connection() as conn:
        await conn.execute("SELECT 1")


# temperatures
def _json_points(where: str) -> str:
    """Query to return JSON of readings from a specific where from temperatures table.

    :param where: condition used in the `where` of the query
    :returns: query string containing the information requested
    """
    return f"""
        SELECT coalesce(json_agg(page), '[]')::text AS body
        FROM (
            SELECT ts AS "timestamp", temperature
            FROM temperatures
            WHERE {where}
            ORDER BY ts, measurement_id
            LIMIT %(limit)s OFFSET %(offset)s
        ) AS page
    """


# wheres for the _json_points query (temp & out-of-range)
_TEMPERATURE_JSON = _json_points("experiment_id = %(exp)s AND ts BETWEEN %(start)s AND %(end)s")
_OUT_OF_RANGE_JSON = {
    (has_start, has_end): _json_points(
        " AND ".join(
            ["experiment_id = %(exp)s", "out_of_range"]
            + (["ts >= %(start)s"] if has_start else [])
            + (["ts <= %(end)s"] if has_end else [])
        )
    )
    for has_start in (False, True)
    for has_end in (False, True)
}


async def temperatures_in_range(
        pool, experiment_id: str, start: float, end: float,
        limit: Optional[int], offset: int) -> str:
    """Get JSON of the temperatures in range written by the consumer.
        - start <= ts <= end with oldest first

    :param pool: async connection pool
    :param experiment_id: experiment id
    :param start: window start in epoch seconds
    :param end: window end in epoch seconds
    :param limit: optional maximum number of rows
    :param offset: number of rows to skip
    :returns: if no limit then return all rows, otherwise json str of temperatures
              within specified range
    """
    async with pool.connection() as conn:
        current = await conn.execute(
            _TEMPERATURE_JSON, {"exp": experiment_id, "start": start, "end": end, "limit": limit, "offset": offset}
        )
        return (await current.fetchone())["body"]


async def out_of_range_temperatures(
        pool, experiment_id: str, start: Optional[float],
        end: Optional[float], limit: Optional[int], offset: int) -> str:
    """Get JSON of out-of-range temperatures written by the consumer.
        - just read flag from consumer with oldest first

    :param pool: async connection pool
    :param experiment_id: experiment id
    :param start: optional window start in epoch seconds
    :param end: optional window end in epoch seconds
    :param limit: optional maximum number of rows
    :param offset: number of rows to skip
    :returns: JSON str of out-of-range temperatures within the range
    """
    sql = _OUT_OF_RANGE_JSON[(start is not None, end is not None)]
    t0 = time.perf_counter()
    async with pool.connection() as conn:
        # got a connection
        t1 = time.perf_counter()
        current = await conn.execute(
            sql, {"exp": experiment_id, "start": start, "end": end, "limit": limit, "offset": offset}
        )
        body = (await current.fetchone())["body"]
        # query done
        t2 = time.perf_counter()
    timing.record("pool_wait", (t1 - t0) * 1000)
    timing.record("query", (t2 - t1) * 1000)
    return body


async def temperature_series(
        pool, experiment_id: str, start: float, end: float,
        interval: float) -> list[dict]:
    """Get temperature series for an experiment, grouped into fixed-width time buckets (for charts)
    buckets are aligned to multiple intervals, so they have same start between req
    ignore empty buckets

    :param pool: async connection pool
    :param experiment_id: experiment id
    :param start: window start in epoch seconds
    :param end: window end in epoch seconds
    :param interval: width of time buckets in seconds
    :returns: list of dict with temperature series in grouped buckets
    """
    async with pool.connection() as conn:
        current = await conn.execute(
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
        )

        return await current.fetchall()


# experiments
_EXPERIMENT_COLUMNS = """experiment_id, researcher, sensors, lower_threshold, upper_threshold, phase"""


async def experiment_known(pool, experiment_id: str) -> bool:
    """Check if the experiment is in experiments/temperatures tables.

    :param pool: async connection pool
    :param experiment_id: experiment id
    :returns: true if experiment exists
    """
    async with pool.connection() as conn:
        current = await conn.execute(
            """
            SELECT EXISTS (SELECT 1 FROM experiments  WHERE experiment_id = %(e)s)
                OR EXISTS (SELECT 1 FROM temperatures WHERE experiment_id = %(e)s)
                AS known
            """,
            {"e": experiment_id},
        )

        row = await current.fetchone()

    return row["known"]


async def get_experiment(pool, experiment_id: str) -> Optional[dict]:
    """Get details from a specific experiment
        - check in experiments table and if not check in temperatures for minimal records

    :param pool: async connection pool
    :param experiment_id: experiment id
    :returns: dict with experiment or None if not found
    """
    async with pool.connection() as conn:
        current = await conn.execute(
            f"SELECT {_EXPERIMENT_COLUMNS} FROM experiments WHERE experiment_id = %s",
            (experiment_id,),
        )
        row = await current.fetchone()

    if row is not None:
        return row

    if await experiment_known(pool, experiment_id):
        return {"experiment_id": experiment_id, "researcher": None, "sensors": None,
                "lower_threshold": None, "upper_threshold": None, "phase": None}
    return None


async def list_experiments(pool, phase: Optional[str], limit: Optional[int], offset: int) -> list[dict]:
    """Get list of experiments and find from experiments table & temperatures filters by phase.
    
    :param pool: async connection pool
    :param phase: optional filter by phase
    :param limit: optional maximum number of rows
    :param offset: number of rows to skip
    :returns: list of dict with experiments
    """
    async with pool.connection() as conn:
        current = await conn.execute(
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
        )

        return await current.fetchall()


async def experiment_stats(
        pool, experiment_id: str, start: Optional[float],
        end: Optional[float]) -> dict:
    """Gets summary of temperature statistics for an experiment

    :param pool: async connection pool
    :param experiment_id: experiment id
    :param start: optional window start in epoch seconds
    :param end: optional window end in epoch seconds
    :returns: dict with experiment temperature stats
    """
    async with pool.connection() as conn:
        current = await conn.execute(
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
        )
        row = await current.fetchone()

    row["experiment_id"] = experiment_id
    row["out_of_range_ratio"] = (row["out_of_range_count"] / row["count"]) if row["count"] else 0.0

    return row
