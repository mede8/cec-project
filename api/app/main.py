"""The API of the service

required endpoints:
  GET /temperature?experiment-id=&start-time=&end-time=
  GET /temperature/out-of-range?experiment-id=

extra endpoints:
  GET /temperature/series -> readings grouped into time buckets (charts)
  GET /experiments -> list experiments
  GET /experiments/{id} -> get 1 specificexperiment
  GET /experiments/{id}/stats -> stats of 1 experiment: count, min, max, average, out-of-range count
  GET /health -> api & db reachable (503 if db down)
  GET /health/live -> api process is up
"""
import logging
import math
import os
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse, Response
import psycopg
from psycopg import OperationalError
from psycopg.errors import QueryCanceled
from psycopg_pool import PoolTimeout
from pydantic import BaseModel, Field

from . import db, timing, cache


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("api")

MAX_BUCKETS = 10_000  # buckets in 1 /temperature/series response
MAX_LIMIT = 1_000_000  # largest page size
MAX_OFFSET = 2_147_483_647
SLOW_MS = float(os.getenv("SLOW_MS", "200"))  # log req slower than this
MAX_IN_FLIGHT = int(os.getenv("MAX_IN_FLIGHT", "64"))
in_flight = 0


# response models
class Measurement(BaseModel):
    timestamp: float = Field(description="Unix epoch seconds")
    temperature: float = Field(description="Average over the experiment's sensors")


class SeriesBucket(BaseModel):
    bucket_start: float
    count: int
    avg_temperature: float
    min_temperature: float
    max_temperature: float
    out_of_range_count: int


class Experiment(BaseModel):
    experiment_id: str
    researcher: Optional[str] = None
    sensors: Optional[list[str]] = None
    lower_threshold: Optional[float] = None
    upper_threshold: Optional[float] = None
    phase: Optional[str] = Field(
        None, description="configured | stabilization | running | terminated; null otherwise"
    )


class ExperimentStats(BaseModel):
    experiment_id: str
    count: int
    min_temperature: Optional[float]
    max_temperature: Optional[float]
    avg_temperature: Optional[float]
    first_timestamp: Optional[float]
    last_timestamp: Optional[float]
    out_of_range_count: int
    out_of_range_ratio: float


class Health(BaseModel):
    status: str


# app & lifecycle
@asynccontextmanager
async def lifespan(app: FastAPI):
    # sync endpoints run in a thread pool
    # default = 40 threads
    app.state.pool = await db.create_pool()
    log.info("connected to the database")
    try:
        yield
    finally:
        await app.state.pool.close()


app = FastAPI(
    title="Temperature Observability API", version="1.0.0",
    description="Query temperatures stored by the observability consumer.",
    lifespan=lifespan
)


class RequestContext:
    """
    Custom middleware that tracks requests and monitors performance.
    """
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        global in_flight
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        if in_flight >= MAX_IN_FLIGHT:
            # too many requests in flight, reject with 503
            timing.record("rejected", 0)
            await send({"type": "http.response.start", "status": 503,
                        "headers": [(b"content-type", b"text/plain"), (b"content-length", b"4")]})
            await send({"type": "http.response.body", "body": b"busy"})
            return

        rid = next((v.decode("latin-1") for k, v in scope["headers"] if k == b"x-request-id"), None) or uuid.uuid4().hex
        scope.setdefault("state", {})["request_id"] = rid   # read by request.state.request_id
        start, status = time.perf_counter(), 500

        async def send_wrapper(message):
            nonlocal status
            if message["type"] == "http.response.start":
                status = message["status"]
                message["headers"] = [*message.get("headers", []), (b"x-request-id", rid.encode("latin-1", "replace"))]
            await send(message)

        in_flight += 1
        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            in_flight -= 1
            ms = (time.perf_counter() - start) * 1000
            if scope["path"].startswith("/temperature"):
                timing.record("app_total", ms)
            if status >= 500 or ms > SLOW_MS:
                log.warning("request_id=%s %s %s -> %s (%.1f ms)", rid, scope["method"],
                            scope["path"], status, ms)


app.add_middleware(RequestContext)


def _error(request: Request, status: int, detail: str) -> JSONResponse:
    return JSONResponse(status_code=status, content={"detail": detail},
                        headers={"X-Request-ID": getattr(request.state, "request_id", "")})


@app.exception_handler(QueryCanceled)
async def query_too_slow(request: Request, exc: QueryCanceled):
    log.warning("query cancelled by statement_timeout: %s", exc)
    return _error(request, 503, "query took too long, try a smaller time window")


@app.exception_handler(OperationalError)
async def database_unavailable(request: Request, exc: Exception):
    log.error("database unavailable: %s", exc)
    return _error(request, 503, "database unavailable")


@app.exception_handler(PoolTimeout)
async def pool_busy(request: Request, exc: PoolTimeout):
    log.warning("no free database connections available: %s", exc)
    return _error(request, 503, "server busy, try again")


@app.exception_handler(psycopg.Error)
async def database_error(request: Request, exc: psycopg.Error):
    # Anything else from the database: log it, return JSON with the request ID.
    log.exception("database error: %s", exc)
    return _error(request, 500, "internal database error")


# input helpers
def parse_time(value: Optional[str], name: str) -> Optional[float]:
    """
    parse time from epoch seconds/ISO-8601 to --> epoch seconds
    time without a zone is UTC
    """
    if value is None:
        return None
    try:
        number = float(value)
    except ValueError:
        number = None
    if number is not None:
        if not math.isfinite(number):
            raise HTTPException(422, f"{name} must be a finite number, got {value!r}")
        return number
    try:
        dt = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        raise HTTPException(422, f"{name} must be epoch seconds or an ISO-8601 time, got {value!r}")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def check_window(start: Optional[float], end: Optional[float]) -> None:
    if start is not None and end is not None and start > end:
        raise HTTPException(400, "start-time must not be after end-time")


def check_id(experiment_id: str) -> str:
    # postgres text cannot contain null bytes, so reject
    if "\x00" in experiment_id:
        raise HTTPException(422, "experiment-id must not contain NUL characters")
    return experiment_id


EXPERIMENT_ID = Query(..., alias="experiment-id", min_length=1, description="Experiment to query")
# no limit by default: endpoints return the whole list
# (load generator compares the whole response)
LIMIT = Query(None, ge=1, le=MAX_LIMIT, description="Optional maximum number of rows (default: all)")
OFFSET = Query(0, ge=0, le=MAX_OFFSET, description="Rows to skip, for paging")


# endpoints
@app.get("/temperature", response_model=list[Measurement], tags=["temperature"],
         summary="Temperatures in a time window")
async def get_temperature(
    request: Request,
    experiment_id: str = EXPERIMENT_ID,
    start_time: str = Query(..., alias="start-time", description="Window start (inclusive)"),
    end_time: str = Query(..., alias="end-time", description="Window end (inclusive)"),
    limit: Optional[int] = LIMIT,
    offset: int = OFFSET,
):
    """
    get averaged temperatures of 1 experiment
        - start-time <= ts <= end-time, oldest first
    unknown experiment/window without readings returns []
    """
    start, end = parse_time(start_time, "start-time"), parse_time(end_time, "end-time")
    check_window(start, end)
    experiment_id = check_id(experiment_id)
    pool = request.app.state.pool

    async def load():
        return await db.temperatures_in_range(pool, experiment_id, start, end, limit, offset)

    # this needs to be checked, otherwise set TTL_S to 0.25
    if end < time.time() - 2:
        # closed window: cannot change any more, safe to cache
        body, hit = await cache.get_or_load(("temp", experiment_id, start, end, limit, offset), load)
        timing.record("cache_hit" if hit else "cache_miss", 0)
    else:
        # open window, always query
        body = await load()
        timing.record("cache_bypass", 0)
    return Response(content=body, media_type="application/json")   # JSON built by Postgres


@app.get("/temperature/out-of-range", response_model=list[Measurement], tags=["temperature"],
         summary="Temperatures outside the experiment's thresholds")
async def get_out_of_range(
    request: Request,
    experiment_id: str = EXPERIMENT_ID,
    start_time: Optional[str] = Query(None, alias="start-time", description="Optional window start"),
    end_time: Optional[str] = Query(None, alias="end-time", description="Optional window end"),
    limit: Optional[int] = LIMIT,
    offset: int = OFFSET,
):
    """
    get out-of-range temperatures of 1 experiment
    consumer flags those out-of-range temp, oldest first
    """
    start, end = parse_time(start_time, "start-time"), parse_time(end_time, "end-time")
    check_window(start, end)
    experiment_id = check_id(experiment_id)
    pool = request.app.state.pool

    async def load():
        return await db.out_of_range_temperatures(pool, experiment_id, start, end, limit, offset)

    body, hit = await cache.get_or_load(("oor", experiment_id, start, end, limit, offset), load)
    timing.record("cache_hit" if hit else "cache_miss", 0)
    return Response(content=body, media_type="application/json")   # JSON built by Postgres


@app.get("/temperature/series", response_model=list[SeriesBucket], tags=["temperature"],
         summary="Readings grouped into time buckets, for charts")
async def get_series(
    request: Request,
    experiment_id: str = EXPERIMENT_ID,
    start_time: str = Query(..., alias="start-time"),
    end_time: str = Query(..., alias="end-time"),
    interval: float = Query(60, gt=0, description="Bucket width in seconds"),
):
    if not math.isfinite(interval):
        raise HTTPException(422, "interval must be a finite number")
    start, end = parse_time(start_time, "start-time"), parse_time(end_time, "end-time")
    check_window(start, end)
    if (end - start) / interval > MAX_BUCKETS:
        raise HTTPException(400, f"too many buckets: use an interval of at least "
                                 f"{(end - start) / MAX_BUCKETS:g} s")
    return await db.temperature_series(request.app.state.pool, check_id(experiment_id), start, end, interval)


@app.get("/experiments", response_model=list[Experiment], tags=["experiments"],
         summary="List experiments")
async def list_experiments(
    request: Request,
    phase: Optional[str] = Query(None, pattern="^(configured|stabilization|running|terminated)$"),
    limit: Optional[int] = LIMIT,
    offset: int = OFFSET,
):
    return await db.list_experiments(request.app.state.pool, phase, limit, offset)


@app.get("/experiments/{experiment_id}", response_model=Experiment, tags=["experiments"],
         summary="One experiment")
async def get_experiment(request: Request, experiment_id: str):
    exp = await db.get_experiment(request.app.state.pool, check_id(experiment_id))
    if exp is None:
        raise HTTPException(404, f"Experiment '{experiment_id}' not found")
    return exp


@app.get("/experiments/{experiment_id}/stats", response_model=ExperimentStats,
         tags=["experiments"], summary="Summary statistics")
async def get_stats(
    request: Request,
    experiment_id: str,
    start_time: Optional[str] = Query(None, alias="start-time"),
    end_time: Optional[str] = Query(None, alias="end-time"),
):
    start, end = parse_time(start_time, "start-time"), parse_time(end_time, "end-time")
    check_window(start, end)
    pool = request.app.state.pool
    check_id(experiment_id)
    if not await db.experiment_known(pool, experiment_id):
        raise HTTPException(404, f"Experiment '{experiment_id}' not found")
    return await db.experiment_stats(pool, experiment_id, start, end)


@app.get("/health", response_model=Health, tags=["operations"],
         summary="Can the API answer queries? (503 if the database is down)")
async def health(request: Request):
    await db.ping(request.app.state.pool)
    return {"status": "ok"}


@app.get("/health/live", response_model=Health, tags=["operations"],
         summary="Is the process up? (never touches the database)")
async def live():
    return {"status": "ok"}
