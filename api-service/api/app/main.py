"""REST API of the Temperature Observability Service.

Researchers query the measurements that the consumer stores in Postgres.
This module wires the application together; the endpoints live in
app/routes/ and all SQL lives in app/db.py.

Timestamps are Unix epoch seconds, the same unit as the Kafka events.
"""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from psycopg import OperationalError
from psycopg.errors import QueryCanceled
from psycopg_pool import PoolTimeout

from . import db
from .config import get_settings
from .middleware import RequestContextMiddleware
from .routes import experiments, health, temperature

log = logging.getLogger("api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    app.state.settings = settings
    app.state.pool = db.create_pool(settings)
    log.info("connected to Postgres, pool size %s", settings.pool_max_size)
    try:
        yield
    finally:
        app.state.pool.close()


app = FastAPI(
    title="Temperature Observability API",
    version="2.0.0",
    description=(
        "Query temperatures that the observability consumer stores in "
        "Postgres. Timestamps are Unix epoch seconds."
    ),
    lifespan=lifespan,
)

app.add_middleware(RequestContextMiddleware)
_origins = get_settings().cors_origins
if _origins:
    app.add_middleware(
        CORSMiddleware, allow_origins=list(_origins), allow_methods=["GET"]
    )

app.include_router(temperature.router)
app.include_router(experiments.router)
app.include_router(health.router)


def _error(status: int, detail: str, request: Request) -> JSONResponse:
    return JSONResponse(
        status_code=status,
        content={"detail": detail},
        headers={"X-Request-ID": getattr(request.state, "request_id", "")},
    )


# QueryCanceled is a subclass of OperationalError, so it is registered
# separately to give a more specific message.
@app.exception_handler(QueryCanceled)
async def query_too_slow(request: Request, exc: QueryCanceled):
    log.warning("query cancelled by statement_timeout: %s", exc)
    return _error(503, "query took too long, try a smaller time window", request)


@app.exception_handler(OperationalError)
@app.exception_handler(PoolTimeout)
async def database_unavailable(request: Request, exc: Exception):
    log.error("database unavailable: %s", exc)
    return _error(503, "database unavailable", request)
