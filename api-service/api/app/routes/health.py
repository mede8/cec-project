"""Operational endpoints: health checks and Prometheus metrics."""

from fastapi import APIRouter, Depends, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from psycopg_pool import ConnectionPool

from .. import db
from ..deps import get_pool
from ..models import Health

router = APIRouter(tags=["operations"])


@router.get("/health/live", response_model=Health, summary="Is the process up?")
def live():
    """Answers as long as the process is running. Never touches the
    database, so a Postgres outage does not make the container look dead
    and get restarted for no reason."""
    return {"status": "ok"}


@router.get(
    "/health", response_model=Health, summary="Can the service answer queries?"
)
def ready(pool: ConnectionPool = Depends(get_pool)):
    """Checks the database too; returns 503 if Postgres is unreachable."""
    db.ping(pool)  # a database error becomes 503 in main.py
    return {"status": "ok"}


@router.get("/metrics", include_in_schema=False)
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
