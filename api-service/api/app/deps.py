"""Shared request dependencies: the connection pool, settings, pagination
and validation helpers used by several routes."""

from dataclasses import dataclass
from typing import Optional

from fastapi import Depends, HTTPException, Query, Request
from psycopg_pool import ConnectionPool

from . import db
from .config import Settings


def get_pool(request: Request) -> ConnectionPool:
    return request.app.state.pool


def get_settings(request: Request) -> Settings:
    return request.app.state.settings


@dataclass
class Page:
    limit: int
    offset: int


def pagination(
    limit: Optional[int] = Query(
        None, ge=1, description="Maximum rows to return (default: the server maximum)"
    ),
    offset: int = Query(0, ge=0, description="Rows to skip, for paging"),
    settings: Settings = Depends(get_settings),
) -> Page:
    if limit is None:
        limit = settings.max_page_size
    if limit > settings.max_page_size:
        raise HTTPException(
            status_code=400,
            detail=f"limit must be at most {settings.max_page_size}",
        )
    return Page(limit=limit, offset=offset)


def check_window(start_time: Optional[float], end_time: Optional[float]) -> None:
    if start_time is not None and end_time is not None and start_time > end_time:
        raise HTTPException(
            status_code=400, detail="start-time must not be after end-time"
        )


def require_experiment(pool: ConnectionPool, experiment_id: str) -> None:
    if not db.experiment_exists(pool, experiment_id):
        raise_not_found(experiment_id)


def raise_not_found(experiment_id: str):
    raise HTTPException(
        status_code=404, detail=f"Experiment '{experiment_id}' not found"
    )
