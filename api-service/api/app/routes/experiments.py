"""Experiment metadata and summary statistics."""

from typing import Literal, Optional

from fastapi import APIRouter, Depends, Query
from psycopg_pool import ConnectionPool

from .. import db
from ..deps import Page, check_window, get_pool, pagination, raise_not_found
from ..models import Experiment, ExperimentStats

router = APIRouter(prefix="/experiments", tags=["experiments"])

Status = Literal["configured", "stabilizing", "running", "terminated"]


@router.get("", response_model=list[Experiment], summary="List experiments")
def list_experiments(
    status: Optional[Status] = Query(None, description="Only this status"),
    page: Page = Depends(pagination),
    pool: ConnectionPool = Depends(get_pool),
):
    return db.list_experiments(pool, status, page.limit, page.offset)


@router.get(
    "/{experiment_id}", response_model=Experiment, summary="One experiment"
)
def get_experiment(experiment_id: str, pool: ConnectionPool = Depends(get_pool)):
    experiment = db.get_experiment(pool, experiment_id)
    if experiment is None:
        raise_not_found(experiment_id)
    return experiment


@router.get(
    "/{experiment_id}/stats",
    response_model=ExperimentStats,
    summary="Summary statistics",
)
def get_stats(
    experiment_id: str,
    start_time: Optional[float] = Query(None, alias="start-time"),
    end_time: Optional[float] = Query(None, alias="end-time"),
    pool: ConnectionPool = Depends(get_pool),
):
    """Count, minimum, maximum and average temperature, plus how many
    readings fell below or above the thresholds. Optionally limited to a
    time window."""
    check_window(start_time, end_time)
    if not db.experiment_exists(pool, experiment_id):
        raise_not_found(experiment_id)
    return db.experiment_stats(pool, experiment_id, start_time, end_time)
