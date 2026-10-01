"""Temperature queries: the two endpoints required by the assignment, plus
a downsampled time series for charts."""

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from psycopg_pool import ConnectionPool

from .. import db
from ..config import Settings
from ..deps import (Page, check_window, get_pool, get_settings, pagination,
                    require_experiment)
from ..models import Measurement, SeriesBucket

router = APIRouter(prefix="/temperature", tags=["temperature"])

EXPERIMENT_ID = Query(..., alias="experiment-id", min_length=1,
                      description="Experiment to query")


@router.get(
    "",
    response_model=list[Measurement],
    summary="Temperatures in a time window",
)
def get_temperature(
    experiment_id: str = EXPERIMENT_ID,
    start_time: float = Query(..., alias="start-time",
                              description="Window start, epoch seconds (inclusive)"),
    end_time: float = Query(..., alias="end-time",
                            description="Window end, epoch seconds (inclusive)"),
    page: Page = Depends(pagination),
    pool: ConnectionPool = Depends(get_pool),
):
    """Averaged temperatures of one experiment with
    `start-time <= timestamp <= end-time`, oldest first."""
    check_window(start_time, end_time)
    require_experiment(pool, experiment_id)
    return db.temperatures_in_range(
        pool, experiment_id, start_time, end_time, page.limit, page.offset
    )


@router.get(
    "/out-of-range",
    response_model=list[Measurement],
    summary="Temperatures outside the experiment's thresholds",
)
def get_out_of_range(
    experiment_id: str = EXPERIMENT_ID,
    start_time: Optional[float] = Query(None, alias="start-time",
                                        description="Optional window start"),
    end_time: Optional[float] = Query(None, alias="end-time",
                                      description="Optional window end"),
    page: Page = Depends(pagination),
    pool: ConnectionPool = Depends(get_pool),
):
    """Measurements below `lower_threshold` or above `upper_threshold`,
    oldest first. A value exactly on a threshold counts as in range."""
    check_window(start_time, end_time)
    require_experiment(pool, experiment_id)
    return db.out_of_range_temperatures(
        pool, experiment_id, start_time, end_time, page.limit, page.offset
    )


@router.get(
    "/series",
    response_model=list[SeriesBucket],
    summary="Downsampled temperatures for charts",
)
def get_series(
    experiment_id: str = EXPERIMENT_ID,
    start_time: float = Query(..., alias="start-time"),
    end_time: float = Query(..., alias="end-time"),
    interval: float = Query(60, gt=0, description="Bucket width in seconds"),
    pool: ConnectionPool = Depends(get_pool),
    settings: Settings = Depends(get_settings),
):
    """Groups measurements into fixed-width buckets (for example one per
    minute) with the average, minimum and maximum in each. A chart can
    then draw a long experiment without downloading every measurement.
    Empty buckets are left out."""
    check_window(start_time, end_time)
    if (end_time - start_time) / interval > settings.max_page_size:
        raise HTTPException(
            status_code=400,
            detail=(f"too many buckets: use an interval of at least "
                    f"{(end_time - start_time) / settings.max_page_size:g} s"),
        )
    require_experiment(pool, experiment_id)
    return db.temperature_series(pool, experiment_id, start_time, end_time, interval)
