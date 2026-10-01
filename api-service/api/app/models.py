"""Response schemas. FastAPI uses these to validate every response and to
generate the interactive documentation at /docs."""

from pydantic import BaseModel, Field


class Measurement(BaseModel):
    timestamp: float = Field(description="Unix epoch seconds")
    temperature: float = Field(description="Averaged over the experiment's sensors")


class Experiment(BaseModel):
    experiment_id: str
    researcher: str | None
    sensors: list[str]
    lower_threshold: float
    upper_threshold: float
    status: str = Field(description="configured | stabilizing | running | terminated")
    started_at: float | None = Field(
        None, description="When stabilisation ended, epoch seconds")
    terminated_at: float | None = Field(None, description="Epoch seconds")


class ExperimentStats(BaseModel):
    experiment_id: str
    count: int = Field(description="Number of measurements in the window")
    min_temperature: float | None
    max_temperature: float | None
    avg_temperature: float | None
    first_timestamp: float | None
    last_timestamp: float | None
    out_of_range_count: int
    below_lower_count: int
    above_upper_count: int
    out_of_range_ratio: float = Field(description="out_of_range_count / count, 0 if empty")


class SeriesBucket(BaseModel):
    bucket_start: float = Field(description="Start of the bucket, epoch seconds")
    count: int
    avg_temperature: float
    min_temperature: float
    max_temperature: float


class Health(BaseModel):
    status: str
