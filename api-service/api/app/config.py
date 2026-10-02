"""Service configuration, read once from environment variables.

Every setting has a safe default, so the service starts with no
configuration at all inside docker compose.
"""

import os
from dataclasses import dataclass, field


def _float(name: str, default: float) -> float:
    return float(os.getenv(name, default))


def _int(name: str, default: int) -> int:
    return int(os.getenv(name, default))


def _database_url() -> str:
    url = os.getenv("DATABASE_URL")
    if url:
        return url
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "postgres")
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = os.getenv("POSTGRES_PORT", "5432")
    name = os.getenv("POSTGRES_DB", "observability")
    return f"postgresql://{user}:{password}@{host}:{port}/{name}"


@dataclass(frozen=True)
class Settings:
    database_url: str = field(default_factory=_database_url)
    # Connections kept open to Postgres (shared by all requests).
    pool_min_size: int = field(default_factory=lambda: _int("DB_POOL_MIN_SIZE", 1))
    pool_max_size: int = field(default_factory=lambda: _int("DB_POOL_SIZE", 10))
    # How long startup waits for Postgres before the service gives up.
    connect_timeout_s: float = field(
        default_factory=lambda: _float("DB_CONNECT_TIMEOUT", 30)
    )
    # How long a request waits for a free connection before returning 503.
    request_timeout_s: float = field(
        default_factory=lambda: _float("DB_REQUEST_TIMEOUT", 5)
    )
    # Postgres cancels any single query that runs longer than this.
    statement_timeout_ms: int = field(
        default_factory=lambda: _int("DB_STATEMENT_TIMEOUT_MS", 5000)
    )
    # Upper bound on rows one response may contain.
    max_page_size: int = field(default_factory=lambda: _int("API_MAX_PAGE_SIZE", 10000))
    # Comma-separated origins allowed to call the API from a browser.
    cors_origins: tuple[str, ...] = field(
        default_factory=lambda: tuple(
            o.strip() for o in os.getenv("CORS_ORIGINS", "").split(",") if o.strip()
        )
    )
    log_level: str = field(default_factory=lambda: os.getenv("LOG_LEVEL", "INFO"))


def get_settings() -> Settings:
    return Settings()
