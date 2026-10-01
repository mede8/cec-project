"""Cross-cutting request handling: request IDs, access logs and metrics.

For every request this middleware:
  1. takes the caller's X-Request-ID or generates one, and echoes it back
     in the response, so one request can be traced through the logs;
  2. writes one access-log line (method, route, status, duration);
  3. updates the Prometheus metrics served at /metrics.
"""

import logging
import time
import uuid

from prometheus_client import Counter, Histogram
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

log = logging.getLogger("api.access")

REQUESTS = Counter(
    "api_requests_total",
    "HTTP requests handled",
    ["method", "route", "status"],
)
LATENCY = Histogram(
    "api_request_duration_seconds",
    "Time spent handling a request",
    ["method", "route"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
)


def _route_template(request: Request) -> str:
    # Label metrics by the route pattern ("/experiments/{experiment_id}"),
    # not the raw path, so each experiment id does not become a new series.
    route = request.scope.get("route")
    return getattr(route, "path", "unmatched")


class RequestContextMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        request_id = request.headers.get("x-request-id") or uuid.uuid4().hex
        request.state.request_id = request_id
        start = time.perf_counter()
        status = 500
        try:
            response = await call_next(request)
            status = response.status_code
            response.headers["X-Request-ID"] = request_id
            return response
        finally:
            elapsed = time.perf_counter() - start
            route = _route_template(request)
            if route != "/metrics":
                REQUESTS.labels(request.method, route, str(status)).inc()
                LATENCY.labels(request.method, route).observe(elapsed)
            log.info(
                "request_id=%s method=%s path=%s status=%s duration_ms=%.1f",
                request_id,
                request.method,
                request.url.path,
                status,
                elapsed * 1000,
            )
