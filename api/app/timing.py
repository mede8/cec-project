"""Collects request timings and logs a summary."""
import logging
import time


log = logging.getLogger("api.timing")
# list of durations in ms
_samples = {}
_last_log = time.monotonic()


def record(part: str, ms: float) -> None:
    """Records a timing sample for a specific part of the request.

    :param part: name of the part of the request being timed.
    :param ms: duration in milliseconds.
    """
    global _last_log
    _samples.setdefault(part, []).append(ms)
    now = time.monotonic()
    if now - _last_log > 5:
        _last_log = now
        for name, values in _samples.items():
            values.sort()
            log.info("timing %s avg=%.1fms p95=%.1fms max=%.1fms n=%d", name,
                     sum(values) / len(values), values[int(0.95 * (len(values) - 1))],
                     values[-1], len(values))
        _samples.clear()
        _last_log = now
