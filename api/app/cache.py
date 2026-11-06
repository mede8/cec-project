"""API cache creation script."""
import asyncio
import os
import time
from typing import Callable, Awaitable


TTL_S = float(os.getenv("CACHE_TTL_S", "1.0"))
MAX_ENTRIES = int(os.getenv("CACHE_MAX_ENTRIES", "10000"))

# (expires_at, body)
_store = {}
# future that the first request fills in
_loading = {}
_FAILED = object()


async def get_or_load(key: tuple, load: Callable[[], Awaitable[bytes]]) -> tuple[str, bool]:
    """Return the cached body (body, hit) for key, or call load() once and share its result.

    :param key: tuple of everything that changes the result (endpoint, all query params).
    :param load: async function that queries the database and returns the JSON text.
    :return: (body, True) if served from the cache, or (body, False) if the request was queried
    """
    entry = _store.get(key)
    if entry is not None and entry[0] > time.monotonic():
        return entry[1], True

    waiting = _loading.get(key)
    if waiting is not None:
        body = await asyncio.shield(waiting)
        if isinstance(body, Exception):
            # shared query failed
            raise body
        if body is not _FAILED:
            return body, True

    fut = asyncio.get_running_loop().create_future()
    _loading[key] = fut
    body = _FAILED
    try:
        try:
            body = await load()
        except Exception as exc:
            body = exc
            raise

        if len(_store) >= MAX_ENTRIES:
            _store.clear()
        _store[key] = (time.monotonic() + TTL_S, body)
        return body, False
    finally:
        if _loading.get(key) is fut:
            del _loading[key]
        if not fut.done():
            fut.set_result(body)
