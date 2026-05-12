"""In-process counters + Prometheus text exposition.

Hand-rolled (no prometheus-client dep). Process-local — fine while uvicorn
runs a single worker. If we ever scale workers, move counters to Redis.
"""
from __future__ import annotations

import threading
from datetime import timedelta
from typing import Iterator

from app.cache import get_client
from app.db import connection

_lock = threading.Lock()
_counters: dict[str, int] = {
    "feed_cache_hits_total": 0,
    "feed_cache_misses_total": 0,
}


def incr(name: str, n: int = 1) -> None:
    with _lock:
        _counters[name] = _counters.get(name, 0) + n


def _ingestion_lag_seconds() -> float:
    sql = """
        SELECT EXTRACT(EPOCH FROM (NOW() - MIN(last_polled_at)))::float
        FROM publishers WHERE active AND last_polled_at IS NOT NULL
    """
    with connection() as conn, conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()
        return float(row[0]) if row and row[0] is not None else 0.0


def _articles_last_24h() -> int:
    with connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM articles WHERE fetched_at > NOW() - INTERVAL '1 day'")
        return int(cur.fetchone()[0])


def _feed_size() -> int:
    return int(get_client().zcard("feed:global"))


def render() -> str:
    with _lock:
        hits = _counters.get("feed_cache_hits_total", 0)
        misses = _counters.get("feed_cache_misses_total", 0)
    total = hits + misses
    hit_rate = (hits / total) if total else 0.0

    def lines() -> Iterator[str]:
        yield "# HELP feed_cache_hits_total Redis MGET hits during feed hydration."
        yield "# TYPE feed_cache_hits_total counter"
        yield f"feed_cache_hits_total {hits}"
        yield "# HELP feed_cache_misses_total Redis MGET misses during feed hydration."
        yield "# TYPE feed_cache_misses_total counter"
        yield f"feed_cache_misses_total {misses}"
        yield "# HELP feed_cache_hit_rate Rolling cache hit rate since process start."
        yield "# TYPE feed_cache_hit_rate gauge"
        yield f"feed_cache_hit_rate {hit_rate:.6f}"
        yield "# HELP ingestion_lag_seconds Seconds since the oldest active publisher was polled."
        yield "# TYPE ingestion_lag_seconds gauge"
        yield f"ingestion_lag_seconds {_ingestion_lag_seconds():.3f}"
        yield "# HELP articles_last_24h Articles fetched in the last 24 hours."
        yield "# TYPE articles_last_24h gauge"
        yield f"articles_last_24h {_articles_last_24h()}"
        yield "# HELP feed_global_size Current cardinality of feed:global ZSET."
        yield "# TYPE feed_global_size gauge"
        yield f"feed_global_size {_feed_size()}"

    return "\n".join(lines()) + "\n"
