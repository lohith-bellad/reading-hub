"""Click tracking.

GET /api/article/{id}/click → INCR trending:{id}:5min (TTL 5min) → 302 to URL.
"""
from __future__ import annotations

import logging

from app.cache import get_client
from app.db import connection

log = logging.getLogger("clicks")

TRENDING_TTL_SECONDS = 5 * 60


def record_click(article_id: int) -> str | None:
    """Record a click and return the article URL, or None if unknown."""
    redis = get_client()

    key = f"trending:{article_id}:5min"
    pipe = redis.pipeline(transaction=False)
    pipe.incr(key)
    pipe.expire(key, TRENDING_TTL_SECONDS)
    pipe.execute()

    cached = redis.get(f"article:{article_id}")
    if cached:
        import json
        try:
            return json.loads(cached).get("url")
        except json.JSONDecodeError:
            pass

    with connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT url FROM articles WHERE id = %s", (article_id,))
        row = cur.fetchone()
        return row[0] if row else None
