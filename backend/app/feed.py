"""Feed API: cursor-paginated ranked feed.

Cursor format: urlsafe-base64 of JSON {"s": <score>, "i": <article_id>}.
Cursor is the *last* item of the previous page; the next page is items strictly
"after" it under (score DESC, id DESC) ordering.

Hot path:
  1. ZSET lookup gives [(id, score), ...] for the page.
  2. MGET article:<id> hydrates each.
  3. Misses fall back to Postgres + warm the cache.
"""
from __future__ import annotations

import base64
import json
import logging
from dataclasses import dataclass
from typing import Any

from psycopg.rows import dict_row

from app.cache import get_client
from app.db import connection
from app import metrics

log = logging.getLogger("feed")

MAX_LIMIT = 50
DEFAULT_LIMIT = 20
ARTICLE_TTL_SECONDS = 14 * 24 * 60 * 60  # mirrors ranking.py
TIE_BUFFER = 50  # extra items fetched to safely filter ties at cursor score


@dataclass
class Cursor:
    score: float
    id: int

    def encode(self) -> str:
        raw = json.dumps({"s": self.score, "i": self.id}, separators=(",", ":")).encode()
        return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()

    @classmethod
    def decode(cls, token: str) -> "Cursor":
        # Re-pad for base64.
        padded = token + "=" * (-len(token) % 4)
        data = json.loads(base64.urlsafe_b64decode(padded))
        return cls(score=float(data["s"]), id=int(data["i"]))


def _zset_page(feed_key: str, limit: int, cursor: Cursor | None) -> list[tuple[int, float]]:
    """Return up to `limit` (article_id, score) pairs from the ZSET, after cursor."""
    redis = get_client()

    if cursor is None:
        raw = redis.zrevrangebyscore(
            feed_key, "+inf", "-inf", start=0, num=limit, withscores=True
        )
        return [(int(m), float(s)) for m, s in raw]

    # Inclusive upper bound (the cursor score itself). Pull extra to filter ties.
    raw = redis.zrevrangebyscore(
        feed_key, cursor.score, "-inf",
        start=0, num=limit + TIE_BUFFER, withscores=True,
    )

    out: list[tuple[int, float]] = []
    for member, score in raw:
        article_id = int(member)
        # Skip items at-or-above the cursor under (score DESC, id DESC).
        if score > cursor.score:
            continue
        if score == cursor.score and article_id >= cursor.id:
            continue
        out.append((article_id, score))
        if len(out) >= limit:
            break
    return out


def _hydrate_from_db(missing_ids: list[int]) -> dict[int, dict[str, Any]]:
    sql = """
        SELECT
            a.id, a.url, a.title, a.summary, a.author, a.thumbnail_url,
            a.published_at,
            p.id   AS publisher_id,
            p.name AS publisher_name,
            p.category
        FROM articles a JOIN publishers p ON p.id = a.publisher_id
        WHERE a.id = ANY(%s)
    """
    out: dict[int, dict[str, Any]] = {}
    with connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, (missing_ids,))
        for row in cur.fetchall():
            out[row["id"]] = {
                "id": row["id"],
                "url": row["url"],
                "title": row["title"],
                "summary": row["summary"],
                "author": row["author"],
                "thumbnail_url": row["thumbnail_url"],
                "published_at": row["published_at"].isoformat() if row["published_at"] else None,
                "publisher": {
                    "id": row["publisher_id"],
                    "name": row["publisher_name"],
                    "category": row["category"],
                },
            }
    return out


def _hydrate(article_ids: list[int]) -> dict[int, dict[str, Any]]:
    """MGET from Redis; Postgres fallback for misses; warm cache for fallbacks."""
    if not article_ids:
        return {}

    redis = get_client()
    keys = [f"article:{i}".encode() for i in article_ids]
    raw = redis.mget(keys)

    out: dict[int, dict[str, Any]] = {}
    missing: list[int] = []
    for article_id, blob in zip(article_ids, raw):
        if blob is None:
            missing.append(article_id)
            continue
        try:
            out[article_id] = json.loads(blob)
        except json.JSONDecodeError:
            missing.append(article_id)

    hits = len(article_ids) - len(missing)
    if hits:
        metrics.incr("feed_cache_hits_total", hits)
    if missing:
        metrics.incr("feed_cache_misses_total", len(missing))
        log.info("cache miss on %d/%d ids", len(missing), len(article_ids))
        fallback = _hydrate_from_db(missing)
        if fallback:
            pipe = redis.pipeline(transaction=False)
            for article_id, payload in fallback.items():
                blob = json.dumps(payload, separators=(",", ":")).encode("utf-8")
                pipe.set(f"article:{article_id}", blob, ex=ARTICLE_TTL_SECONDS)
            pipe.execute()
        out.update(fallback)

    return out


def get_feed(
    category: str | None = None,
    limit: int = DEFAULT_LIMIT,
    cursor_token: str | None = None,
) -> dict[str, Any]:
    limit = max(1, min(limit, MAX_LIMIT))
    cursor = Cursor.decode(cursor_token) if cursor_token else None
    feed_key = f"feed:{category}" if category else "feed:global"

    page = _zset_page(feed_key, limit, cursor)
    if not page:
        return {"items": [], "next_cursor": None}

    article_ids = [aid for aid, _ in page]
    hydrated = _hydrate(article_ids)

    items: list[dict[str, Any]] = []
    for article_id, score in page:
        if record := hydrated.get(article_id):
            items.append({**record, "score": score})

    last_id, last_score = page[-1]
    next_cursor = Cursor(score=last_score, id=last_id).encode() if len(page) == limit else None

    return {"items": items, "next_cursor": next_cursor}
