"""Ranking worker.

Drains `article_outbox` and writes to Redis:
  - ZADD feed:global         <score> <article_id>
  - ZADD feed:<category>     <score> <article_id>   (if publisher has a category)
  - SET  article:<id>        <json>                 (for feed-API hydration)
  - Caps each feed ZSET at the top FEED_CAP via ZREMRANGEBYRANK.

Score formula (README §Day 3):
    score = w_f * freshness(age_hours) + w_e * publisher_score

freshness uses half-life decay: 2^(-age / half_life).
"""
from __future__ import annotations

import json
import logging
import math
import os
import time
from datetime import datetime, timezone
from typing import Any

from psycopg.rows import dict_row

from app.cache import get_client
from app.db import connection

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("ranking")

POLL_INTERVAL_SECONDS = 1.0
BATCH_SIZE = 100
FEED_CAP = 500

W_FRESHNESS = 10.0
W_PUBLISHER = 1.0
HALF_LIFE_HOURS = 12.0
ARTICLE_TTL_SECONDS = 14 * 24 * 60 * 60  # 14 days


def freshness(age_hours: float) -> float:
    return math.pow(2.0, -age_hours / HALF_LIFE_HOURS)


def compute_score(published_at: datetime | None, fetched_at: datetime,
                  publisher_score: float) -> float:
    timestamp = published_at or fetched_at
    age_hours = (datetime.now(timezone.utc) - timestamp).total_seconds() / 3600.0
    age_hours = max(0.0, age_hours)
    return W_FRESHNESS * freshness(age_hours) + W_PUBLISHER * publisher_score


def article_json(row: dict[str, Any]) -> bytes:
    payload = {
        "id": row["article_id"],
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
    return json.dumps(payload, separators=(",", ":")).encode("utf-8")


def drain_batch() -> int:
    """Drain up to BATCH_SIZE outbox events. Returns count processed."""
    fetch_sql = """
        SELECT
            o.id              AS outbox_id,
            a.id              AS article_id,
            a.url, a.title, a.summary, a.author, a.thumbnail_url,
            a.published_at, a.fetched_at, a.metadata,
            p.id              AS publisher_id,
            p.name            AS publisher_name,
            p.category, p.publisher_score
        FROM article_outbox o
        JOIN articles a   ON a.id = o.article_id
        JOIN publishers p ON p.id = a.publisher_id
        WHERE o.processed_at IS NULL
        ORDER BY o.id
        LIMIT %s
        FOR UPDATE OF o SKIP LOCKED
    """
    mark_sql = "UPDATE article_outbox SET processed_at = NOW() WHERE id = ANY(%s)"

    redis_client = get_client()

    with connection() as conn:
        with conn.transaction():
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(fetch_sql, (BATCH_SIZE,))
                rows = cur.fetchall()

                if not rows:
                    return 0

                pipe = redis_client.pipeline(transaction=False)
                for row in rows:
                    score = compute_score(
                        row["published_at"], row["fetched_at"], row["publisher_score"]
                    )
                    article_id = row["article_id"]

                    pipe.zadd("feed:global", {article_id: score})
                    if cat := row["category"]:
                        pipe.zadd(f"feed:{cat}", {article_id: score})

                    pipe.set(f"article:{article_id}", article_json(row),
                             ex=ARTICLE_TTL_SECONDS)

                # Cap ZSETs to top FEED_CAP (highest scores).
                pipe.zremrangebyrank("feed:global", 0, -FEED_CAP - 1)
                categories_touched = {r["category"] for r in rows if r["category"]}
                for cat in categories_touched:
                    pipe.zremrangebyrank(f"feed:{cat}", 0, -FEED_CAP - 1)

                pipe.execute()

                outbox_ids = [r["outbox_id"] for r in rows]
                cur.execute(mark_sql, (outbox_ids,))
                return len(rows)


def main() -> None:
    log.info("ranking worker starting; poll every %.1fs", POLL_INTERVAL_SECONDS)
    while True:
        start = time.monotonic()
        try:
            n = drain_batch()
            if n:
                elapsed_ms = (time.monotonic() - start) * 1000
                log.info("processed %d events in %.1fms", n, elapsed_ms)
        except Exception:
            log.exception("drain failed")
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
