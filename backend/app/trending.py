"""Trending sweeper.

Every TRENDING_SWEEP_SECONDS:
  1. List all article ids in feed:global.
  2. MGET trending:{id}:5min for each.
  3. Recompute composite = base_score + W_TREND * log2(1 + clicks).
  4. ZADD feed:global (and feed:<category>) with the new score.

Base score components come from Postgres (publisher_score + freshness).
"""
from __future__ import annotations

import logging
import math
import os
import time
from datetime import datetime, timezone

from psycopg.rows import dict_row

from app.cache import get_client
from app.db import connection

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("trending")

TRENDING_SWEEP_SECONDS = 30
W_FRESHNESS = 10.0
W_PUBLISHER = 1.0
W_TREND = 1.0
HALF_LIFE_HOURS = 12.0


def base_score(published_at: datetime | None, fetched_at: datetime,
               publisher_score: float) -> float:
    timestamp = published_at or fetched_at
    age_hours = (datetime.now(timezone.utc) - timestamp).total_seconds() / 3600.0
    freshness = math.pow(2.0, -max(0.0, age_hours) / HALF_LIFE_HOURS)
    return W_FRESHNESS * freshness + W_PUBLISHER * publisher_score


def trend_boost(clicks: int) -> float:
    return W_TREND * math.log2(1 + clicks) if clicks > 0 else 0.0


def sweep() -> int:
    redis = get_client()
    ids_raw = redis.zrange("feed:global", 0, -1)
    if not ids_raw:
        return 0

    article_ids = [int(m) for m in ids_raw]

    pipe = redis.pipeline(transaction=False)
    for aid in article_ids:
        pipe.get(f"trending:{aid}:5min")
    click_blobs = pipe.execute()
    clicks_by_id = {
        aid: int(blob) if blob else 0
        for aid, blob in zip(article_ids, click_blobs)
    }

    sql = """
        SELECT a.id, a.published_at, a.fetched_at,
               p.publisher_score, p.category
        FROM articles a JOIN publishers p ON p.id = a.publisher_id
        WHERE a.id = ANY(%s)
    """
    with connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, (article_ids,))
        rows = {r["id"]: r for r in cur.fetchall()}

    pipe = redis.pipeline(transaction=False)
    nudged = 0
    for aid in article_ids:
        row = rows.get(aid)
        if not row:
            continue
        score = base_score(row["published_at"], row["fetched_at"], row["publisher_score"])
        boost = trend_boost(clicks_by_id.get(aid, 0))
        composite = score + boost
        pipe.zadd("feed:global", {aid: composite})
        if cat := row["category"]:
            pipe.zadd(f"feed:{cat}", {aid: composite})
        nudged += 1
    pipe.execute()
    return nudged


def main() -> None:
    log.info("trending sweeper starting; interval %ds", TRENDING_SWEEP_SECONDS)
    while True:
        start = time.monotonic()
        try:
            n = sweep()
            elapsed_ms = (time.monotonic() - start) * 1000
            log.info("rescored %d articles in %.1fms", n, elapsed_ms)
        except Exception:
            log.exception("sweep failed")
        elapsed = time.monotonic() - start
        time.sleep(max(0.0, TRENDING_SWEEP_SECONDS - elapsed))


if __name__ == "__main__":
    main()
