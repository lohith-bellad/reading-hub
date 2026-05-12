"""RSS/Atom ingestion worker.

Loop:
  1. Pick due publishers (next_poll_at <= now, active=TRUE), oldest first.
  2. Fetch feed with httpx, honoring ETag/Last-Modified (conditional GET).
  3. Parse with feedparser.
  4. Insert articles ON CONFLICT (url) DO NOTHING.
  5. Update publisher's next_poll_at via adaptive cadence.
  6. Log to ingest_runs.

Idle 60s between sweeps.
"""
from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import feedparser
import httpx
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from app.db import connection

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("ingestion")

SWEEP_INTERVAL_SECONDS = 60
HTTP_TIMEOUT = 20.0
USER_AGENT = "reading-hub/0.1 (+https://darkbuffers.com/hub)"

# Adaptive polling clamps.
MIN_POLL_SECONDS = 5 * 60          # 5 min
MAX_POLL_SECONDS = 24 * 60 * 60    # 1 day
BACKOFF_FACTOR = 2.0
MAX_BACKOFF_SECONDS = 6 * 60 * 60  # 6 hr


@dataclass
class Publisher:
    id: int
    name: str
    feed_url: str
    poll_interval_seconds: int
    etag: str | None
    last_modified: str | None
    consecutive_failures: int


def fetch_due_publishers(limit: int = 50) -> list[Publisher]:
    sql = """
        SELECT id, name, feed_url, poll_interval_seconds, etag, last_modified,
               consecutive_failures
        FROM publishers
        WHERE active AND next_poll_at <= NOW()
        ORDER BY next_poll_at ASC
        LIMIT %s
    """
    with connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, (limit,))
        return [Publisher(**row) for row in cur.fetchall()]


def start_ingest_run(publisher_id: int) -> int:
    with connection() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO ingest_runs (publisher_id) VALUES (%s) RETURNING id",
            (publisher_id,),
        )
        return cur.fetchone()[0]


def finish_ingest_run(
    run_id: int,
    status: str,
    http_status: int | None,
    found: int,
    inserted: int,
    error: str | None,
) -> None:
    with connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE ingest_runs
            SET finished_at = NOW(),
                status = %s,
                http_status = %s,
                articles_found = %s,
                articles_inserted = %s,
                error = %s
            WHERE id = %s
            """,
            (status, http_status, found, inserted, error, run_id),
        )


def upsert_articles(publisher_id: int, entries: list[dict[str, Any]]) -> int:
    """Insert new articles + enqueue outbox events in one transaction.

    Uses RETURNING id to capture only rows that were actually inserted
    (ON CONFLICT DO NOTHING skips RETURNING for conflicts).
    """
    if not entries:
        return 0

    insert_sql = """
        INSERT INTO articles
            (publisher_id, url, title, summary, author, thumbnail_url,
             published_at, metadata)
        VALUES
            (%(publisher_id)s, %(url)s, %(title)s, %(summary)s, %(author)s,
             %(thumbnail_url)s, %(published_at)s, %(metadata)s)
        ON CONFLICT (url) DO NOTHING
        RETURNING id
    """
    outbox_sql = "INSERT INTO article_outbox (article_id) VALUES (%s)"

    inserted = 0
    with connection() as conn:
        with conn.transaction():
            with conn.cursor() as cur:
                for entry in entries:
                    entry["publisher_id"] = publisher_id
                    cur.execute(insert_sql, entry)
                    row = cur.fetchone()
                    if row is None:
                        continue  # duplicate URL, nothing inserted
                    article_id = row[0]
                    cur.execute(outbox_sql, (article_id,))
                    inserted += 1
    return inserted


def parse_entry(entry: Any) -> dict[str, Any] | None:
    url = entry.get("link")
    title = entry.get("title")
    if not url or not title:
        return None

    summary = entry.get("summary") or entry.get("description")
    author = entry.get("author")

    thumbnail = None
    if media := entry.get("media_thumbnail"):
        thumbnail = media[0].get("url")
    elif media := entry.get("media_content"):
        thumbnail = media[0].get("url")

    published_at = None
    for key in ("published_parsed", "updated_parsed"):
        if value := entry.get(key):
            published_at = datetime(*value[:6], tzinfo=timezone.utc)
            break

    metadata = {}
    if tags := entry.get("tags"):
        metadata["tags"] = [t.get("term") for t in tags if t.get("term")]
    if entry_id := entry.get("id"):
        metadata["guid"] = entry_id

    return {
        "url": url,
        "title": title.strip(),
        "summary": summary,
        "author": author,
        "thumbnail_url": thumbnail,
        "published_at": published_at,
        "metadata": Jsonb(metadata),
    }


def compute_next_poll(pub: Publisher, inserted: int, failed: bool) -> tuple[int, datetime]:
    """Adaptive cadence.

    On success:
      - inserted > 0  → shrink interval toward MIN (feed is alive)
      - inserted == 0 → grow interval toward MAX (feed is quiet)
    On failure: exponential backoff up to MAX_BACKOFF_SECONDS.
    """
    interval = pub.poll_interval_seconds

    if failed:
        interval = min(int(interval * BACKOFF_FACTOR), MAX_BACKOFF_SECONDS)
    elif inserted > 0:
        interval = max(int(interval * 0.75), MIN_POLL_SECONDS)
    else:
        interval = min(int(interval * 1.25), MAX_POLL_SECONDS)

    interval = max(MIN_POLL_SECONDS, min(interval, MAX_POLL_SECONDS))
    next_at = datetime.now(timezone.utc) + timedelta(seconds=interval)
    return interval, next_at


def update_publisher_state(
    pub: Publisher,
    new_interval: int,
    next_at: datetime,
    etag: str | None,
    last_modified: str | None,
    failed: bool,
) -> None:
    if failed:
        sql = """
            UPDATE publishers
            SET poll_interval_seconds = %s,
                next_poll_at = %s,
                consecutive_failures = consecutive_failures + 1,
                last_polled_at = NOW()
            WHERE id = %s
        """
        params = (new_interval, next_at, pub.id)
    else:
        sql = """
            UPDATE publishers
            SET poll_interval_seconds = %s,
                next_poll_at = %s,
                consecutive_failures = 0,
                last_polled_at = NOW(),
                last_success_at = NOW(),
                etag = COALESCE(%s, etag),
                last_modified = COALESCE(%s, last_modified)
            WHERE id = %s
        """
        params = (new_interval, next_at, etag, last_modified, pub.id)

    with connection() as conn, conn.cursor() as cur:
        cur.execute(sql, params)


def poll_publisher(client: httpx.Client, pub: Publisher) -> None:
    log.info("polling %s (id=%d)", pub.name, pub.id)
    run_id = start_ingest_run(pub.id)

    headers = {"User-Agent": USER_AGENT}
    if pub.etag:
        headers["If-None-Match"] = pub.etag
    if pub.last_modified:
        headers["If-Modified-Since"] = pub.last_modified

    status: str
    http_status: int | None = None
    found = 0
    inserted = 0
    error: str | None = None
    failed = False
    new_etag = None
    new_last_modified = None

    try:
        resp = client.get(pub.feed_url, headers=headers, timeout=HTTP_TIMEOUT)
        http_status = resp.status_code

        if resp.status_code == 304:
            status = "not_modified"
        elif resp.status_code >= 400:
            status = "error"
            error = f"HTTP {resp.status_code}"
            failed = True
        else:
            parsed = feedparser.parse(resp.content)
            entries = [parse_entry(e) for e in parsed.entries]
            entries = [e for e in entries if e is not None]
            found = len(entries)
            inserted = upsert_articles(pub.id, entries)
            new_etag = resp.headers.get("etag")
            new_last_modified = resp.headers.get("last-modified")
            status = "success"
    except Exception as e:  # network error, parse blowup, etc.
        status = "error"
        error = f"{type(e).__name__}: {e}"
        failed = True
        log.exception("poll failed for %s", pub.name)

    finish_ingest_run(run_id, status, http_status, found, inserted, error)

    new_interval, next_at = compute_next_poll(pub, inserted, failed)
    update_publisher_state(pub, new_interval, next_at, new_etag, new_last_modified, failed)

    log.info(
        "%s: status=%s http=%s found=%d inserted=%d next_in=%ds",
        pub.name, status, http_status, found, inserted, new_interval,
    )


def sweep() -> None:
    pubs = fetch_due_publishers()
    if not pubs:
        log.info("no due publishers")
        return

    log.info("sweep: %d due", len(pubs))
    with httpx.Client(follow_redirects=True) as client:
        for pub in pubs:
            poll_publisher(client, pub)


def main() -> None:
    log.info("ingestion worker starting; sweep every %ds", SWEEP_INTERVAL_SECONDS)
    while True:
        start = time.monotonic()
        try:
            sweep()
        except Exception:
            log.exception("sweep failed")
        elapsed = time.monotonic() - start
        sleep_for = max(0.0, SWEEP_INTERVAL_SECONDS - elapsed)
        time.sleep(sleep_for)


if __name__ == "__main__":
    main()
