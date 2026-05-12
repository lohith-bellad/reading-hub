-- Reading Hub: initial schema (Day 1)
-- Tables: publishers, articles, ingest_runs

BEGIN;

CREATE TABLE publishers (
    id                    BIGSERIAL PRIMARY KEY,
    name                  TEXT        NOT NULL,
    feed_url              TEXT        NOT NULL UNIQUE,
    site_url              TEXT,
    category              TEXT,
    publisher_score       REAL        NOT NULL DEFAULT 1.0,
    active                BOOLEAN     NOT NULL DEFAULT TRUE,
    poll_interval_seconds INTEGER     NOT NULL DEFAULT 900,
    next_poll_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_polled_at        TIMESTAMPTZ,
    last_success_at       TIMESTAMPTZ,
    consecutive_failures  INTEGER     NOT NULL DEFAULT 0,
    etag                  TEXT,
    last_modified         TEXT,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Polling worker: pick up due, active feeds in oldest-first order.
CREATE INDEX publishers_due_idx
    ON publishers (next_poll_at)
    WHERE active;

CREATE TABLE articles (
    id            BIGSERIAL   PRIMARY KEY,
    publisher_id  BIGINT      NOT NULL REFERENCES publishers(id) ON DELETE CASCADE,
    url           TEXT        NOT NULL UNIQUE,
    title         TEXT        NOT NULL,
    summary       TEXT,
    author        TEXT,
    thumbnail_url TEXT,
    published_at  TIMESTAMPTZ,
    fetched_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata      JSONB       NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX articles_publisher_published_idx
    ON articles (publisher_id, published_at DESC);

CREATE INDEX articles_published_idx
    ON articles (published_at DESC NULLS LAST);

CREATE INDEX articles_metadata_gin_idx
    ON articles USING GIN (metadata jsonb_path_ops);

CREATE TABLE ingest_runs (
    id                 BIGSERIAL   PRIMARY KEY,
    publisher_id       BIGINT      NOT NULL REFERENCES publishers(id) ON DELETE CASCADE,
    started_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at        TIMESTAMPTZ,
    status             TEXT        NOT NULL DEFAULT 'running'
        CHECK (status IN ('running', 'success', 'error', 'not_modified')),
    http_status        INTEGER,
    articles_found     INTEGER     NOT NULL DEFAULT 0,
    articles_inserted  INTEGER     NOT NULL DEFAULT 0,
    error              TEXT
);

CREATE INDEX ingest_runs_publisher_started_idx
    ON ingest_runs (publisher_id, started_at DESC);

COMMIT;
