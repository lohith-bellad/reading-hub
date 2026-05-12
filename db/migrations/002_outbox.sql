-- Reading Hub: outbox pattern for article events.
-- Ingestion writes a row here in the same txn as the article INSERT.
-- Ranking worker drains it with FOR UPDATE SKIP LOCKED and updates Redis ZSETs.

BEGIN;

CREATE TABLE article_outbox (
    id           BIGSERIAL   PRIMARY KEY,
    article_id   BIGINT      NOT NULL REFERENCES articles(id) ON DELETE CASCADE,
    event_type   TEXT        NOT NULL DEFAULT 'created'
        CHECK (event_type IN ('created', 'rescore')),
    enqueued_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMPTZ
);

-- Partial index: only unprocessed rows are interesting to the ranking worker.
-- Stays small even after millions of rows, since processed rows are pruned.
CREATE INDEX article_outbox_pending_idx
    ON article_outbox (id)
    WHERE processed_at IS NULL;

COMMIT;
