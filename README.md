# Reading Hub

A self-hosted personal news aggregator. Polls 10–15 RSS/Atom feeds, ranks articles in Redis sorted sets, and serves a cursor-paginated infinite-scroll feed.

**Live:** [darkbuffers.com/hub](https://darkbuffers.com/hub)

Built in a week to get real production miles on **Redis (ZSETs, cursor pagination, event-driven ranking)**, **Postgres (outbox pattern, JSONB metadata, partial indexes)**, and **Nginx as an API gateway**.

---

## What it does

1. A worker polls each publisher's RSS/Atom feed on an adaptive schedule — feeds that publish often get polled often, quiet feeds back off.
2. New articles land in Postgres with `ON CONFLICT (url) DO NOTHING` for dedupe, and in the same transaction an event is written to an outbox table.
3. A ranking worker drains the outbox, computes a freshness-decayed score, and stores articles in Redis sorted sets (`feed:global`, `feed:<category>`).
4. The API serves a cursor-paginated feed backed by `ZREVRANGEBYSCORE` + `MGET` for article hydration, with Postgres as the fallback cache layer.
5. Clicks increment a sliding-window trending counter; a 30s sweeper folds trending into the composite score so popular articles surface.
6. Nginx fronts the API with per-IP rate limiting and TLS.
7. Prometheus scrapes the API; Grafana dashboards show ingestion lag, cache hit rate, articles/day.

---

## Architecture

```
   ┌───────────┐  RSS poll    ┌────────────┐
   │ Publishers│ ───────────▶ │ Ingestion  │
   └───────────┘              │  Worker    │  adaptive polling, ETag/304
                              └─────┬──────┘
                                    │ INSERT articles + outbox  (one txn)
                          ┌─────────▼──────────┐
                          │ Postgres           │
                          │ (source of truth)  │
                          └─────────┬──────────┘
                                    │ FOR UPDATE SKIP LOCKED
                                    ▼
                          ┌────────────────────┐
                          │ Ranking Worker     │  score = freshness + publisher
                          │ score → ZADD       │
                          └─────────┬──────────┘
                                    │
                          ┌─────────▼──────────┐         clicks → trending counter
                          │ Redis              │ ◀────── (5min TTL, log-scaled)
                          │  feed:global ZSET  │
                          │  feed:<cat> ZSET   │
                          │  article:<id>      │
                          │  trending:<id>:5m  │
                          └─────────┬──────────┘
                                    │ ZREVRANGEBYSCORE + MGET
                                    ▼
   Browser ──▶ Nginx ──▶ Feed API (FastAPI)
   /hub        /api        ├── /api/feed?cursor=…
   (static)    (rate-      ├── /api/article/{id}/click → 302
                limited)   └── /api/metrics  (Prometheus)
                                    │
                          ┌─────────▼──────────┐
                          │ Prometheus ─▶ Grafana
                          └────────────────────┘
```

---

## Tech stack

- **Backend** — FastAPI (Python 3.12), psycopg 3, redis-py, feedparser, httpx
- **Datastore** — Postgres 16
- **Cache + ranking** — Redis 7
- **Gateway** — Nginx (path routing, rate limiting, TLS via Cloudflare Origin Certificate)
- **Frontend** — Vanilla JS + CSS, no build step, infinite scroll via `IntersectionObserver`
- **Container runtime** — Docker + Compose
- **Observability** — Prometheus + Grafana

---

## Repo layout

```
reading-hub/
├── README.md
├── backend/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app/
│       ├── main.py          ← FastAPI routes
│       ├── feed.py          ← cursor pagination + hydration
│       ├── ingestion.py     ← RSS polling worker
│       ├── ranking.py       ← outbox → score → ZADD worker
│       ├── trending.py      ← click-based score sweeper
│       ├── clicks.py        ← click endpoint logic
│       ├── metrics.py       ← Prometheus exposition
│       ├── db.py            ← psycopg3 connection pool
│       └── cache.py         ← Redis client
├── frontend/
│   ├── index.html
│   ├── style.css
│   └── app.js
├── db/
│   └── migrations/
│       ├── 001_init.sql      ← publishers, articles, ingest_runs
│       └── 002_outbox.sql    ← article_outbox
└── ops/
    └── seed_publishers.sql   ← ~10 starter feeds
```

---

## Running locally

```bash
git clone <repo> reading-hub
cd reading-hub
cp .env.example .env
docker compose up -d --build
docker exec -i hub-postgres psql -U hub -d readinghub < ops/seed_publishers.sql

curl http://localhost:8080/api/health        # → {"ok": true}
open http://localhost:8080/hub/
```

The compose file brings up Postgres, Redis, the API (`hub-backend`), and the three workers (`hub-ingestion`, `hub-ranking`, `hub-trending`). Within 1–2 minutes you should see articles streaming into the feed.

---

## Design notes

### Outbox pattern
Ingestion inserts a new article and enqueues an event in `article_outbox` in the **same transaction**. The ranking worker drains the outbox with `SELECT … FOR UPDATE SKIP LOCKED LIMIT 100`, computes scores, and updates Redis in a pipeline. Either both rows commit (article + event) or neither does — no orphaned articles missing from the feed, no orphan events pointing at nothing.

```sql
INSERT INTO articles (...) VALUES (...) ON CONFLICT (url) DO NOTHING RETURNING id;
-- RETURNING fires only for actual inserts; conflicts skip the outbox row.
INSERT INTO article_outbox (article_id) VALUES (%s);
```

### Cursor pagination on (score, id)
The cursor encodes `{"s": score, "i": article_id}` as base64. The next page fetches `ZREVRANGEBYSCORE feed:global cursor_score -inf LIMIT 0 N+M`, then filters client-side to skip items at or above `(cursor_score, cursor_id)`. This is stable under concurrent writes — new high-scoring articles appear at the top of the feed on a refresh but never re-appear mid-scroll. No `OFFSET`, no skipped articles, no duplicates.

### Adaptive polling
Each publisher has a `poll_interval_seconds` that the ingestion worker updates after every poll:
- New articles found → shrink interval toward `MIN_POLL_SECONDS` (5 min)
- No new articles → grow interval toward `MAX_POLL_SECONDS` (24 hr)
- Errors → exponential backoff up to 6 hr

ETag and Last-Modified headers are persisted on the publisher row, so a feed that hasn't changed returns 304 — no parse, no insert, no outbox event. Saves bandwidth and DB churn for quiet feeds.

### Ranking score
```
score = 10 * 2^(-age_hours / 12) + 1.0 * publisher_score + log2(1 + clicks)
```

- 12-hour half-life on freshness — a 12h-old article has half the freshness of a fresh one.
- Publisher score is a hand-tuned multiplier per source (high-quality feeds float higher).
- Click boost uses log-scaling so a viral article doesn't drown the feed.

The trending sweeper recomputes this every 30s for every article in `feed:global`, so click boosts decay naturally as the underlying trending counter expires (5-min TTL).

### Cache hydration with fallback
The feed API does one `MGET article:<id_1> … article:<id_20>` per page. Cache misses fall back to a batched Postgres query and warm Redis on the way out. The cache hit rate stabilizes near 100% in steady state, since the ranking worker writes every new article to `article:<id>` at ingest time.

### Partial indexes for hot queries
The polling worker queries publishers due for polling: `WHERE active AND next_poll_at <= NOW()`. A partial index on `(next_poll_at) WHERE active` keeps this query at index-only-scan speed even as deactivated publishers accumulate. Same story for `article_outbox(id) WHERE processed_at IS NULL` — the outbox stays small and fast regardless of how many events have been processed.

### Rate limiting in Nginx, not the app
`limit_req_zone $binary_remote_addr zone=hub_api:10m rate=5r/s;` at the gateway layer means an abusive client never reaches FastAPI in the first place. The `burst=10 nodelay` lets users with bursty navigation patterns through without being penalized for catching up after coming back to a tab.

---

## Observability

The API exposes `/api/metrics` in Prometheus text format:

| Metric | Type | What it tells you |
|---|---|---|
| `feed_cache_hits_total` / `_misses_total` | counter | Redis cache effectiveness |
| `feed_cache_hit_rate` | gauge | Running ratio since process start |
| `ingestion_lag_seconds` | gauge | Time since the most-stale active publisher was polled |
| `articles_last_24h` | gauge | Throughput sanity check |
| `feed_global_size` | gauge | `ZCARD feed:global` — should hover near the cap |

A pre-provisioned Grafana dashboard visualizes these. If ingestion lag spikes, a feed is broken; if hit rate dips, the cache TTL needs tuning.

---

## Stretch goals

| # | Idea | Why it's interesting |
|---|------|----------------------|
| 1 | SimHash dedupe across publishers | Same story syndicated 5 times shows up once |
| 2 | Per-user personalization vector | Click-stream-driven preference weights |
| 3 | Full-text search via `tsvector` + GIN | One-line addition, big UX win |
| 4 | Two-week retention + cold archive | Postgres native partitioning by month |
| 5 | SSE `/api/feed/live` for push updates | New articles appear without refresh |
| 6 | pgvector for embedding-based dedup | Semantic dedup beyond SimHash |

---

## What I learned

- **Redis ZSETs in production** — `ZADD`, `ZREVRANGEBYSCORE`, `ZREMRANGEBYRANK`, pipelines for batch ops.
- **Cursor-based pagination** is genuinely tricky with ties; the inclusive-bound + client-filter pattern is correct and cheap.
- **Outbox over LISTEN/NOTIFY** for a single-consumer pipeline — easier to reason about, easier to debug, naturally idempotent.
- **Adaptive polling** beats flat cron by an order of magnitude on bandwidth — most feeds publish once a day, not every 5 minutes.
- **Nginx as gateway** is still the right answer for path routing + rate limiting + TLS at low scale. No reason to reach for Traefik/Envoy at this size.

---

## License

MIT.
