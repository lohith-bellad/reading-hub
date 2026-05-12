from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import PlainTextResponse, RedirectResponse

from app import metrics
from app.clicks import record_click
from app.feed import DEFAULT_LIMIT, MAX_LIMIT, get_feed

app = FastAPI(title="Reading Hub")


@app.get("/api/health")
def health():
    return {"ok": True}


@app.get("/api/feed")
def feed(
    cursor: str | None = Query(default=None),
    limit: int = Query(default=DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
    category: str | None = Query(default=None),
):
    try:
        return get_feed(category=category, limit=limit, cursor_token=cursor)
    except (ValueError, KeyError) as exc:
        raise HTTPException(status_code=400, detail=f"invalid cursor: {exc}") from exc


@app.get("/api/article/{article_id}/click")
def click(article_id: int):
    url = record_click(article_id)
    if not url:
        raise HTTPException(status_code=404, detail="article not found")
    return RedirectResponse(url=url, status_code=302)


@app.get("/api/metrics", response_class=PlainTextResponse)
def metrics_endpoint():
    return PlainTextResponse(metrics.render(), media_type="text/plain; version=0.0.4")
