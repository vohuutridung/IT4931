from __future__ import annotations

from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse, Response
from fastapi.middleware.cors import CORSMiddleware

from serving.merge_service import ServeQuery

app = FastAPI(title="Social Lambda Pipeline API", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)
service = ServeQuery()


def problem(status: int, title: str, detail: str) -> JSONResponse:
    return JSONResponse(
        status_code=status,
        content={"type": "about:blank", "title": title, "status": status, "detail": detail},
    )


@app.exception_handler(HTTPException)
async def http_exception_handler(_request, exc: HTTPException):
    return problem(exc.status_code, "Request failed", str(exc.detail))


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/api/v1/posts")
def posts(
    platform: str | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    limit: int = Query(100, ge=1, le=500),
) -> dict:
    end = end or datetime.now(timezone.utc)
    start = start or end - timedelta(hours=24)
    return {"data": service.query_posts(platform, start, end, limit), "limit": limit}


@app.get("/api/v1/sentiment/trend")
def sentiment_trend(
    platform: str | None = None,
    granularity: str = Query("hour", pattern="^(hour|day)$"),
    start: datetime | None = None,
    end: datetime | None = None,
) -> dict:
    end = end or datetime.now(timezone.utc)
    start = start or end - timedelta(days=7)
    return {"data": service.query_sentiment_trend(platform, granularity, start, end)}


@app.get("/api/v1/hashtags/top")
def top_hashtags(
    platform: str | None = None,
    window_hours: int = Query(24),
    top_n: int = Query(20, ge=1, le=100),
) -> dict:
    if window_hours not in {1, 6, 24, 168}:
        raise HTTPException(status_code=400, detail="window_hours must be one of 1, 6, 24, 168")
    return {"data": service.query_top_hashtags(platform, window_hours, top_n)}


@app.get("/api/v1/stats/realtime")
def realtime_stats(platform: str | None = None) -> dict:
    return service.query_realtime_stats(platform)


@app.get("/metrics")
def metrics() -> Response:
    try:
        from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
    except Exception:
        return Response(b"", media_type="text/plain")
