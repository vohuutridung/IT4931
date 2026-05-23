from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse, Response
from fastapi.middleware.cors import CORSMiddleware

from serving.merge_service import ServeQuery

logger = logging.getLogger(__name__)

app = FastAPI(title="Social Lambda Pipeline API", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


@app.middleware("http")
async def add_cache_control_header(request: Request, call_next):
    response = await call_next(request)
    if request.url.path.startswith("/api/v1/"):
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    return response

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
    if platform and platform not in {"reddit", "facebook", "instagram"}:
        raise HTTPException(status_code=400, detail="platform must be one of 'reddit', 'facebook', 'instagram'")
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
    if platform and platform not in {"reddit", "facebook", "instagram"}:
        raise HTTPException(status_code=400, detail="platform must be one of 'reddit', 'facebook', 'instagram'")
    end = end or datetime.now(timezone.utc)
    start = start or end - timedelta(days=7)
    return {"data": service.query_sentiment_trend(platform, granularity, start, end)}


@app.get("/api/v1/hashtags/top")
def top_hashtags(
    platform: str | None = None,
    window_hours: int = Query(24),
    top_n: int = Query(20, ge=1, le=100),
    week: str | None = Query(None),
) -> dict:
    if platform and platform not in {"reddit", "facebook", "instagram"}:
        raise HTTPException(status_code=400, detail="platform must be one of 'reddit', 'facebook', 'instagram'")
    if window_hours not in {1, 6, 24, 168}:
        raise HTTPException(status_code=400, detail="window_hours must be one of 1, 6, 24, 168")
    return {"data": service.query_top_hashtags(platform, window_hours, top_n, week)}


@app.get("/api/v1/hashtags/weeks")
def hashtag_weeks(platform: str | None = None) -> dict:
    if platform and platform not in {"reddit", "facebook", "instagram"}:
        raise HTTPException(status_code=400, detail="platform must be one of 'reddit', 'facebook', 'instagram'")
    return {"data": service.query_hashtag_weeks(platform)}


@app.get("/api/v1/stats/realtime")
def realtime_stats(platform: str | None = None) -> dict:
    if platform and platform not in {"reddit", "facebook", "instagram"}:
        raise HTTPException(status_code=400, detail="platform must be one of 'reddit', 'facebook', 'instagram'")
    return service.query_realtime_stats(platform)


@app.get("/metrics")
def metrics() -> Response:
    try:
        from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
    except ImportError as exc:
        logger.error("Prometheus client not available: %s", exc)
        raise HTTPException(status_code=503, detail="Metrics unavailable")
    except Exception as exc:
        logger.error("Error generating metrics: %s", exc)
        raise HTTPException(status_code=500, detail="Error generating metrics")
