from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone

# Sanitize SSL environment variables if they point to non-existent files/directories
for var in ["SSL_CERT_FILE", "SSL_CERT_DIR"]:
    if var in os.environ and not os.path.exists(os.environ[var]):
        del os.environ[var]

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

from serving.merge_service import ServeQuery
from serving.topic_service import TopicService
from serving.network_service import NetworkService
from config.settings import API_ADMIN_TOKEN, API_ALLOW_ENV_WRITES, API_CORS_ALLOW_ORIGINS

logger = logging.getLogger(__name__)

app = FastAPI(title="Social Lambda Pipeline API", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=list(API_CORS_ALLOW_ORIGINS),
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type", "X-Admin-Token"],
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
topics = TopicService()
network_svc = NetworkService()


def require_admin_token(x_admin_token: str | None = Header(default=None, alias="X-Admin-Token")) -> None:
    if not API_ADMIN_TOKEN:
        raise HTTPException(status_code=403, detail="Admin API is disabled until API_ADMIN_TOKEN is configured")
    if x_admin_token != API_ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid admin token")


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
    data = service.query_posts(platform, start, end, limit)
    return {"data": data, "limit": limit}


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
    data = service.query_sentiment_trend(platform, granularity, start, end)
    velocities = [float(r.get("velocity") or 0) for r in data[1:]]
    avg_velocity = round(sum(velocities) / len(velocities), 4) if velocities else 0.0
    total_posts = sum(int(r.get("post_count") or 0) for r in data)
    return {
        "data": data,
        "meta": {
            "trend_direction": service.trend_direction(data),
            "avg_velocity": avg_velocity,
            "total_posts": total_posts,
            "buckets": len(data),
        },
    }


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
    import re
    if week and not re.match(r"^\d{4}-\d{2}-\d{2}( \d{2}:\d{2}:\d{2})?$", week):
        raise HTTPException(status_code=400, detail="Invalid week format")
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


# ── Topic Modeling endpoints ─────────────────────────────────────────────────

@app.get("/api/v1/topics/distribution")
def topic_distribution(platform: str | None = None) -> dict:
    """Per-topic post count, keywords, and UMAP 2-D position."""
    if platform and platform not in {"reddit", "facebook", "instagram"}:
        raise HTTPException(status_code=400, detail="platform must be one of 'reddit', 'facebook', 'instagram'")
    result = topics.query_topic_distribution(platform)
    return {"data": result["data"], "simulated": result.get("simulated", False)}


@app.get("/api/v1/topics/trend")
def topic_trend(
    platform: str | None = None,
    weeks: int = Query(8, ge=1, le=52),
) -> dict:
    """Weekly post count per topic (for line / animated timeline chart)."""
    if platform and platform not in {"reddit", "facebook", "instagram"}:
        raise HTTPException(status_code=400, detail="platform must be one of 'reddit', 'facebook', 'instagram'")
    result = topics.query_topic_trend(platform, weeks)
    return {"data": result["data"], "simulated": result.get("simulated", False)}


@app.get("/api/v1/topics/sentiment-heatmap")
def topic_sentiment_heatmap(platform: str | None = None) -> dict:
    """Average sentiment score per topic × platform (heatmap matrix)."""
    if platform and platform not in {"reddit", "facebook", "instagram"}:
        raise HTTPException(status_code=400, detail="platform must be one of 'reddit', 'facebook', 'instagram'")
    result = topics.query_sentiment_heatmap(platform)
    return {"data": result["data"], "simulated": result.get("simulated", False)}


@app.get("/api/v1/topics/network")
def topic_network(platform: str | None = None) -> dict:
    """Topic co-occurrence graph (nodes + edges) for network visualisation."""
    if platform and platform not in {"reddit", "facebook", "instagram"}:
        raise HTTPException(status_code=400, detail="platform must be one of 'reddit', 'facebook', 'instagram'")
    return topics.query_topic_network(platform)


# ── Network & Community Analysis endpoints ─────────────────────────────────

@app.get("/api/v1/network/graph")
def network_graph(platform: str | None = None) -> dict:
    """User interaction graph (nodes + edges) with community and PageRank."""
    if platform and platform not in {"reddit", "facebook", "instagram"}:
        raise HTTPException(status_code=400, detail="platform must be one of 'reddit', 'facebook', 'instagram'")
    return network_svc.query_graph(platform)


@app.get("/api/v1/network/communities")
def network_communities(platform: str | None = None) -> dict:
    """Community size distribution."""
    if platform and platform not in {"reddit", "facebook", "instagram"}:
        raise HTTPException(status_code=400, detail="platform must be one of 'reddit', 'facebook', 'instagram'")
    return {"data": network_svc.query_community_sizes(platform)}


@app.get("/api/v1/network/pagerank")
def network_pagerank(
    platform: str | None = None,
    top_n: int = Query(20, ge=1, le=100),
) -> dict:
    """Top-N influencers by PageRank score."""
    if platform and platform not in {"reddit", "facebook", "instagram"}:
        raise HTTPException(status_code=400, detail="platform must be one of 'reddit', 'facebook', 'instagram'")
    return {"data": network_svc.query_top_influencers(platform, top_n)}


# ── Virality Prediction endpoints ─────────────────────────────────────────────

import json as _json
import os as _os
import re as _re
import subprocess as _subprocess
import threading as _threading
from pathlib import Path as _Path
from pydantic import BaseModel, Field

_VIRALITY_ARTIFACTS_DIR = _os.getenv("VIRALITY_ARTIFACTS_DIR", "ml/virality/artifacts")
_VIRALITY_LOG_FILE      = _os.path.join(_VIRALITY_ARTIFACTS_DIR, "train.log")
_ENV_FILE               = _os.getenv("ENV_FILE", ".env")
_PROJECT_ROOT           = _Path(__file__).resolve().parents[1]
_TRAIN_DATA_ROOTS       = tuple(
    (_PROJECT_ROOT / part.strip()).resolve()
    for part in _os.getenv("TRAIN_DATA_ROOTS", "data").split(",")
    if part.strip()
)

# Global training state (in-process; resets on API restart)
_train_state: dict = {"running": False, "pid": None, "started_at": None}
_train_lock = _threading.Lock()

# Cached predictor instances — loaded once, reused across requests
_virality_predictor_cache = None
_virality_predictor_lock  = _threading.Lock()


def _get_virality_predictor():
    global _virality_predictor_cache
    if _virality_predictor_cache is None:
        with _virality_predictor_lock:
            if _virality_predictor_cache is None:
                from ml.virality.predictor import ViralityPredictor
                _virality_predictor_cache = ViralityPredictor(_VIRALITY_ARTIFACTS_DIR)
    return _virality_predictor_cache


def _reset_virality_predictor_cache():
    global _virality_predictor_cache
    with _virality_predictor_lock:
        _virality_predictor_cache = None


def _read_metadata() -> dict:
    path = _os.path.join(_VIRALITY_ARTIFACTS_DIR, "training_metadata.json")
    if not _os.path.exists(path):
        return {}
    with open(path, encoding="utf-8") as f:
        return _json.load(f)


def _read_retrain_history(n: int = 10) -> list:
    path = _os.path.join(_VIRALITY_ARTIFACTS_DIR, "retrain_history.jsonl")
    if not _os.path.exists(path):
        return []
    lines = _Path(path).read_text(encoding="utf-8").splitlines()
    return [_json.loads(line) for line in lines[-n:] if line.strip()]


def _resolve_train_data_dir(value: str) -> str:
    candidate = (_PROJECT_ROOT / value).resolve() if not _Path(value).is_absolute() else _Path(value).resolve()
    if not any(candidate == root or root in candidate.parents for root in _TRAIN_DATA_ROOTS):
        raise HTTPException(status_code=400, detail="data_dir must be inside configured TRAIN_DATA_ROOTS")
    if not candidate.exists() or not candidate.is_dir():
        raise HTTPException(status_code=400, detail="data_dir does not exist")
    return str(candidate)


def _validate_cron(cron: str) -> str:
    value = cron.strip()
    fields = value.split()
    if len(fields) != 5:
        raise HTTPException(status_code=400, detail="cron must have exactly 5 fields")
    if not _re.fullmatch(r"[0-9A-Za-z*/,\-\s]+", value):
        raise HTTPException(status_code=400, detail="cron contains unsupported characters")
    return value


def _write_env_value(key: str, value: str) -> bool:
    _os.environ[key] = value
    if not API_ALLOW_ENV_WRITES:
        return False
    env_path = _Path(_ENV_FILE)
    if env_path.exists():
        original = env_path.read_text(encoding="utf-8")
        pattern = _re.compile(rf"^{_re.escape(key)}=.*$", _re.MULTILINE)
        updated = (
            pattern.sub(f"{key}={value}", original)
            if pattern.search(original)
            else original.rstrip("\n") + f"\n{key}={value}\n"
        )
        env_path.write_text(updated, encoding="utf-8")
    else:
        env_path.write_text(f"{key}={value}\n", encoding="utf-8")
    return True


@app.get("/api/v1/virality/status")
def virality_status() -> dict:
    """Model status, training metadata, and retrain schedule."""
    meta = _read_metadata()
    history = _read_retrain_history(5)
    cron = _os.getenv("VIRALITY_RETRAIN_CRON", "0 2 * * 1")
    artifacts_exist = _os.path.exists(_os.path.join(_VIRALITY_ARTIFACTS_DIR, "lgbm_model.pkl"))
    return {
        "model_ready":         artifacts_exist,
        "training_running":    _train_state["running"],
        "training_pid":        _train_state.get("pid"),
        "training_started_at": _train_state.get("started_at"),
        "metadata":            meta,
        "retrain_history":     history,
        "retrain_cron":        cron,
    }


class ViralityPredictBody(BaseModel):
    content:        str
    url:            str = ""
    author_id:      str = "unknown"
    created_at:     int = 0
    created_at_iso: str | None = None


@app.post("/api/v1/virality/predict")
def virality_predict(body: ViralityPredictBody) -> dict:
    """Real-time virality prediction for a single Facebook post."""
    if not _os.path.exists(_os.path.join(_VIRALITY_ARTIFACTS_DIR, "lgbm_model.pkl")):
        raise HTTPException(
            status_code=503,
            detail="Model not ready. Run POST /api/v1/virality/train first."
        )

    import datetime as _dt
    created_at = body.created_at
    if not created_at and body.created_at_iso:
        try:
            created_at = int(
                _dt.datetime.fromisoformat(body.created_at_iso.replace("Z", "+00:00")).timestamp()
            )
        except Exception:
            created_at = 0
    if not created_at:
        created_at = int(_dt.datetime.now(_dt.timezone.utc).timestamp())

    try:
        predictor = _get_virality_predictor()
        result = predictor.predict({
            "content":    body.content,
            "url":        body.url,
            "author_id":  body.author_id,
            "created_at": created_at,
        })
        return {"ok": True, "result": result}
    except Exception as exc:
        logger.error("Virality predict error: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc))


class ViralityTrainBody(BaseModel):
    local:       bool = True
    data_dir:    str  = Field("data/facebook_data/raw_data", max_length=500)
    tune:        bool = False
    use_phobert: bool = False


@app.post("/api/v1/virality/train", dependencies=[Depends(require_admin_token)])
def virality_train(body: ViralityTrainBody) -> dict:
    """Kick off a training job in the background subprocess."""
    with _train_lock:
        if _train_state["running"]:
            return {"ok": False, "message": "Training already running", "pid": _train_state["pid"]}

        import datetime as _dt
        import sys as _sys

        _Path(_VIRALITY_ARTIFACTS_DIR).mkdir(parents=True, exist_ok=True)
        log_fh = open(_VIRALITY_LOG_FILE, "w", encoding="utf-8")

        cmd = [_sys.executable, "-m", "ml.virality.train",
               "--output-dir", _VIRALITY_ARTIFACTS_DIR]
        if body.local:          cmd += ["--local", "--data-dir", _resolve_train_data_dir(body.data_dir)]
        if body.tune:           cmd += ["--tune"]
        if not body.use_phobert: cmd += ["--no-phobert"]

        env = _os.environ.copy()
        env["KMP_DUPLICATE_LIB_OK"] = "TRUE"
        env["OMP_NUM_THREADS"] = "4"

        proc = _subprocess.Popen(cmd, stdout=log_fh, stderr=_subprocess.STDOUT, text=True, env=env)
        _train_state["running"]    = True
        _train_state["pid"]        = proc.pid
        _train_state["started_at"] = _dt.datetime.now(_dt.timezone.utc).isoformat()

        def _watch():
            exit_code = proc.wait()
            log_fh.close()
            try:
                with open(_VIRALITY_LOG_FILE, "a", encoding="utf-8") as f:
                    f.write(f"\n[API] Subprocess exited with code {exit_code}\n")
            except Exception:
                pass
            if exit_code == 0:
                _reset_virality_predictor_cache()
            with _train_lock:
                _train_state["running"] = False
                _train_state["pid"]     = None

        _threading.Thread(target=_watch, daemon=True).start()

    return {"ok": True, "message": "Training started", "pid": proc.pid}


@app.get("/api/v1/virality/train/log", dependencies=[Depends(require_admin_token)])
def virality_train_log(tail: int = Query(50, ge=1, le=500)) -> dict:
    """Return the last N lines of the training log."""
    if not _os.path.exists(_VIRALITY_LOG_FILE):
        return {"ok": True, "lines": [], "running": _train_state["running"]}
    try:
        text  = _Path(_VIRALITY_LOG_FILE).read_text(encoding="utf-8", errors="replace")
        raw_lines = text.split('\n')[-tail:]
        lines = [line.split('\r')[-1] for line in raw_lines]
    except Exception as exc:
        lines = [f"Error reading log: {exc}"]
    return {"ok": True, "lines": lines, "running": _train_state["running"]}


class RetrainScheduleBody(BaseModel):
    cron: str


@app.post("/api/v1/virality/retrain-schedule", dependencies=[Depends(require_admin_token)])
def set_retrain_schedule(body: RetrainScheduleBody) -> dict:
    """Update VIRALITY_RETRAIN_CRON in the .env file (takes effect after Airflow restart)."""
    cron = _validate_cron(body.cron)
    persisted = _write_env_value("VIRALITY_RETRAIN_CRON", cron)
    return {"ok": True, "cron": cron,
            "persisted": persisted,
            "message": "Schedule updated in process" + (". Restart Airflow scheduler to apply persisted value." if persisted else ".")}


# ── Sentiment Model endpoints ─────────────────────────────────────────────────

_SENTIMENT_ARTIFACTS_DIR = _os.getenv("SENTIMENT_ARTIFACTS_DIR", "ml/sentiment/artifacts")
_SENTIMENT_LOG_FILE      = _os.path.join(_SENTIMENT_ARTIFACTS_DIR, "train.log")

_sentiment_train_state: dict = {"running": False, "pid": None, "started_at": None}
_sentiment_train_lock = _threading.Lock()

# Cached sentiment predictor — loaded once on first predict call
_sentiment_predictor_cache = None
_sentiment_predictor_lock  = _threading.Lock()


def _get_sentiment_predictor():
    global _sentiment_predictor_cache
    if _sentiment_predictor_cache is None:
        with _sentiment_predictor_lock:
            if _sentiment_predictor_cache is None:
                from ml.sentiment.predictor import SentimentPredictor
                _sentiment_predictor_cache = SentimentPredictor(_SENTIMENT_ARTIFACTS_DIR)
    return _sentiment_predictor_cache


def _reset_sentiment_predictor_cache():
    global _sentiment_predictor_cache
    with _sentiment_predictor_lock:
        _sentiment_predictor_cache = None


def _read_sentiment_metadata() -> dict:
    path = _os.path.join(_SENTIMENT_ARTIFACTS_DIR, "training_metadata.json")
    if not _os.path.exists(path):
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            return _json.load(f)
    except Exception:
        return {}


def _read_sentiment_retrain_history(n: int = 10) -> list:
    path = _os.path.join(_SENTIMENT_ARTIFACTS_DIR, "retrain_history.jsonl")
    if not _os.path.exists(path):
        return []
    try:
        lines = _Path(path).read_text(encoding="utf-8").splitlines()
        return [_json.loads(line) for line in lines[-n:] if line.strip()]
    except Exception:
        return []


@app.get("/api/v1/sentiment/model-status")
def sentiment_model_status() -> dict:
    """Sentiment model status, training metadata, and retrain schedule."""
    meta = _read_sentiment_metadata()
    history = _read_sentiment_retrain_history(5)
    cron = _os.getenv("SENTIMENT_RETRAIN_CRON", "0 3 * * 1")
    artifacts_exist = _os.path.exists(_os.path.join(_SENTIMENT_ARTIFACTS_DIR, "fine_tuned_phobert", "config.json"))
    return {
        "model_ready":         artifacts_exist,
        "training_running":    _sentiment_train_state["running"],
        "training_pid":        _sentiment_train_state.get("pid"),
        "training_started_at": _sentiment_train_state.get("started_at"),
        "metadata":            meta,
        "retrain_history":     history,
        "retrain_cron":        cron,
    }


class SentimentPredictBody(BaseModel):
    content: str


@app.post("/api/v1/sentiment/predict")
def sentiment_predict(body: SentimentPredictBody) -> dict:
    """Real-time sentiment prediction using the custom fine-tuned model (or fallback)."""
    model_path = _os.path.join(_SENTIMENT_ARTIFACTS_DIR, "fine_tuned_phobert")
    if _os.path.exists(_os.path.join(model_path, "config.json")):
        try:
            predictor = _get_sentiment_predictor()
            result = predictor.predict(body.content)
            return {"ok": True, "result": result, "source": "fine-tuned-phobert"}
        except Exception as exc:
            logger.error("Fine-tuned sentiment predict error: %s", exc)

    # Fallback to speed layer analyze_sentiment
    try:
        from speed.nlp_pipeline import analyze_sentiment
        res = analyze_sentiment(body.content)
        return {
            "ok": True,
            "result": {
                "prediction": 2 if res["label"] == "positive" else 0 if res["label"] == "negative" else 1,
                "label": res["label"],
                "score": res["score"],
                "confidence": abs(res["score"]) if res["label"] != "neutral" else 1.0,
                "probabilities": {}
            },
            "source": "lexicon-fallback"
        }
    except Exception as exc:
        logger.error("Sentiment predict fallback error: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


class SentimentTrainBody(BaseModel):
    local:      bool = True
    data_dir:   str  = Field("data/facebook_data/raw_data", max_length=500)
    epochs:     int  = Field(3, ge=1, le=20)
    batch_size: int  = Field(8, ge=1, le=128)
    smoke_test: bool = False


@app.post("/api/v1/sentiment/train", dependencies=[Depends(require_admin_token)])
def sentiment_train(body: SentimentTrainBody) -> dict:
    """Kick off a PhoBERT sentiment training job in the background."""
    with _sentiment_train_lock:
        if _sentiment_train_state["running"]:
            return {"ok": False, "message": "Sentiment training already running", "pid": _sentiment_train_state["pid"]}

        import datetime as _dt
        import sys as _sys

        _Path(_SENTIMENT_ARTIFACTS_DIR).mkdir(parents=True, exist_ok=True)
        log_fh = open(_SENTIMENT_LOG_FILE, "w", encoding="utf-8")

        cmd = [_sys.executable, "-m", "ml.sentiment.train",
               "--output-dir", _SENTIMENT_ARTIFACTS_DIR,
               "--epochs", str(body.epochs),
               "--batch-size", str(body.batch_size)]
        if body.local:      cmd += ["--local", "--data-dir", _resolve_train_data_dir(body.data_dir)]
        if body.smoke_test: cmd += ["--smoke-test"]

        env = _os.environ.copy()
        env["KMP_DUPLICATE_LIB_OK"] = "TRUE"

        proc = _subprocess.Popen(cmd, stdout=log_fh, stderr=_subprocess.STDOUT, text=True, env=env)
        _sentiment_train_state["running"]    = True
        _sentiment_train_state["pid"]        = proc.pid
        _sentiment_train_state["started_at"] = _dt.datetime.now(_dt.timezone.utc).isoformat()

        def _watch():
            exit_code = proc.wait()
            log_fh.close()
            try:
                with open(_SENTIMENT_LOG_FILE, "a", encoding="utf-8") as f:
                    f.write(f"\n[API] Subprocess exited with code {exit_code}\n")
            except Exception:
                pass
            if exit_code == 0:
                _reset_sentiment_predictor_cache()
            with _sentiment_train_lock:
                _sentiment_train_state["running"] = False
                _sentiment_train_state["pid"]     = None

        _threading.Thread(target=_watch, daemon=True).start()

    return {"ok": True, "message": "Sentiment training started", "pid": proc.pid}


@app.get("/api/v1/sentiment/train/log", dependencies=[Depends(require_admin_token)])
def sentiment_train_log(tail: int = Query(50, ge=1, le=500)) -> dict:
    """Return the last N lines of the sentiment training log."""
    if not _os.path.exists(_SENTIMENT_LOG_FILE):
        return {"ok": True, "lines": [], "running": _sentiment_train_state["running"]}
    try:
        text  = _Path(_SENTIMENT_LOG_FILE).read_text(encoding="utf-8", errors="replace")
        raw_lines = text.split('\n')[-tail:]
        lines = [line.split('\r')[-1] for line in raw_lines]
    except Exception as exc:
        lines = [f"Error reading log: {exc}"]
    return {"ok": True, "lines": lines, "running": _sentiment_train_state["running"]}


class SentimentRetrainScheduleBody(BaseModel):
    cron: str


@app.post("/api/v1/sentiment/retrain-schedule", dependencies=[Depends(require_admin_token)])
def set_sentiment_retrain_schedule(body: SentimentRetrainScheduleBody) -> dict:
    """Update SENTIMENT_RETRAIN_CRON in the .env file."""
    cron = _validate_cron(body.cron)
    persisted = _write_env_value("SENTIMENT_RETRAIN_CRON", cron)
    return {"ok": True, "cron": cron, "persisted": persisted, "message": "Schedule updated in process."}
