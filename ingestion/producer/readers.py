"""
Readers — đọc từng loại file/folder, yield (topic, raw_dict).

Vì data đã chia sẵn theo tên file/folder:
  *_before_2026_04_10  →  TOPIC_BATCH
  *_after_2026_04_10   →  TOPIC_REALTIME
  data_before_*        →  TOPIC_BATCH
  data_after_*         →  TOPIC_REALTIME

Reader chịu trách nhiệm gắn topic — normalizer và producer không cần biết.
"""

import json
import logging
from pathlib import Path
from typing import Generator

from ingestion.config.settings import TOPIC_BATCH, TOPIC_REALTIME, PATHS

logger = logging.getLogger(__name__)


# ── Generic JSONL reader ──────────────────────────────────────────────────────

def _read_jsonl(path: Path, topic: str) -> Generator[tuple, None, None]:
    if not path.exists():
        logger.warning("File not found: %s", path)
        return
    logger.info("Reading %s → topic=%s", path, topic)
    with path.open(encoding="utf-8") as f:
        for lineno, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                yield topic, json.loads(line)
            except json.JSONDecodeError as e:
                logger.warning("Bad JSON line %d in %s: %s", lineno, path.name, e)


# ── Facebook folder reader ────────────────────────────────────────────────────
# Cấu trúc: data_before_2026_04_10/{page}/{timestamp}/post.json

def _read_fb_folder(folder: Path, topic: str) -> Generator[tuple, None, None]:
    if not folder.exists():
        logger.warning("Folder not found: %s", folder)
        return
    post_files = sorted(folder.rglob("post.json"))
    logger.info("Reading %d FB post.json files from %s → topic=%s",
                len(post_files), folder, topic)
    for pf in post_files:
        try:
            # Use utf-8-sig to handle BOM (Byte Order Mark)
            raw = json.loads(pf.read_text(encoding="utf-8-sig"))
            # Gắn thêm source_folder để debug
            raw["_source_file"] = str(pf)
            yield topic, raw
        except Exception as e:
            logger.warning("Skip %s: %s", pf, e)


# ── Public API: 1 generator per source ───────────────────────────────────────

def reddit_records() -> Generator[tuple, None, None]:
    cfg = PATHS["reddit"]
    yield from _read_jsonl(cfg["batch"],    TOPIC_BATCH)
    yield from _read_jsonl(cfg["realtime"], TOPIC_REALTIME)


def instagram_records() -> Generator[tuple, None, None]:
    cfg = PATHS["instagram"]
    yield from _read_jsonl(cfg["batch"],    TOPIC_BATCH)
    yield from _read_jsonl(cfg["realtime"], TOPIC_REALTIME)


def facebook_records() -> Generator[tuple, None, None]:
    cfg = PATHS["facebook"]
    yield from _read_fb_folder(cfg["batch"],    TOPIC_BATCH)
    yield from _read_fb_folder(cfg["realtime"], TOPIC_REALTIME)
