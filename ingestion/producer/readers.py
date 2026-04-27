"""
Readers — đọc từng loại file/folder, yield (topic, raw_dict).
"""

import json
import logging
import time
import random
from pathlib import Path
from typing import Generator, Iterator

from ingestion.config.settings import TOPIC_BATCH, TOPIC_REALTIME, PATHS

logger = logging.getLogger(__name__)


# ── Generic JSONL reader ──────────────────────────────────────────────────────

def _read_jsonl(path: Path, topic: str, delay: float = 0.0) -> Iterator[tuple[str, dict]]:
    if not path.exists():
        logger.warning("File not found: %s", path)
        return iter(())

    logger.info("Reading %s → topic=%s", path, topic)

    def _gen():
        try:
            f_handle = path.open(encoding="utf-8")
        except OSError as e:
            logger.warning("Cannot open file: %s (%s)", path, e)
            return

        with f_handle:
            for lineno, line in enumerate(f_handle, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    raw = json.loads(line)
                    if isinstance(raw, dict):
                        yield topic, raw
                        if delay > 0:
                            # Mô phỏng real-time: random delay từ 0.5 tới 1.5 lần mức base
                            time.sleep(delay * random.uniform(0.5, 1.5))
                    else:
                        logger.debug("Skip non-dict line %d in %s", lineno, path.name)
                except json.JSONDecodeError as e:
                    logger.warning("Bad JSON line %d in %s (%s)", lineno, path.name, e)

    return _gen()


# ── Facebook folder reader ────────────────────────────────────────────────────

def _read_fb_folder(folder: Path, topic: str, delay: float = 0.0) -> Iterator[tuple[str, dict]]:
    if not folder.exists():
        logger.warning("Folder not found: %s", folder)
        return iter(())

    def _gen():
        post_files = sorted(folder.rglob("post.json"))
        logger.info(
            "Reading %d FB post.json files from %s → topic=%s",
            len(post_files), folder, topic,
        )

        for pf in post_files:
            # race condition: file có thể bị xóa giữa lúc rglob và stat
            try:
                if pf.stat().st_size == 0:
                    logger.debug("Skip empty file: %s", pf)
                    continue
            except OSError:
                logger.debug("Skip inaccessible file: %s", pf)
                continue

            try:
                with pf.open(encoding="utf-8-sig") as f:
                    raw = json.load(f)
            except json.JSONDecodeError as e:
                logger.warning("Invalid JSON in file: %s (%s)", pf, e)
                continue
            except OSError as e:
                logger.warning("Cannot read file: %s (%s)", pf, e)
                continue

            if not isinstance(raw, dict):
                logger.debug("Skip non-dict file: %s", pf)
                continue

            # KHÔNG mutate raw
            yield topic, {**raw, "_source_file": str(pf)}
            
            if delay > 0:
                time.sleep(delay * random.uniform(0.5, 1.5))

    return _gen()


# ── Public API ────────────────────────────────────────────────────────────────

def reddit_records() -> Generator[tuple[str, dict], None, None]:
    cfg = PATHS["reddit"]
    yield from _read_jsonl(cfg["batch"],    TOPIC_BATCH)
    yield from _read_jsonl(cfg["realtime"], TOPIC_REALTIME, delay=0.5)


def instagram_records() -> Generator[tuple[str, dict], None, None]:
    cfg = PATHS["instagram"]
    yield from _read_jsonl(cfg["batch"],    TOPIC_BATCH)
    yield from _read_jsonl(cfg["realtime"], TOPIC_REALTIME, delay=0.5)


def facebook_records() -> Generator[tuple[str, dict], None, None]:
    cfg = PATHS["facebook"]
    yield from _read_fb_folder(cfg["batch"],    TOPIC_BATCH)
    yield from _read_fb_folder(cfg["realtime"], TOPIC_REALTIME, delay=0.5)