import json
import re

import pytest

import ingestion.simulator as simulator
from ingestion.simulator import (
    default_source_for_platform,
    iter_source_files,
    normalise,
    read_records,
    to_sop_schema,
    validate_sop_post,
)


def test_read_records_supports_single_json_object(tmp_path):
    source = tmp_path / "post.json"
    source.write_text(json.dumps({"post_id": "p1"}), encoding="utf-8")
    assert list(read_records(source)) == [{"post_id": "p1"}]


def test_read_records_falls_back_to_json_lines_when_file_starts_with_object(tmp_path):
    source = tmp_path / "posts.jsonl"
    source.write_text(
        "\n".join([json.dumps({"post_id": "p1"}), json.dumps({"post_id": "p2"})]),
        encoding="utf-8",
    )
    assert list(read_records(source)) == [{"post_id": "p1"}, {"post_id": "p2"}]


def test_iter_source_files_recursively_finds_supported_files(tmp_path):
    nested = tmp_path / "page" / "2026_05_21"
    nested.mkdir(parents=True)
    supported = nested / "post.json"
    ignored = nested / "notes.txt"
    supported.write_text(json.dumps({"post_id": "p1"}), encoding="utf-8")
    ignored.write_text("not a source record", encoding="utf-8")

    assert list(iter_source_files(tmp_path)) == [supported]


def test_default_source_for_platform_uses_raw_data_directory():
    assert default_source_for_platform("facebook").as_posix() == "data/facebook_data/raw_data"


def test_replay_deduplicates_posts_with_same_canonical_id(tmp_path, monkeypatch):
    source = tmp_path / "raw"
    source.mkdir()
    raw = {
        "post_id": "abc123",
        "title": "Launch notes",
        "selftext": "New release #Data",
        "author_fullname": "t2_author",
        "author": "alice",
        "subreddit": "datascience",
        "created_utc_raw": 1_770_000_000,
    }
    (source / "a.json").write_text(json.dumps(raw), encoding="utf-8")
    (source / "b.json").write_text(json.dumps(raw), encoding="utf-8")

    produced = []

    class FakeProducer:
        def __init__(self, _config):
            pass

        def produce(self, **kwargs):
            produced.append(kwargs)

        def poll(self, _timeout):
            pass

        def flush(self):
            pass

    monkeypatch.setattr(simulator, "Producer", FakeProducer)

    simulator.replay(source, "reddit", 10_000, False, "localhost:9092", dedupe=True)

    assert len(produced) == 1
    assert produced[0]["topic"] == "social.reddit.posts"


def test_normalise_emits_sop_canonical_schema_for_reddit():
    raw = {
        "post_id": "abc123",
        "title": "Launch notes",
        "selftext": "New release #Data",
        "author_fullname": "t2_author",
        "author": "alice",
        "subreddit": "datascience",
        "created_utc_raw": 1_770_000_000,
        "upvotes": 7,
        "comment_count": 2,
        "crossposts_count": 1,
        "url": "https://reddit.com/r/datascience/comments/abc123",
        "comments": [
            {
                "comment_id": "c1",
                "body": "Great update",
                "author": "bob",
                "author_fullname": "t2_bob",
                "created_utc_raw": 1_770_000_010,
                "score": 3,
                "replies": [],
            }
        ],
    }

    post = normalise("reddit", raw)

    assert post == {
        "post_id": "reddit_abc123",
        "platform": "reddit",
        "source_id": "datascience",
        "author_id": post["author_id"],
        "content": "Launch notes New release #Data",
        "title": "Launch notes",
        "media_urls": [],
        "hashtags": ["data"],
        "comments": post["comments"],
        "created_at": "2026-02-02T02:40:00Z",
        "ingested_at": post["ingested_at"],
        "metrics": {"likes": 7, "comments": 2, "shares": 1, "views": 0},
    }
    assert post["comments"][0]["comment_id"] == "c1"
    assert post["comments"][0]["text"] == "Great update"
    assert re.fullmatch(r"[0-9a-f]{64}", post["author_id"])
    assert post["author_id"] != raw["author_fullname"]
    assert re.fullmatch(r"[0-9a-f]{64}", post["comments"][0]["author_id"])
    assert post["comments"][0]["author_id"] != raw["comments"][0]["author_fullname"]


def test_to_sop_schema_requires_event_time():
    with pytest.raises(ValueError, match="event_time"):
        to_sop_schema(
            "instagram",
            {"id": "ig1", "ownerId": "owner"},
            {
                "post_id": "ig1",
                "event_time": None,
                "ingest_time": 1_700_000_000_000,
                "author_id": "owner",
                "content": "caption",
                "hashtags": [],
                "engagement": {},
            },
        )


def test_validate_sop_post_rejects_bad_metrics():
    with pytest.raises(ValueError, match="metrics"):
        validate_sop_post(
            {
                "post_id": "reddit_p1",
                "platform": "reddit",
                "source_id": "datascience",
                "author_id": "a" * 64,
                "content": "body",
                "title": None,
                "media_urls": [],
                "hashtags": [],
                "comments": [],
                "created_at": "2023-01-01T00:00:00Z",
                "ingested_at": "2023-01-01T00:00:01Z",
                "metrics": {"likes": 1},
            }
        )
