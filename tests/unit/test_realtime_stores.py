import pytest
from unittest.mock import MagicMock
from speed.realtime_stores import RealtimeViewWriter


def test_write_clickhouse_realtime_views(monkeypatch):
    called = []

    def mock_post(self, url, **kwargs):
        called.append((url, kwargs.get("params"), kwargs.get("data"), kwargs.get("timeout")))
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        return mock_response

    monkeypatch.setattr("requests.Session.post", mock_post)

    writer = RealtimeViewWriter()
    writer.write([
        {
            "post_id": "p1",
            "platform": "reddit",
            "author_id": "author1",
            "content": "hello",
            "created_at": "2026-06-10 12:00:00",
            "hashtags": ["AI"],
            "enrichment": {"sentiment_score": 0.5},
        }
    ])

    assert len(called) == 1
    url, params, data, timeout = called[0]
    assert params["user"] == "social"
    assert params["password"] == "social"
    assert params["database"] == "social"
    assert b"INSERT INTO realtime_posts FORMAT JSONEachRow" in data
    assert b'"post_id": "p1"' in data
    assert b'"platform": "reddit"' in data
    assert b'"sentiment": 0.5' in data
