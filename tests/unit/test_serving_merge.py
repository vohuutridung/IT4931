import pytest
from unittest.mock import MagicMock
from serving.merge_service import ServeQuery


def test_query_posts_returns_merged_posts(monkeypatch):
    service = ServeQuery()
    called_queries = []

    def mock_post(url, params, data, timeout):
        query = data.decode("utf-8")
        called_queries.append(query)
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        
        if "fact_top_posts" in query:
            # Batch posts
            mock_response.json = lambda: {
                "data": [
                    {
                        "post_id": "p1",
                        "platform": "reddit",
                        "author_id": "a1",
                        "content": "batch content",
                        "sentiment_score": 0.5,
                        "event_ts": "2026-06-10 12:00:00",
                        "loaded_at": "2026-06-10 13:00:00"
                    }
                ]
            }
        else:
            # Realtime posts
            mock_response.json = lambda: {
                "data": [
                    {
                        "post_id": "p1",
                        "platform": "reddit",
                        "author_id": "a1",
                        "content": "speed content",
                        "sentiment_score": -0.1,
                        "event_ts": "2026-06-10 12:00:00",
                        "loaded_at": "2026-06-10 12:01:00"
                    },
                    {
                        "post_id": "p2",
                        "platform": "reddit",
                        "author_id": "a2",
                        "content": "speed content 2",
                        "sentiment_score": 0.8,
                        "event_ts": "2026-06-10 12:05:00",
                        "loaded_at": "2026-06-10 12:06:00"
                    }
                ]
            }
        return mock_response

    monkeypatch.setattr("requests.post", mock_post)

    res = service.query_posts("reddit", "2026-06-10T00:00:00Z", "2026-06-10T23:59:59Z", 10)
    assert len(res) == 2
    # p2 is newest, should be first
    assert res[0]["post_id"] == "p2"
    assert res[0]["sentiment_score"] == 0.8
    # p1 is batch content because batch overrides speed
    assert res[1]["post_id"] == "p1"
    assert res[1]["content"] == "batch content"
    assert res[1]["sentiment_score"] == 0.5


def test_query_sentiment_trend_merges_data(monkeypatch):
    service = ServeQuery()

    def mock_post(url, params, data, timeout):
        query = data.decode("utf-8")
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        
        if "fact_sentiment_time_series" in query:
            # Batch data
            mock_response.json = lambda: {
                "data": [
                    {
                        "platform": "reddit",
                        "event_hour": "2026-06-10 12:00:00",
                        "avg_sentiment": 0.2,
                        "post_count": 5
                    }
                ]
            }
        else:
            # Speed data
            mock_response.json = lambda: {
                "data": [
                    {
                        "platform": "reddit",
                        "event_hour": "2026-06-10 12:00:00",
                        "avg_sentiment": 0.1,
                        "post_count": 2
                    },
                    {
                        "platform": "reddit",
                        "event_hour": "2026-06-10 13:00:00",
                        "avg_sentiment": 0.6,
                        "post_count": 3
                    }
                ]
            }
        return mock_response

    monkeypatch.setattr("requests.post", mock_post)

    res = service.query_sentiment_trend("reddit", "hour", "2026-06-10T00:00:00Z", "2026-06-10T23:59:59Z")
    assert len(res) == 2
    # event_hour: 12:00:00, value should be batch value (0.2)
    assert res[0]["event_hour"] == "2026-06-10 12:00:00"
    assert res[0]["avg_sentiment"] == 0.2
    assert res[0]["post_count"] == 5
    assert res[0]["velocity"] == 0.0

    # event_hour: 13:00:00, value should be speed value (0.6)
    assert res[1]["event_hour"] == "2026-06-10 13:00:00"
    assert res[1]["avg_sentiment"] == 0.6
    assert res[1]["post_count"] == 3
    # velocity = 0.6 - 0.2 = 0.4
    assert res[1]["velocity"] == 0.4


def test_query_hashtag_weeks(monkeypatch):
    service = ServeQuery()

    def mock_post(url, params, data, timeout):
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json = lambda: {
            "data": [
                {"event_week": "2026-06-08 00:00:00"},
                {"event_week": "2026-06-01 00:00:00"}
            ]
        }
        return mock_response

    monkeypatch.setattr("requests.post", mock_post)

    res = service.query_hashtag_weeks("reddit")
    assert res == ["2026-06-08 00:00:00", "2026-06-01 00:00:00"]


def test_query_top_hashtags(monkeypatch):
    service = ServeQuery()

    def mock_post(url, params, data, timeout):
        query = data.decode("utf-8")
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        
        if "max(event_week)" in query:
            mock_response.json = lambda: {
                "data": [{"latest_week": "2026-06-08 00:00:00"}]
            }
        else:
            mock_response.json = lambda: {
                "data": [
                    {"hashtag": "python", "frequency": 10},
                    {"hashtag": "spark", "frequency": 5}
                ]
            }
        return mock_response

    monkeypatch.setattr("requests.post", mock_post)

    res = service.query_top_hashtags("reddit", 24, 5)
    assert len(res) == 2
    assert res[0] == {"hashtag": "python", "frequency": 10, "week": "2026-06-08 00:00:00"}
    assert res[1] == {"hashtag": "spark", "frequency": 5, "week": "2026-06-08 00:00:00"}


def test_query_realtime_stats(monkeypatch):
    service = ServeQuery()

    def mock_post(url, params, data, timeout):
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json = lambda: {
            "data": [
                {"platform": "reddit", "post_count": 12, "avg_sentiment": 0.3541}
            ]
        }
        return mock_response

    monkeypatch.setattr("requests.post", mock_post)

    res = service.query_realtime_stats("reddit")
    assert res["platform"] == "reddit"
    assert len(res["stats"]) == 1
    assert res["stats"][0] == {"platform": "reddit", "post_count": 12, "avg_sentiment": 0.3541}
