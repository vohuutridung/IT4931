from serving.merge_service import ServeQuery


def test_dedupe_preserves_first_seen():
    posts = [{"post_id": "1", "v": "rt"}, {"post_id": "1", "v": "batch"}, {"post_id": "2"}]
    assert ServeQuery._dedupe(posts, 10) == [{"post_id": "1", "v": "rt"}, {"post_id": "2"}]


def test_query_posts_sorts_by_event_time_desc(monkeypatch):
    service = ServeQuery()
    calls = []

    def fake_search(index, query, size=100, sort=None):
        calls.append((index, query, size, sort))
        return []

    monkeypatch.setattr(service, "_search", fake_search)

    service.query_posts(
        None,
        "2023-01-01T00:00:00+00:00",
        "2026-05-22T00:00:00+00:00",
        25,
    )

    assert calls
    assert all(call[3] == [{"event_ts": {"order": "desc", "missing": "_last"}}] for call in calls)


class DummyRedis:
    def __init__(self):
        self.zsets = {
            "rt:hashtags:reddit:2026-05-22T03:00:00+00:00": [("ai", 3.0)],
            "rt:hashtags:reddit:2026-05-20T03:00:00+00:00": [("old", 10.0)],
            "rt:hashtags:facebook:2026-05-22T03:00:00+00:00": [("fb", 5.0)],
        }

    def keys(self, pattern):
        prefix = pattern.removesuffix("*")
        return [key for key in self.zsets if key.startswith(prefix)]

    def zrevrange(self, key, start, end, withscores=False):
        values = self.zsets[key][start:end + 1]
        return values if withscores else [tag for tag, _score in values]


def test_top_hashtags_filters_platform_and_window(monkeypatch):
    service = ServeQuery()
    service._redis = DummyRedis()

    class FixedDatetime:
        @classmethod
        def now(cls, tz=None):
            from datetime import datetime

            return datetime(2026, 5, 22, 4, 0, tzinfo=tz)

        @classmethod
        def fromisoformat(cls, value):
            from datetime import datetime

            return datetime.fromisoformat(value)

    monkeypatch.setattr("serving.merge_service.datetime", FixedDatetime)

    assert service.query_top_hashtags("reddit", 6, 10) == [{"hashtag": "ai", "frequency": 3.0}]
