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

    def scan_iter(self, pattern):
        prefix = pattern.removesuffix("*")
        return iter([key for key in self.zsets if key.startswith(prefix)])

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

    assert service.query_top_hashtags("reddit", 6, 10) == [{"hashtag": "ai", "frequency": 3.0, "week": "realtime"}]


def test_query_hashtag_weeks(monkeypatch):
    service = ServeQuery()
    called_payload = []
    
    def fake_post(url, json, timeout):
        class FakeResponse:
            def raise_for_status(self):
                pass
            def json(self):
                return {
                    "aggregations": {
                        "weeks": {
                            "buckets": [
                                {"key_as_string": "2026-05-18T00:00:00Z"},
                                {"key_as_string": "2026-05-11T00:00:00Z"}
                            ]
                        }
                    }
                }
        called_payload.append(json)
        return FakeResponse()

    monkeypatch.setattr("requests.post", fake_post)
    
    weeks = service.query_hashtag_weeks("reddit")
    assert weeks == ["2026-05-18T00:00:00Z", "2026-05-11T00:00:00Z"]
    assert called_payload[0]["query"]["bool"]["filter"] == [
        {"term": {"view": "top_hashtags_weekly"}},
        {"term": {"platform": "reddit"}}
    ]


def test_query_top_hashtags_with_week_filters_correctly(monkeypatch):
    service = ServeQuery()
    service._redis = None
    
    searches = []
    def fake_search(index, query, size=100, sort=None):
        searches.append((index, query, size, sort))
        return [
            {"hashtag": "test", "frequency": 5, "event_week": "2026-05-18T00:00:00Z"},
            {"hashtag": "another", "frequency": 2, "event_week": "2026-05-18T00:00:00Z"}
        ]
        
    monkeypatch.setattr(service, "_search", fake_search)
    
    res = service.query_top_hashtags("reddit", 24, 10, week="2026-05-18T00:00:00Z")
    assert len(res) == 2
    assert res[0] == {"hashtag": "test", "frequency": 5, "week": "2026-05-18T00:00:00Z"}
    assert res[1] == {"hashtag": "another", "frequency": 2, "week": "2026-05-18T00:00:00Z"}
    
    _, query, _, _ = searches[0]
    filters = query["bool"]["filter"]
    assert {"term": {"view": "top_hashtags_weekly"}} in filters
    assert {"term": {"platform": "reddit"}} in filters
    assert {"term": {"event_week": "2026-05-18T00:00:00Z"}} in filters


def test_query_top_hashtags_fallback_latest_week(monkeypatch):
    service = ServeQuery()
    service._redis = None
    
    def fake_weeks(platform):
        return ["2026-05-18T00:00:00Z", "2026-05-11T00:00:00Z"]
    monkeypatch.setattr(service, "query_hashtag_weeks", fake_weeks)
    
    searches = []
    def fake_search(index, query, size=100, sort=None):
        searches.append((index, query, size, sort))
        return [
            {"hashtag": "latest", "frequency": 10, "event_week": "2026-05-18T00:00:00Z"}
        ]
    monkeypatch.setattr(service, "_search", fake_search)
    
    res = service.query_top_hashtags("reddit", 24, 10, week=None)
    assert len(res) == 1
    assert res[0]["hashtag"] == "latest"
    assert res[0]["week"] == "2026-05-18T00:00:00Z"
    
    _, query, _, _ = searches[0]
    filters = query["bool"]["filter"]
    assert {"term": {"event_week": "2026-05-18T00:00:00Z"}} in filters
