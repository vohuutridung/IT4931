from serving.network_service import NetworkService
from serving.topic_service import TopicService


def test_topic_service_returns_empty_when_demo_fallback_disabled(monkeypatch):
    monkeypatch.setattr("serving.topic_service.ENABLE_DEMO_FALLBACK", False)
    service = TopicService()
    monkeypatch.setattr(service, "_search", lambda *_args, **_kwargs: [])

    assert service.query_topic_distribution() == {"data": [], "simulated": False}
    assert service.query_topic_network() == {"nodes": [], "edges": [], "simulated": False}


def test_network_service_returns_empty_when_demo_fallback_disabled(monkeypatch):
    monkeypatch.setattr("serving.network_service.ENABLE_DEMO_FALLBACK", False)
    service = NetworkService()
    service._redis = None
    monkeypatch.setattr(service, "_search", lambda *_args, **_kwargs: [])

    assert service.query_graph() == {"nodes": [], "edges": [], "simulated": False}
    assert service.query_community_sizes() == []
    assert service.query_top_influencers() == []
