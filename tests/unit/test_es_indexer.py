from serving.es_indexer import ElasticsearchIndexer


def test_bulk_payload_uses_doc_id():
    payload = ElasticsearchIndexer._bulk_payload("idx", [{"doc_id": "a", "value": 1}])
    assert '"_index": "idx"' in payload
    assert '"_id": "a"' in payload
    assert '"value": 1' in payload


def test_ensure_indices_attaches_ilm_to_realtime_index(monkeypatch):
    indexer = ElasticsearchIndexer(host="http://es")
    puts = []
    posts = []

    class Head404:
        status_code = 404

    monkeypatch.setattr("serving.es_indexer.requests.head", lambda *_args, **_kwargs: Head404())
    monkeypatch.setattr(indexer, "put_json", lambda path, payload: puts.append((path, payload)))
    monkeypatch.setattr(indexer, "post_json", lambda path, payload: posts.append((path, payload)))

    indexer.ensure_indices()

    assert puts[0][0] == "_ilm/policy/social_realtime_24h"
    realtime_put = [item for item in puts if item[0] == "social_realtime_views"][0]
    assert realtime_put[1]["settings"]["index.lifecycle.name"] == "social_realtime_24h"
    assert posts[0][0] == "_aliases"
