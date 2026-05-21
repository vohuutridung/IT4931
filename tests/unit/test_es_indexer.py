from serving.es_indexer import ElasticsearchIndexer


def test_bulk_payload_uses_doc_id():
    payload = ElasticsearchIndexer._bulk_payload("idx", [{"doc_id": "a", "value": 1}])
    assert '"_index": "idx"' in payload
    assert '"_id": "a"' in payload
    assert '"value": 1' in payload
