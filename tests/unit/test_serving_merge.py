from serving.merge_service import ServeQuery


def test_dedupe_preserves_first_seen():
    posts = [{"post_id": "1", "v": "rt"}, {"post_id": "1", "v": "batch"}, {"post_id": "2"}]
    assert ServeQuery._dedupe(posts, 10) == [{"post_id": "1", "v": "rt"}, {"post_id": "2"}]
