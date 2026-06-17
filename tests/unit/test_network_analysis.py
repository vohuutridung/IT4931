from batch.network_analysis import DirectedGraph, _add_comment_edges, _parse_comments_json


def test_add_comment_edges_links_comments_to_post_and_parent_author():
    graph = DirectedGraph()
    comments = [
        {"comment_id": "c1", "author_id": "commenter-1", "parent_id": None},
        {"comment_id": "c2", "author_id": "commenter-2", "parent_id": "c1"},
        {"comment_id": "c3", "author_id": "commenter-3", "parent_id": "t1_c2"},
    ]

    _add_comment_edges(graph, "post-author", comments)

    assert sorted(graph.edges()) == [
        ("commenter-1", "post-author", 1.0),
        ("commenter-2", "commenter-1", 1.0),
        ("commenter-3", "commenter-2", 1.0),
    ]


def test_parse_comments_json_ignores_malformed_payloads():
    assert _parse_comments_json('[{"comment_id": "c1"}, "bad"]') == [{"comment_id": "c1"}]
    assert _parse_comments_json("not json") == []
