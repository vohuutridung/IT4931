from batch.object_store_writer import flatten, parse_object_uri, partition_key


def test_parse_object_uri():
    assert parse_object_uri("s3a://social-lake/data/raw") == ("social-lake", "data/raw")


def test_flatten_and_partition_key():
    row = flatten(
        {
            "post_id": "p1",
            "platform": "reddit",
            "source_id": "datascience",
            "created_at": "2023-11-14T22:13:20Z",
            "ingested_at": "2023-11-14T22:13:21Z",
            "comments": [{"comment_id": "c1", "text": "hello"}],
            "metrics": {"likes": 1, "comments": 2, "shares": 3, "views": 4},
        }
    )
    assert row["likes"] == 1
    assert row["views"] == 4
    assert '"comment_id": "c1"' in row["comments_json"]
    assert partition_key(row)[0] == "reddit"
