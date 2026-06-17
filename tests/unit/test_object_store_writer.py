from unittest.mock import MagicMock, patch
from batch.object_store_writer import flatten, parse_object_uri, partition_key, flush, publish_dlq
from config.settings import STORAGE_RAW_BASE, STORAGE_DISCARDED_BASE


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


@patch("batch.object_store_writer.write_rows")
def test_flush_routing(mock_write_rows, monkeypatch):
    monkeypatch.setattr("batch.object_store_writer.EVENT_TIME_MIN", "2026-01-01")
    monkeypatch.setattr("batch.object_store_writer.EVENT_TIME_MAX", "2026-04-30")
    client = MagicMock()
    
    # 1. Dữ liệu hợp lệ (trong khoảng Jan - Apr 2026)
    row_valid = flatten({
        "post_id": "p1",
        "platform": "reddit",
        "created_at": "2026-03-15T10:00:00Z",
        "ingested_at": "2026-03-15T10:01:00Z",
    })
    
    # 2. Dữ liệu không hợp lệ (ngoài khoảng: ví dụ 2026-05)
    row_invalid_may = flatten({
        "post_id": "p2",
        "platform": "reddit",
        "created_at": "2026-05-01T00:00:00Z",
        "ingested_at": "2026-05-01T00:01:00Z",
    })

    # 3. Dữ liệu không hợp lệ (năm 2025)
    row_invalid_2025 = flatten({
        "post_id": "p3",
        "platform": "reddit",
        "created_at": "2025-12-31T23:59:59Z",
        "ingested_at": "2025-12-31T23:59:59Z",
    })

    buffers = {
        partition_key(row_valid): [row_valid],
        partition_key(row_invalid_may): [row_invalid_may],
        partition_key(row_invalid_2025): [row_invalid_2025],
    }

    # Chạy flush
    flush(client, buffers)
    
    # Kiểm tra xem mock_write_rows được gọi đúng 3 lần với các storage_base tương ứng
    assert mock_write_rows.call_count == 3
    
    # Duyệt qua các lệnh gọi để kiểm tra
    calls = mock_write_rows.call_args_list
    for call in calls:
        args, kwargs = call
        # args[1] là rows, args[2] là key, args[3] là storage_base
        key = args[2]
        storage_base = args[3]
        
        platform, year, month, day = key
        if year == 2026 and 1 <= month <= 4:
            assert storage_base == STORAGE_RAW_BASE
        else:
            assert storage_base == STORAGE_DISCARDED_BASE


def test_flush_routes_all_to_raw_when_no_time_bounds(monkeypatch):
    monkeypatch.setattr("batch.object_store_writer.EVENT_TIME_MIN", "")
    monkeypatch.setattr("batch.object_store_writer.EVENT_TIME_MAX", "")
    row = flatten({
        "post_id": "p1",
        "platform": "reddit",
        "created_at": "2026-05-01T00:00:00Z",
        "ingested_at": "2026-05-01T00:01:00Z",
    })
    buffers = {partition_key(row): [row]}
    with patch("batch.object_store_writer.write_rows") as mock_write_rows:
        flush(MagicMock(), buffers)
    assert mock_write_rows.call_args.args[3] == STORAGE_RAW_BASE


def test_publish_dlq_includes_source_metadata():
    producer = MagicMock()
    msg = MagicMock()
    msg.topic.return_value = "social.reddit.posts"
    msg.partition.return_value = 2
    msg.offset.return_value = 10
    msg.value.return_value = b"{bad json"

    assert publish_dlq(producer, msg, ValueError("bad payload")) is True
    topic, payload = producer.produce.call_args.args
    assert topic == "social.dlq"
    assert b"bad payload" in payload
    producer.flush.assert_called_once_with(5)
