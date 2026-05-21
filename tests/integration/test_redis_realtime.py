import os

import pytest


@pytest.mark.skipif(os.getenv("RUN_INTEGRATION") != "1", reason="requires Redis service")
def test_redis_realtime_keyspace_reachable():
    import redis

    client = redis.Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6379")),
        decode_responses=True,
    )
    assert client.ping()
