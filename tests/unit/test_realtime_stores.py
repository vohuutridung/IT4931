from speed.realtime_stores import RealtimeViewWriter


class DummyRedis:
    def __init__(self):
        self.commands = []

    def pipeline(self, transaction=False):
        return self

    def zadd(self, *args, **kwargs):
        self.commands.append(("zadd", args, kwargs))

    def zremrangebyrank(self, *args, **kwargs):
        self.commands.append(("zremrangebyrank", args, kwargs))

    def expire(self, *args, **kwargs):
        self.commands.append(("expire", args, kwargs))

    def hset(self, *args, **kwargs):
        self.commands.append(("hset", args, kwargs))

    def zincrby(self, *args, **kwargs):
        self.commands.append(("zincrby", args, kwargs))

    def zrem(self, *args, **kwargs):
        self.commands.append(("zrem", args, kwargs))

    def execute(self):
        self.commands.append(("execute", (), {}))


def test_write_redis_realtime_views():
    writer = object.__new__(RealtimeViewWriter)
    writer.redis = DummyRedis()
    writer._write_redis(
        [
            {
                "post_id": "p1",
                "platform": "reddit",
                "created_at": "2023-11-14T22:13:20Z",
                "hashtags": ["AI"],
                "enrichment": {"sentiment_score": 0.5},
            }
        ]
    )
    command_names = [cmd[0] for cmd in writer.redis.commands]
    assert "hset" in command_names
    assert "zadd" in command_names
