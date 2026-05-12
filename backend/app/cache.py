import os

import redis

REDIS_URL = os.environ["REDIS_URL"]

_client: redis.Redis | None = None


def get_client() -> redis.Redis:
    global _client
    if _client is None:
        _client = redis.from_url(REDIS_URL, decode_responses=False)
    return _client
