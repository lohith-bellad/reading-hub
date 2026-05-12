import os
from contextlib import contextmanager

from psycopg_pool import ConnectionPool

DATABASE_URL = os.environ["DATABASE_URL"]

_pool: ConnectionPool | None = None


def get_pool() -> ConnectionPool:
    global _pool
    if _pool is None:
        _pool = ConnectionPool(DATABASE_URL, min_size=1, max_size=8, open=True)
    return _pool


@contextmanager
def connection():
    with get_pool().connection() as conn:
        yield conn
