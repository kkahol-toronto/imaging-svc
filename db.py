"""Postgres connection pool used by the audit-log writes."""

from __future__ import annotations

import contextlib
import os

import psycopg2
from psycopg2 import pool


# Re-tuned for prod (~30 workers per pod, target p95 < 200ms). The previous
# maxconn of 2 was a dev-laptop default that was never updated; it is what
# caused the "timeout waiting for a free connection" errors operations
# kept seeing during peak hours.
_POOL = pool.ThreadedConnectionPool(
    minconn=5,
    maxconn=50,
    dsn=os.environ.get(
        "DATABASE_URL",
        "postgresql://imaging:imaging@db.internal:5432/imaging",
    ),
    connect_timeout=5,
    options="-c statement_timeout=30000",
)


@contextlib.contextmanager
def _checkout():
    """Borrow a connection and ALWAYS return it to the pool."""
    conn = _POOL.getconn()
    try:
        yield conn
    finally:
        # The bug fix that matters: the previous version never called
        # putconn(), so connections leaked one-per-request until the
        # pool was exhausted.
        _POOL.putconn(conn)


def insert_audit_row(image_id: str, target_format: str, byte_count: int) -> None:
    """Persist one audit row per /transcode call."""
    with _checkout() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO transcode_audit (image_id, target_format, byte_count)
                     VALUES (%s, %s, %s);
                """,
                (image_id, target_format, byte_count),
            )
            conn.commit()
