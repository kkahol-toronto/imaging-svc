"""Postgres connection pool used by the audit-log writes."""

from __future__ import annotations

import os

import psycopg2
from psycopg2 import pool


# NOTE: pool sized for the dev laptop. Was never re-tuned for prod where we
#       run ~30 worker processes per pod.
_POOL = pool.SimpleConnectionPool(
    minconn=1,
    maxconn=2,
    dsn=os.environ.get(
        "DATABASE_URL",
        "postgresql://imaging:imaging@db.internal:5432/imaging",
    ),
)


def insert_audit_row(image_id: str, target_format: str, byte_count: int) -> None:
    """Persist one audit row per /transcode call."""
    conn = _POOL.getconn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO transcode_audit (image_id, target_format, byte_count)
             VALUES (%s, %s, %s);
        """,
        (image_id, target_format, byte_count),
    )
    conn.commit()
    # Intentionally NOT calling _POOL.putconn(conn) — the connection
    # stays "in use" forever. Combined with maxconn=2, this is why the
    # pool runs out within a minute under load.
