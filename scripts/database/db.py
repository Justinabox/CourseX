import os
import ssl
import logging
from contextlib import contextmanager
from typing import Iterable, List, Optional, Sequence, Tuple

import pymysql
from dotenv import load_dotenv


load_dotenv()
logger = logging.getLogger(__name__)


def _build_ssl_kwargs() -> dict:
    host = os.getenv("TIDB_HOST", "")
    ssl_ca = os.getenv("TIDB_SSL_CA")
    if ssl_ca:
        # Use explicit CA bundle and require verification + hostname check
        ctx = ssl.create_default_context(cafile=ssl_ca)
        ctx.check_hostname = True
        ctx.verify_mode = ssl.CERT_REQUIRED
        return {"ssl": ctx}
    # If connecting to TiDB Cloud Serverless, TLS is mandatory even if no CA path is provided
    if "tidbcloud.com" in host:
        ctx = ssl.create_default_context()
        ctx.check_hostname = True
        ctx.verify_mode = ssl.CERT_REQUIRED
        return {"ssl": ctx}
    return {}


def get_connection(autocommit: bool = False) -> pymysql.connections.Connection:
    return pymysql.connect(
        host=os.getenv("TIDB_HOST", "127.0.0.1"),
        port=int(os.getenv("TIDB_PORT", "4000")),
        user=os.getenv("TIDB_USER", "root"),
        password=os.getenv("TIDB_PASSWORD", ""),
        database=os.getenv("TIDB_DATABASE", "coursex"),
        charset="utf8mb4",
        cursorclass=pymysql.cursors.Cursor,
        autocommit=autocommit,
        **_build_ssl_kwargs(),
    )


@contextmanager
def db_cursor(autocommit: bool = False):
    conn = get_connection(autocommit=autocommit)
    try:
        with conn.cursor() as cur:
            yield cur
        if not autocommit:
            conn.commit()
    except Exception:
        if not autocommit:
            conn.rollback()
        raise
    finally:
        conn.close()


def _chunks(rows: Sequence[Sequence], batch_size: int) -> Iterable[List[Sequence]]:
    for i in range(0, len(rows), batch_size):
        yield rows[i : i + batch_size]


def insert_many(
    cursor: pymysql.cursors.Cursor,
    table: str,
    columns: Sequence[str],
    rows: Sequence[Sequence],
    on_duplicate_update_cols: Optional[Sequence[str]] = None,
    batch_size: int = 1000,
) -> int:
    if not rows:
        return 0

    cols_sql = ", ".join(f"`{c}`" for c in columns)
    placeholders = ", ".join(["%s"] * len(columns))
    base_sql = f"INSERT INTO `{table}` ({cols_sql}) VALUES ({placeholders})"

    if on_duplicate_update_cols:
        update_clause = ", ".join(
            f"`{c}`=VALUES(`{c}`)" for c in on_duplicate_update_cols
        )
        base_sql = f"{base_sql} ON DUPLICATE KEY UPDATE {update_clause}"

    total = 0
    total_rows = len(rows)
    for chunk in _chunks(rows, batch_size):
        cursor.executemany(base_sql, chunk)
        total += len(chunk)
        logger.info("[%s] inserted %d/%d", table, total, total_rows)
    return total


