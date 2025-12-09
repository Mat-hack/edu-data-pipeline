"""Lightweight DB helper for Airflow tasks using psycopg2."""
import os
from datetime import date
from typing import Iterable, List, Mapping, Sequence, Optional
from urllib.parse import urlparse

import psycopg2
from psycopg2.extras import DictCursor, execute_values


def _pg_conn_str() -> str:
    raw = os.environ.get("AIRFLOW__CORE__SQL_ALCHEMY_CONN") or os.environ.get("DATABASE_URL")
    if not raw:
        raise RuntimeError("Database connection string not set")
    # strip sqlalchemy driver if present
    if raw.startswith("postgresql+psycopg2"):
        raw = raw.replace("postgresql+psycopg2", "postgresql", 1)
    return raw


def get_conn():
    return psycopg2.connect(_pg_conn_str())


def fetch_all(query: str, params: Optional[tuple] = None) -> List[Mapping]:
    with get_conn() as conn:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(query, params or ())
            return [dict(row) for row in cur.fetchall()]


def insert_rows(table: str, rows: Iterable[Mapping], columns: Sequence[str]) -> int:
    rows = list(rows)
    if not rows:
        return 0
    values = [[row.get(col) for col in columns] for row in rows]
    placeholders = ",".join(columns)
    stmt = f"INSERT INTO {table} ({placeholders}) VALUES %s"
    with get_conn() as conn:
        with conn.cursor() as cur:
            execute_values(cur, stmt, values)
    return len(rows)


def upsert_rows(table: str, rows: Iterable[Mapping], columns: Sequence[str], conflict_cols: Sequence[str]) -> int:
    rows = list(rows)
    if not rows:
        return 0
    # Deduplicate rows on conflict keys to avoid cardinality violations when the same key
    # appears multiple times in a single execute_values batch.
    key_idx = [columns.index(c) for c in conflict_cols]
    deduped = {}
    for row in rows:
        vals = [row.get(col) for col in columns]
        key = tuple(vals[i] for i in key_idx)
        deduped[key] = vals  # last write wins within batch
    values = list(deduped.values())
    placeholders = ",".join(columns)
    conflict = ",".join(conflict_cols)
    updates = ",".join([f"{c}=EXCLUDED.{c}" for c in columns])
    stmt = f"INSERT INTO {table} ({placeholders}) VALUES %s ON CONFLICT ({conflict}) DO UPDATE SET {updates}"
    with get_conn() as conn:
        with conn.cursor() as cur:
            execute_values(cur, stmt, values)
    return len(rows)


def _date_key(d: date) -> int:
    return d.year * 10000 + d.month * 100 + d.day


def ensure_dim_dates(dates: Iterable[Optional[date]]) -> None:
    rows = []
    for d in dates:
        if not d:
            continue
        dk = _date_key(d)
        rows.append(
            {
                "date_key": dk,
                "full_date": d,
                "day_of_week": d.isoweekday(),
                "day_name": d.strftime("%A")[:10],
                "day_of_month": d.day,
                "week_of_year": int(d.strftime("%U")),
                "month_number": d.month,
                "month_name": d.strftime("%B")[:10],
                "quarter": (d.month - 1) // 3 + 1,
                "year": d.year,
                "is_weekend": d.isoweekday() in (6, 7),
            }
        )
    if not rows:
        return
    upsert_rows(
        "warehouse.dim_date",
        rows,
        [
            "date_key",
            "full_date",
            "day_of_week",
            "day_name",
            "day_of_month",
            "week_of_year",
            "month_number",
            "month_name",
            "quarter",
            "year",
            "is_weekend",
        ],
        ["date_key"],
    )
