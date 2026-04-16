"""Load step — write enriched rows to a target store.

The template ships with a SQLite loader. Implement the same two methods
(open / upsert) to swap in MySQL, Postgres, BigQuery, or S3 Parquet.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Protocol

from .models import ProductEnriched


class Loader(Protocol):
    def open(self) -> None: ...
    def upsert(self, row: ProductEnriched) -> None: ...
    def close(self) -> None: ...


class SqliteLoader:
    DDL = """
    CREATE TABLE IF NOT EXISTS products (
        id         TEXT PRIMARY KEY,
        name       TEXT NOT NULL,
        brand      TEXT NOT NULL,
        category   TEXT NOT NULL,
        tags       TEXT NOT NULL,
        price_usd  REAL
    )
    """

    UPSERT = """
    INSERT INTO products (id, name, brand, category, tags, price_usd)
    VALUES (:id, :name, :brand, :category, :tags, :price_usd)
    ON CONFLICT(id) DO UPDATE SET
        name       = excluded.name,
        brand      = excluded.brand,
        category   = excluded.category,
        tags       = excluded.tags,
        price_usd  = excluded.price_usd
    """

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = str(db_path)
        self._conn: sqlite3.Connection | None = None

    def open(self) -> None:
        self._conn = sqlite3.connect(self.db_path)
        self._conn.execute(self.DDL)
        self._conn.commit()

    def upsert(self, row: ProductEnriched) -> None:
        assert self._conn is not None, "call open() first"
        self._conn.execute(
            self.UPSERT,
            {
                "id": row.id,
                "name": row.name,
                "brand": row.brand,
                "category": row.category,
                "tags": ",".join(row.tags),
                "price_usd": row.price_usd,
            },
        )
        self._conn.commit()

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None
