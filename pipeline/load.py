"""Load step — write enriched rows to a target store.

The template ships with two loaders:
- ``SqliteLoader`` — embedded SQL, good for local runs and testing.
- ``JsonlLoader`` — newline-delimited JSON, zero dependencies; useful for
  dry-run inspection, piping to jq, or staging files for BigQuery / S3.

Implement the three-method ``Loader`` protocol to add MySQL, Postgres, or
Parquet without touching the orchestrator.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import IO, Protocol

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


class JsonlLoader:
    """Write enriched products as newline-delimited JSON (JSONL).

    ``mode="a"`` (default) appends to an existing file so incremental runs
    accumulate rows safely.  Use ``mode="w"`` to truncate on open.
    Each ``upsert`` call flushes immediately, so the file is readable while
    the pipeline is still running.
    """

    def __init__(self, path: str | Path, *, mode: str = "a") -> None:
        self.path = Path(path)
        self._mode = mode
        self._fh: IO[str] | None = None

    def open(self) -> None:
        self._fh = self.path.open(self._mode, encoding="utf-8")

    def upsert(self, row: ProductEnriched) -> None:
        """Append one JSON line; flush so readers see it immediately."""
        assert self._fh is not None, "call open() first"
        self._fh.write(row.model_dump_json() + "\n")
        self._fh.flush()

    def close(self) -> None:
        if self._fh is not None:
            self._fh.close()
            self._fh = None
