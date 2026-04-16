"""Tests for SqliteLoader and JsonlLoader."""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from pipeline.load import JsonlLoader, SqliteLoader
from pipeline.models import ProductEnriched


def _row(i: int = 1) -> ProductEnriched:
    return ProductEnriched(
        id=f"p{i}",
        name=f"Product {i}",
        brand="Acme",
        category="electronics",
        tags=["wireless", "compact"],
        price_usd=9.99,
    )


# ── SqliteLoader ──────────────────────────────────────────────────────────────


def test_sqlite_upsert_roundtrip(tmp_path: Path) -> None:
    loader = SqliteLoader(tmp_path / "test.db")
    loader.open()
    loader.upsert(_row(1))
    loader.close()

    conn = sqlite3.connect(str(tmp_path / "test.db"))
    result = conn.execute("SELECT id, brand FROM products WHERE id='p1'").fetchone()
    conn.close()
    assert result == ("p1", "Acme")


def test_sqlite_upsert_is_idempotent(tmp_path: Path) -> None:
    """ON CONFLICT UPDATE — second upsert of the same id updates in place."""
    loader = SqliteLoader(tmp_path / "test.db")
    loader.open()
    loader.upsert(_row(1))
    loader.upsert(_row(1).model_copy(update={"brand": "NewBrand"}))
    loader.close()

    conn = sqlite3.connect(str(tmp_path / "test.db"))
    count, brand = conn.execute("SELECT COUNT(*), brand FROM products").fetchone()
    conn.close()
    assert count == 1
    assert brand == "NewBrand"


# ── JsonlLoader ───────────────────────────────────────────────────────────────


def test_jsonl_writes_valid_json(tmp_path: Path) -> None:
    path = tmp_path / "out.jsonl"
    loader = JsonlLoader(path)
    loader.open()
    loader.upsert(_row(1))
    loader.upsert(_row(2))
    loader.close()

    lines = path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 2
    obj = json.loads(lines[0])
    assert obj["id"] == "p1"
    assert obj["brand"] == "Acme"
    assert obj["category"] == "electronics"


def test_jsonl_append_across_sessions(tmp_path: Path) -> None:
    """Default mode='a': two separate open/close cycles accumulate rows."""
    path = tmp_path / "out.jsonl"
    for i in (1, 2):
        loader = JsonlLoader(path)
        loader.open()
        loader.upsert(_row(i))
        loader.close()

    lines = path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 2
    ids = {json.loads(ln)["id"] for ln in lines}
    assert ids == {"p1", "p2"}


def test_jsonl_overwrite_mode(tmp_path: Path) -> None:
    """mode='w' truncates on open so only the latest session's rows survive."""
    path = tmp_path / "out.jsonl"
    JsonlLoader(path, mode="w").open()  # create first
    loader = JsonlLoader(path, mode="w")
    loader.open()
    loader.upsert(_row(2))
    loader.close()

    lines = path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
    assert json.loads(lines[0])["id"] == "p2"
