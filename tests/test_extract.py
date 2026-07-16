"""Tests for the extract step."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline.extract import extract_from_fixture
from pipeline.models import RawProduct


def _write(path: Path, data: object) -> Path:
    path.write_text(json.dumps(data), encoding="utf-8")
    return path


def test_yields_raw_products(tmp_path: Path) -> None:
    fixture = _write(
        tmp_path / "products.json",
        [
            {"id": "p1", "name": "Wireless Mouse", "price_usd": 9.99},
            {"id": "p2", "name": "USB Cable", "raw_brand": "acme"},
        ],
    )
    rows = list(extract_from_fixture(fixture))
    assert [r.id for r in rows] == ["p1", "p2"]
    assert all(isinstance(r, RawProduct) for r in rows)
    assert rows[0].price_usd == 9.99
    assert rows[1].raw_brand == "acme"


def test_is_lazy_iterator(tmp_path: Path) -> None:
    """extract yields lazily so large sources stream instead of buffering."""
    fixture = _write(tmp_path / "products.json", [{"id": "p1", "name": "Thing"}])
    gen = extract_from_fixture(fixture)
    assert next(gen).id == "p1"
    with pytest.raises(StopIteration):
        next(gen)


def test_empty_array_yields_nothing(tmp_path: Path) -> None:
    fixture = _write(tmp_path / "products.json", [])
    assert list(extract_from_fixture(fixture)) == []


def test_non_array_fixture_raises_valueerror(tmp_path: Path) -> None:
    """A single JSON object (not an array) fails loudly, not mid-stream."""
    fixture = _write(tmp_path / "products.json", {"id": "p1", "name": "Thing"})
    with pytest.raises(ValueError, match="must contain a JSON array"):
        list(extract_from_fixture(fixture))
