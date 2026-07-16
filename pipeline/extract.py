"""Extract step — read raw rows from a source.

The template ships with a JSON fixture. Swap this module to read from:
  - a scraper's output directory
  - an S3 bucket
  - a source warehouse table
  - a message queue
...without touching transform or load.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path

from .models import RawProduct


def extract_from_fixture(path: str | Path) -> Iterator[RawProduct]:
    """Yield raw product rows from a JSON fixture file.

    The fixture must hold a JSON *array* of product objects. A non-array
    top-level (e.g. a single object) raises ``ValueError`` up front instead of
    silently iterating dict keys and failing later with an opaque validation
    error mid-stream.
    """
    with Path(path).open("r", encoding="utf-8") as f:
        rows = json.load(f)
    if not isinstance(rows, list):
        raise ValueError(
            f"fixture {Path(path)} must contain a JSON array of products, "
            f"got {type(rows).__name__}"
        )
    for row in rows:
        yield RawProduct.model_validate(row)
