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
    """Yield raw product rows from a JSON fixture file."""
    with Path(path).open("r", encoding="utf-8") as f:
        for row in json.load(f):
            yield RawProduct.model_validate(row)
