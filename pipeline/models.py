"""Pydantic schemas shared by extract / transform / load."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field, field_validator


class RawProduct(BaseModel):
    """A raw product row as it comes out of extraction (scraped or ingested)."""

    id: str
    name: str
    description: str | None = None
    price_usd: float | None = None
    raw_brand: str | None = None


class ProductEnriched(BaseModel):
    """Output of the LLM enrichment step — structured, validated."""

    id: str
    name: str
    brand: str = Field(
        ...,
        description="Canonical brand name, title-cased. If unknown, set to 'Unknown'.",
    )
    category: Literal[
        "electronics",
        "apparel",
        "home",
        "beauty",
        "grocery",
        "sports",
        "toys",
        "other",
    ]
    tags: list[str] = Field(default_factory=list, max_length=8)
    price_usd: float | None = None

    @field_validator("tags")
    @classmethod
    def _normalize_tags(cls, tags: list[str]) -> list[str]:
        """Canonicalize tags: lowercased, trimmed, de-duplicated, no blanks.

        The enrichment prompt asks the LLM for lowercase tags, but ETL output
        must not depend on the model honoring that on every call. Normalizing
        here keeps downstream grouping and dedup stable regardless of casing or
        stray whitespace — ``"Wireless"``, ``" wireless "`` and ``"wireless"``
        collapse to a single tag. Order of first appearance is preserved.
        """
        seen: set[str] = set()
        normalized: list[str] = []
        for tag in tags:
            cleaned = tag.strip().lower()
            if cleaned and cleaned not in seen:
                seen.add(cleaned)
                normalized.append(cleaned)
        return normalized
