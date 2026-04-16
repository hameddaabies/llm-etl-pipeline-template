"""Pydantic schemas shared by extract / transform / load."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


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
