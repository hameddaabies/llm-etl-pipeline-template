"""Tests for the Transformer (LLM enrichment) step."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from pipeline.cost_tracker import BudgetExhausted, CostTracker
from pipeline.models import ProductEnriched, RawProduct
from pipeline.transform import Transformer


def _raw(price: float | None = 19.99) -> RawProduct:
    return RawProduct(
        id="p1",
        name="Wireless Earbuds Pro",
        raw_brand="sony",
        description="Noise-cancelling earbuds",
        price_usd=price,
    )


def _enriched(**overrides) -> ProductEnriched:
    defaults: dict = dict(
        id="p1",
        name="Wireless Earbuds Pro",
        brand="Sony",
        category="electronics",
        tags=["wireless", "noise-cancelling"],
        price_usd=None,
    )
    return ProductEnriched(**(defaults | overrides))


def _make_client(
    parsed: ProductEnriched | None,
    *,
    prompt_tokens: int = 100,
    completion_tokens: int = 50,
) -> MagicMock:
    """Return a mocked OpenAI client whose parse() returns a canned response."""
    response = MagicMock()
    response.choices[0].message.parsed = parsed
    response.usage.prompt_tokens = prompt_tokens
    response.usage.completion_tokens = completion_tokens
    client = MagicMock()
    client.beta.chat.completions.parse.return_value = response
    return client


# ── happy-path ────────────────────────────────────────────────────────────────


def test_enrich_returns_enriched_product() -> None:
    transformer = Transformer(client=_make_client(_enriched()), model="gpt-4o-mini")
    result = transformer.enrich_one(_raw(price=None))
    assert result.id == "p1"
    assert result.brand == "Sony"
    assert result.category == "electronics"


def test_price_passthrough_from_raw() -> None:
    """raw.price_usd must survive even when the LLM leaves price_usd unset."""
    transformer = Transformer(
        client=_make_client(_enriched(price_usd=None)), model="gpt-4o-mini"
    )
    result = transformer.enrich_one(_raw(price=29.99))
    assert result.price_usd == pytest.approx(29.99)


# ── cost tracking ─────────────────────────────────────────────────────────────


def test_cost_recorded_on_enrich() -> None:
    tracker = CostTracker(max_usd=10.00)
    client = _make_client(_enriched(), prompt_tokens=200, completion_tokens=80)
    Transformer(client=client, model="gpt-4o-mini", cost_tracker=tracker).enrich_one(
        _raw()
    )
    assert tracker.calls == 1
    # gpt-4o-mini: $0.15/1M in, $0.60/1M out
    expected = (200 * 0.15 + 80 * 0.60) / 1_000_000
    assert tracker.spent_usd == pytest.approx(expected)


def test_max_usd_zero_raises_budget_exhausted() -> None:
    """CostTracker(max_usd=0.0) must raise BudgetExhausted on the first token."""
    tracker = CostTracker(max_usd=0.0)
    transformer = Transformer(
        client=_make_client(_enriched()),
        model="gpt-4o-mini",
        cost_tracker=tracker,
    )
    with patch("time.sleep"):  # suppress tenacity back-off in retry loop
        with pytest.raises(BudgetExhausted):
            transformer.enrich_one(_raw())


# ── error handling ────────────────────────────────────────────────────────────


def test_null_parsed_raises_runtime_error() -> None:
    """RuntimeError must propagate when the LLM returns no parsed object."""
    transformer = Transformer(client=_make_client(parsed=None), model="gpt-4o-mini")
    with patch("time.sleep"):  # suppress tenacity back-off in retry loop
        with pytest.raises(RuntimeError, match="no parsed object"):
            transformer.enrich_one(_raw())
