"""Transform step — LLM enrichment with structured outputs.

Each raw product is sent to the LLM, which returns a ProductEnriched
Pydantic object. Wrapped in a retry loop and gated by CostTracker.
"""

from __future__ import annotations

import os

from openai import OpenAI
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from .cost_tracker import CostTracker
from .models import ProductEnriched, RawProduct

SYSTEM_PROMPT = (
    "You normalize noisy e-commerce product data. "
    "Return a canonical brand name (title-case), classify into one of the "
    "fixed categories, and add 2–6 concise lowercase tags describing the item. "
    "If the brand is truly unknown, use 'Unknown'."
)


class Transformer:
    def __init__(
        self,
        *,
        client: OpenAI | None = None,
        model: str | None = None,
        cost_tracker: CostTracker | None = None,
    ) -> None:
        self.client = client or OpenAI()
        self.model = model or os.getenv("OPENAI_MODEL", "gpt-4o-mini")
        self.cost_tracker = cost_tracker

    @retry(
        reraise=True,
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(Exception),
    )
    def enrich_one(self, raw: RawProduct) -> ProductEnriched:
        user_msg = (
            f"id: {raw.id}\n"
            f"name: {raw.name}\n"
            f"raw_brand: {raw.raw_brand or '(none)'}\n"
            f"description: {raw.description or '(none)'}\n"
            f"price_usd: {raw.price_usd if raw.price_usd is not None else '(none)'}"
        )
        response = self.client.beta.chat.completions.parse(
            model=self.model,
            response_format=ProductEnriched,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_msg},
            ],
        )
        if self.cost_tracker and response.usage:
            self.cost_tracker.record(
                input_tokens=response.usage.prompt_tokens,
                output_tokens=response.usage.completion_tokens,
                model=self.model,
            )
        parsed = response.choices[0].message.parsed
        if parsed is None:
            raise RuntimeError(f"LLM returned no parsed object for {raw.id}")
        # Keep price pass-through — the LLM shouldn't hallucinate a number here.
        parsed.price_usd = raw.price_usd
        return parsed
