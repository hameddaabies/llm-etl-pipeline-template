"""Budget gate — halts the pipeline before it drains your API credits.

Usage:
    tracker = CostTracker(max_usd=1.00)
    tracker.record(input_tokens=500, output_tokens=120, model="gpt-4o-mini")
    # raises BudgetExhausted if running total >= max_usd

Prices below are a starting point — check OpenAI's pricing page for current
numbers and update PRICE_PER_1M_TOKENS as needed.
"""

from __future__ import annotations

from dataclasses import dataclass, field


class BudgetExhausted(RuntimeError):
    """Raised when cumulative spend reaches the configured cap."""


# USD per 1M tokens. Approximate — update to match your account pricing.
PRICE_PER_1M_TOKENS: dict[str, tuple[float, float]] = {
    # model: (input, output)
    # OpenAI
    "gpt-4o-mini": (0.15, 0.60),
    "gpt-4o": (2.50, 10.00),
    "gpt-4.1-mini": (0.40, 1.60),
    # Anthropic
    "claude-3-5-haiku": (0.80, 4.00),
    "claude-3-5-sonnet": (3.00, 15.00),
    "claude-3-opus": (15.00, 75.00),
    # Google
    "gemini-1.5-flash": (0.075, 0.30),
    "gemini-1.5-pro": (1.25, 5.00),
}


@dataclass
class CostTracker:
    max_usd: float
    spent_usd: float = 0.0
    calls: int = 0
    _per_model_tokens: dict[str, tuple[int, int]] = field(default_factory=dict)

    def record(self, *, input_tokens: int, output_tokens: int, model: str) -> float:
        """Add a call's cost to the running total. Returns the USD delta."""
        in_price, out_price = PRICE_PER_1M_TOKENS.get(model, (0.0, 0.0))
        delta = (input_tokens * in_price + output_tokens * out_price) / 1_000_000
        self.spent_usd += delta
        self.calls += 1
        prev_in, prev_out = self._per_model_tokens.get(model, (0, 0))
        self._per_model_tokens[model] = (prev_in + input_tokens, prev_out + output_tokens)
        if self.spent_usd >= self.max_usd:
            raise BudgetExhausted(
                f"Spent ${self.spent_usd:.4f} / cap ${self.max_usd:.2f} "
                f"after {self.calls} calls"
            )
        return delta

    def summary(self) -> str:
        lines = [
            f"calls={self.calls}  spent=${self.spent_usd:.4f}/{self.max_usd:.2f}",
        ]
        for m, (i, o) in self._per_model_tokens.items():
            lines.append(f"  {m}: in={i} out={o}")
        return "\n".join(lines)
