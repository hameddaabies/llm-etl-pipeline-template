import pytest

from pipeline.cost_tracker import BudgetExhausted, CostTracker


def test_single_call_within_budget():
    t = CostTracker(max_usd=1.00)
    t.record(input_tokens=1000, output_tokens=500, model="gpt-4o-mini")
    assert t.calls == 1
    assert t.spent_usd > 0


def test_exhaust_raises():
    t = CostTracker(max_usd=0.0001)
    with pytest.raises(BudgetExhausted):
        t.record(input_tokens=10_000, output_tokens=5_000, model="gpt-4o-mini")


def test_unknown_model_costs_zero():
    t = CostTracker(max_usd=1.00)
    delta = t.record(input_tokens=1_000_000, output_tokens=0, model="some-new-model")
    assert delta == 0.0
