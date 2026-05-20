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


def test_zero_budget_raises_on_any_priced_call():
    t = CostTracker(max_usd=0.0)
    with pytest.raises(BudgetExhausted):
        t.record(input_tokens=1, output_tokens=1, model="gpt-4o-mini")


def test_breaching_call_is_still_recorded():
    # The call that pushes spend over the cap is counted before BudgetExhausted
    # is raised — callers can read final totals off the tracker after a halt.
    t = CostTracker(max_usd=0.0001)
    with pytest.raises(BudgetExhausted):
        t.record(input_tokens=10_000, output_tokens=5_000, model="gpt-4o-mini")
    assert t.calls == 1
    assert t.spent_usd > t.max_usd
    assert "gpt-4o-mini" in t.summary()


def test_register_model_price_for_unknown_model():
    t = CostTracker(max_usd=1.00)
    t.register_model_price(
        "ft:gpt-4o-mini:acme:custom",
        input_usd_per_1m=0.30,
        output_usd_per_1m=1.20,
    )
    delta = t.record(
        input_tokens=1_000_000,
        output_tokens=0,
        model="ft:gpt-4o-mini:acme:custom",
    )
    assert delta == pytest.approx(0.30)


def test_register_model_price_overrides_builtin():
    t = CostTracker(max_usd=10.00)
    t.register_model_price("gpt-4o-mini", input_usd_per_1m=1.00, output_usd_per_1m=2.00)
    delta = t.record(input_tokens=1_000_000, output_tokens=0, model="gpt-4o-mini")
    # Built-in gpt-4o-mini input price is 0.15 / 1M; override is 1.00 / 1M.
    assert delta == pytest.approx(1.00)


def test_summary_aggregates_multiple_models():
    t = CostTracker(max_usd=10.00)
    t.record(input_tokens=1000, output_tokens=200, model="gpt-4o-mini")
    t.record(input_tokens=500, output_tokens=100, model="gpt-4o-mini")
    t.record(input_tokens=2000, output_tokens=400, model="claude-3-5-haiku")
    summary = t.summary()
    assert "calls=3" in summary
    assert "gpt-4o-mini: in=1500 out=300" in summary
    assert "claude-3-5-haiku: in=2000 out=400" in summary
