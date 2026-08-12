"""Tests for the end-to-end orchestrator."""
from __future__ import annotations

import sqlite3

from pipeline import run as run_mod
from pipeline.cost_tracker import BudgetExhausted
from pipeline.models import ProductEnriched, RawProduct


class _FakeTransformer:
    """Enriches every row except the ids listed in ``fail_ids``."""

    def __init__(self, *, fail_ids: set[str], budget_out_at: str | None = None, **_):
        self.fail_ids = fail_ids
        self.budget_out_at = budget_out_at

    def enrich_one(self, raw: RawProduct) -> ProductEnriched:
        if raw.id == self.budget_out_at:
            raise BudgetExhausted("cap reached")
        if raw.id in self.fail_ids:
            raise RuntimeError(f"LLM returned no parsed object for {raw.id}")
        return ProductEnriched(
            id=raw.id, name=raw.name, brand="Acme", category="other", tags=[]
        )


def _patch_pipeline(monkeypatch, tmp_path, transformer, ids=("p1", "p2", "p3")):
    """Point run.main() at an in-memory row source and a throwaway sqlite file."""
    rows = [RawProduct(id=i, name=f"product {i}") for i in ids]
    monkeypatch.setenv("PIPELINE_DB_PATH", str(tmp_path / "out.db"))
    monkeypatch.setattr(run_mod, "extract_from_fixture", lambda _path: iter(rows))
    monkeypatch.setattr(run_mod, "Transformer", lambda **kw: transformer)


def _loaded_ids(tmp_path) -> set[str]:
    with sqlite3.connect(tmp_path / "out.db") as conn:
        return {r[0] for r in conn.execute("SELECT id FROM products")}


def test_bad_row_is_skipped_and_batch_continues(monkeypatch, tmp_path):
    # A row whose enrichment raises must not abort the rows behind it.
    transformer = _FakeTransformer(fail_ids={"p2"})
    _patch_pipeline(monkeypatch, tmp_path, transformer)
    assert run_mod.main() == 0
    assert _loaded_ids(tmp_path) == {"p1", "p3"}


def test_budget_exhausted_still_halts_the_run(monkeypatch, tmp_path):
    # The skip-and-continue path must not swallow BudgetExhausted: a tripped
    # cost cap means stop spending, so p3 is never enriched.
    transformer = _FakeTransformer(fail_ids=set(), budget_out_at="p2")
    _patch_pipeline(monkeypatch, tmp_path, transformer)
    assert run_mod.main() == 0
    assert _loaded_ids(tmp_path) == {"p1"}
