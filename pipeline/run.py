"""End-to-end orchestrator.

Run with:  python -m pipeline.run
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

from dotenv import load_dotenv

from .config import PipelineConfig
from .cost_tracker import BudgetExhausted, CostTracker
from .extract import extract_from_fixture
from .load import SqliteLoader
from .transform import Transformer

LOG = logging.getLogger("pipeline")


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    )
    load_dotenv()
    cfg = PipelineConfig()

    fixture = Path(__file__).resolve().parent.parent / "fixtures" / "products.json"

    tracker = CostTracker(max_usd=cfg.max_usd)
    transformer = Transformer(model=cfg.openai_model, cost_tracker=tracker)
    loader = SqliteLoader(cfg.db_path)
    loader.open()

    processed = 0
    try:
        for raw in extract_from_fixture(fixture):
            try:
                enriched = transformer.enrich_one(raw)
            except BudgetExhausted as e:
                LOG.warning("budget exhausted: %s — stopping early", e)
                break
            loader.upsert(enriched)
            processed += 1
            LOG.info("loaded id=%s brand=%s category=%s", enriched.id, enriched.brand, enriched.category)
    finally:
        loader.close()

    LOG.info("done. processed=%d\n%s", processed, tracker.summary())
    return 0


if __name__ == "__main__":
    sys.exit(main())
