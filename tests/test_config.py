"""Tests for typed PipelineConfig settings."""
from __future__ import annotations

import pytest

from pipeline.config import PipelineConfig


def test_defaults() -> None:
    """Unset environment yields the documented defaults."""
    cfg = PipelineConfig()
    assert cfg.db_path == "pipeline.db"
    assert cfg.max_usd == 1.00
    assert cfg.openai_model == "gpt-4o-mini"


def test_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    """Each alias maps correctly to its env variable."""
    monkeypatch.setenv("PIPELINE_DB_PATH", "/tmp/test.db")
    monkeypatch.setenv("PIPELINE_MAX_USD", "2.50")
    monkeypatch.setenv("OPENAI_MODEL", "gpt-4o")
    cfg = PipelineConfig()
    assert cfg.db_path == "/tmp/test.db"
    assert cfg.max_usd == 2.50
    assert cfg.openai_model == "gpt-4o"


def test_max_usd_coerced_to_float(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pydantic coerces the raw env string to float automatically."""
    monkeypatch.setenv("PIPELINE_MAX_USD", "0.50")
    cfg = PipelineConfig()
    assert isinstance(cfg.max_usd, float)
    assert cfg.max_usd == pytest.approx(0.50)
