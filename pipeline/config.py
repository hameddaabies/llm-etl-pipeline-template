"""Typed pipeline settings backed by environment variables.

Values are read from the process environment (or a .env file loaded by
python-dotenv before this module is imported).  Override in tests with
``monkeypatch.setenv``.

Example::

    from pipeline.config import PipelineConfig
    cfg = PipelineConfig()
    print(cfg.max_usd)   # 1.0  (or whatever PIPELINE_MAX_USD is set to)
"""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings


class PipelineConfig(BaseSettings):
    """Runtime knobs for the LLM ETL pipeline.

    All fields map 1-to-1 to an environment variable (see ``alias``).
    Pydantic coerces and validates the raw string values automatically,
    so callers always see the correct Python type.
    """

    db_path: str = Field("pipeline.db", alias="PIPELINE_DB_PATH")
    max_usd: float = Field(1.00, alias="PIPELINE_MAX_USD")
    openai_model: str = Field("gpt-4o-mini", alias="OPENAI_MODEL")

    model_config = {"populate_by_name": True}
