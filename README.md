# llm-etl-pipeline-template

A minimal, production-shaped template for integrating an **LLM enrichment step** into an ETL pipeline. Extract raw records → LLM classifies & normalizes with structured JSON output → load into SQLite (swap in your own warehouse).

This is the pattern I use on real client pipelines — open-sourced as a starting point so you don't have to reinvent the glue for retries, schema validation, cost tracking, and batching every time.

```
  extract.py           transform.py             load.py
┌──────────────┐     ┌──────────────────┐     ┌──────────────┐
│  Raw records │ ──► │  OpenAI enrich   │ ──► │  SQLite DB   │
│  (fixtures)  │     │  Pydantic schema │     │  (replaceable)│
└──────────────┘     └──────────────────┘     └──────────────┘
                             │
                             ▼
                     cost_tracker.py
                    (token + $ budget)
```

## Why this exists

Most "LLM in a pipeline" tutorials skip the boring parts that actually matter in production:

- **Structured outputs** — the LLM returns JSON conforming to a Pydantic schema, not free-form prose you have to regex-parse
- **Retries with backoff** — transient API errors don't nuke the batch
- **Cost tracking** — every call books tokens + USD against a running budget; a cap halts the run before it drains your credits
- **Idempotent loads** — rerunning the pipeline doesn't double-insert
- **No framework lock-in** — just Python + OpenAI + Pydantic + SQLite. Swap any layer.

## Quickstart

```bash
pip install -r requirements.txt
cp .env.example .env
# edit .env, put your OPENAI_API_KEY in it
python -m pipeline.run
```

You should see 10 synthetic product records extracted, enriched (category + normalized brand + tags), and written to `pipeline.db`.

```bash
sqlite3 pipeline.db "SELECT id, name, brand, category FROM products LIMIT 5"
```

## Design notes

### Structured outputs

`transform.py` uses OpenAI's `response_format` with a Pydantic schema (`models.ProductEnriched`). This is the difference between a pipeline that works 99% of the time and one that works 100% — you never have to parse text, you either get a valid object or an exception.

### Cost tracker

`cost_tracker.CostTracker` increments a running total on every API call and raises `BudgetExhausted` when `max_usd` is hit. Wire this into your own workflow — I use this pattern to cap per-domain spend in production scrapers.

### Swap the loader

`load.SqliteLoader` implements a simple `Loader` protocol. Replace it with `MysqlLoader`, `PostgresLoader`, or `S3ParquetLoader` by implementing two methods — no other file needs to change.

## Project layout

```
llm-etl-pipeline-template/
├── pipeline/
│   ├── __init__.py
│   ├── extract.py          # loads from fixtures/products.json
│   ├── transform.py        # OpenAI structured-output enrichment
│   ├── load.py             # SQLite loader (pluggable)
│   ├── models.py           # Pydantic schemas
│   ├── cost_tracker.py     # token + $ budget gate
│   └── run.py              # orchestrator
├── fixtures/
│   └── products.json       # 10 synthetic product rows
├── tests/
│   ├── test_models.py
│   └── test_cost_tracker.py
├── .env.example
├── requirements.txt
└── README.md
```

## Tests

```bash
pytest
```

## Who wrote this

Hamed Daabies — Data Engineer ([Upwork](https://www.upwork.com/freelancers/hameddaabies) · [LinkedIn](https://www.linkedin.com/in/hameddaabies/)).

I build pipelines like this for a living. If you need help integrating an LLM step into your own ETL, get in touch.

## License

MIT
