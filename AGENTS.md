# AGENTS.md

These rules are mandatory for every file in this repository. They are not suggestions and they are not scoped to a single phase.

## 1. Databricks Free Edition: Serverless and Spark Connect only

- Never use RDD APIs or JVM internals.
- Forbidden tokens include `sc.parallelize`, `df.rdd`, `df._jdf`, and `SparkContext`.
- Use only Spark Connect-compatible DataFrame APIs and Spark SQL.
- Do not assume Spark UI access, driver log access, or classic cluster configuration.
- Prefer Unity Catalog managed Delta tables under `workspace.claims_ops`.

## 2. Programmatic telemetry only

- Capture formatted physical plans by redirecting stdout from `df.explain(mode="formatted")`.
- Measure data skew with DataFrame `groupBy` aggregations.
- Produce reconciliation breakdowns with explicit DataFrame audits.
- Capture exception tracebacks with the `observe_stage` context manager.
- Persist telemetry to both Delta observability tables and the local SQLite fallback when available.

## 3. Strictly comment-free source

- Do not write inline comments, block comments, or docstrings in any `.py`, `.sql`, `.html`, `.css`, or `.js` file.
- Do not add commented-out code.
- Let function names, types, and structure communicate intent.
- Markdown documentation is allowed to contain normal prose.

## 4. Strict AMOLED UI with zero green

- Canvas: `#000000`.
- Cards: `#09090B`.
- Elevated panels: `#121215`.
- Borders: `#27272A`.
- Primary text: `#FAFAFA`.
- Secondary text: `#A1A1AA`.
- Healthy and safe: cyan `#06B6D4` or white.
- Skew and warnings: amber `#F59E0B`.
- Failures, payout risk, and unsafe retries: crimson `#EF4444`.
- Never use green, emerald, lime, teal, or any green hex, rgb, or hsl value anywhere under `web/`.

## 5. Zero-build frontend and offline demo safety

- Serve `web/index.html`, `web/styles.css`, and `web/app.js` directly from FastAPI.
- Do not introduce Node, npm, bundlers, or a compile step.
- Support `MOCK_MODE=true` with deterministic local execution and SQLite caching at `backend/local_cache.db`.
- Support OpenAI-compatible APIs and Ollama through environment variables.
- Always provide a deterministic rule-backed analyzer when `LLM_API_KEY` is absent or the provider is unreachable.
- The local dashboard must be fully usable without network access.

## Delivery discipline

- Every endpoint, notebook, SQL query, and UI panel must be complete and executable.
- No stubs, TODOs, placeholder functions, fake handlers, or commented-out alternatives.
- Verify each phase end to end before moving on.
- Fix failures rather than documenting around them.
