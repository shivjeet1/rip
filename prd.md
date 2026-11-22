# PRD: Autonomous Insurance Claims Pipeline & Incident Remediation Agent

## 1. Executive Summary

This project delivers an **Autonomous AI Agent-Based Insurance Claims Lakehouse Pipeline** built for **Databricks Free Edition (Serverless Compute)**. It pairs an end-to-end Insurance Claims Medallion pipeline (Bronze -> Silver -> Gold Star Schema with automated source-to-target reconciliation) with a tool-calling AI agent that monitors execution telemetry, diagnoses real pipeline incidents, quantifies insurance payout risk, generates Spark Connect-safe PySpark code diffs and enforces idempotent retry safety.

To stay within Databricks Free Edition quotas and outbound network restrictions, the pipeline and Unity Catalog Delta tables execute inside Databricks Serverless Compute while the FastAPI control plane, SQLite cache, tool-calling AI agent and pure-black AMOLED web UI run locally with a 1-command `MOCK_MODE=true` offline demo path.

---

## 2. Hard Platform & Engineering Constraints

### 2.1 Databricks Free Edition (Serverless Compute)

* **Spark Connect Strictly Enforced:** Never use RDD or JVM internals (`sc.parallelize`, `df.rdd`, `df._jdf`, `SparkContext`). Use only Spark Connect-compatible DataFrame and Spark SQL APIs.
* **Programmatic Telemetry (No Spark UI / Driver Logs):** Capture physical query plans via stdout-redirected `df.explain(mode="formatted")`, data skew via DataFrame `groupBy` aggregations, reconciliation counts via explicit DataFrame audits and exception traces via a Python context manager.
* **Quota & Network Safety:** Keep raw synthetic datasets at 50,000 claim records so full 3-stage Serverless runs finish in under 2 minutes without hitting daily Free Edition limits. Cache all `databricks-sql-connector` reads in local SQLite (`CACHE_TTL_SECONDS=60`) and run the LLM agent locally to avoid Free Edition outbound network blocks.

### 2.2 Engineering & Design Standards

* **Code Style:** Clean, modular, strictly comment-free code across all Python, SQL, HTML, CSS and JavaScript files.
* **UI Theme (Strict AMOLED, Zero Green):** Pure black (`#000000`) background, `#09090B` cards, `#121215` elevated panels, `#27272A` 1px borders and `#FAFAFA`/`#A1A1AA` typography. Never use green anywhere in CSS, badges or diffs. Use cyan (`#06B6D4`) or white for healthy/safe states, amber (`#F59E0B`) for skew/warnings and crimson (`#EF4444`) for failures, payout risks and unsafe retries.
* **Zero-Build Frontend:** Serve the dashboard directly from FastAPI (`web/index.html`, `web/styles.css`, `web/app.js`) with zero Node/npm build dependencies.
* **LLM Portability & Reliability:** Support OpenAI-compatible APIs and local Ollama via `LLM_PROVIDER`, `LLM_BASE_URL`, `LLM_MODEL` and `LLM_API_KEY`, plus a deterministic rule-backed fallback so local demos never break if an external API key is absent or rate-limited.

---

## 3. System Architecture & Repository Structure

| Component | Runtime | Responsibility |
| --- | --- | --- |
| **Raw Data Generator** | Local / Databricks Volume | Produces multi-format source files (`claims_raw.json`, `providers.csv`, `policy_tiers.csv`) preserving raw landing files

 |
| **Insurance Claims Lakehouse** | Databricks Free Edition | 3 Serverless notebooks executing Bronze ingestion, Silver adjudication + quarantine + reconciliation and Gold Star Schema aggregation

 |
| **Orchestration Layer** | Databricks Job + Airflow DAG | 3-task Databricks Serverless Job plus a production-style Airflow DAG (`DatabricksRunNowOperator`)

 |
| **Observability & Control Plane** | Local (FastAPI + SQLite) | Triggers Databricks Jobs (`databricks-sdk`), reads Unity Catalog observability tables (`databricks-sql-connector`) with SQLite caching and supports `MOCK_MODE=true` |
| **Autonomous Remediation Agent** | Local (Python) | Tool-calling LLM agent inspecting Silver incidents, reconciliation reports, formatted Spark plans, tracebacks and notebook source code to output verified diffs and payout-safe recovery plans |
| **Observability Dashboard** | Local (FastAPI Static UI) | Pure-black AMOLED interface for incident triage, claims reconciliation audit, scenario injection and diff/retry review |

```text
.
├── AGENTS.md
├── prd.md
├── README.md
├── .env.example
├── requirements.txt
├── data/
│   └── raw/                        # Generated claims_raw.json, providers.csv, policy_tiers.csv
├── databricks_notebooks/
│   ├── setup_tables.sql            # Unity Catalog schema, domain Star Schema & observability DDL
│   ├── observability_helper.py     # Spark Connect-safe observe_stage context manager + SQLite fallback
│   ├── 01_ingest_raw_claims.py     # Reads raw JSON/CSV -> claims_bronze, providers_dim, policy_tiers_dim
│   ├── 02_adjudicate_claims.py     # Validates, quarantines, reconciles & enriches -> claims_silver + quarantine
│   └── 03_generate_payout_batch.py # Builds Gold Star Schema (fact_claims_payouts + dim_date)
├── sql/
│   └── business_analytics.sql      # 5 insurance business & reconciliation SQL queries
├── airflow/
│   └── dags/
│       └── insurance_claims_dag.py # Airflow DAG orchestrating the Databricks Job + SLA alerting
├── backend/
│   ├── config.py
│   ├── db.py
│   ├── databricks_client.py
│   ├── main.py
│   └── agent/
│       ├── schemas.py
│       ├── tools.py
│       ├── prompts.py
│       └── engine.py
├── web/
│   ├── index.html
│   ├── styles.css
│   └── app.js
└── scripts/
    ├── generate_raw_data.py        # Generates JSON & CSV source files per scenario
    └── test_local_mock.py          # End-to-end Phase 1 verification & SQLite seed script

```

---

## 4. Data Model & Observability Contracts

Default Unity Catalog namespace: `workspace.claims_ops` (overridable via `DATABRICKS_CATALOG` and `DATABRICKS_SCHEMA`). Mirror `obs_bronze_runs`, `obs_silver_incidents`, `claims_reconciliation_report` and `agent_diagnoses` in local SQLite (`backend/local_cache.db`).

### 4.1 Insurance Claims Medallion & Star Schema Tables

* **Bronze (`claims_bronze`):** `claim_id STRING`, `policy_id STRING`, `provider_id STRING`, `claim_amount STRING`, `claim_date STRING`, `claim_status STRING`, `diagnosis_code STRING`, `ingested_at TIMESTAMP`, `run_id STRING` (preserves raw strings verbatim for auditability).


* **Silver (`claims_silver`):** `claim_id STRING`, `policy_id STRING`, `provider_id STRING`, `claim_date DATE`, `claim_amount DOUBLE`, `approved_payout DOUBLE`, `claim_status STRING`, `diagnosis_code STRING`, `adjudicated_at TIMESTAMP`, `run_id STRING` (deduplicated, type-validated and enriched).


* **Quarantine (`claims_quarantine`):** `claim_id STRING`, `run_id STRING`, `rejection_reason STRING` (`DUPLICATE_CLAIM_ID`, `MALFORMED_AMOUNT`, `INVALID_PROVIDER_ID`, `INVALID_POLICY_ID`, `INVALID_DATE`), `raw_payload STRING`, `quarantined_at TIMESTAMP`.


* **Reconciliation (`claims_reconciliation_report`):** `run_id STRING`, `scenario STRING`, `records_received BIGINT`, `records_valid BIGINT`, `records_rejected BIGINT`, `duplicate_count BIGINT`, `malformed_amount_count BIGINT`, `invalid_provider_count BIGINT`, `invalid_policy_count BIGINT`, `rejection_rate DOUBLE`, `total_payout_usd DOUBLE`, `created_at TIMESTAMP`.


* Gold Star Schema:


* `dim_provider`: `provider_id STRING`, `provider_name STRING`, `network_tier STRING`, `region STRING`
* `dim_policy_tier`: `policy_id STRING`, `tier_name STRING`, `coverage_pct DOUBLE`, `deductible DOUBLE`
* `dim_date`: `date_id INT`, `full_date DATE`, `month INT`, `quarter INT`, `year INT`
* `fact_claims_payouts`: `batch_id STRING`, `run_id STRING`, `provider_id STRING`, `date_id INT`, `region STRING`, `claim_count BIGINT`, `total_claimed_usd DOUBLE`, `total_payout_usd DOUBLE`, `created_at TIMESTAMP`



### 4.2 Observability Tables

* **`obs_bronze_runs`:** `event_id STRING`, `run_id STRING`, `job_id STRING`, `orchestrator STRING`, `dag_id STRING`, `task_key STRING`, `scenario STRING`, `notebook_path STRING`, `status STRING` (`SUCCESS`, `DEGRADED`, `FAILED`), `query_plan_formatted STRING`, `stack_trace STRING`, `raw_metrics_json STRING`, `created_at TIMESTAMP`.
* **`obs_silver_incidents`:** `incident_id STRING`, `event_id STRING`, `run_id STRING`, `task_key STRING`, `scenario STRING`, `status STRING`, `severity STRING` (`LOW`, `MEDIUM`, `HIGH`, `CRITICAL`), `business_criticality STRING` (`MEDIUM`, `HIGH`, `CRITICAL`), `duration_sec DOUBLE`, `row_count_in BIGINT`, `row_count_out BIGINT`, `rejected_row_count BIGINT`, `top_key_skew_ratio DOUBLE`, `payout_exposure_usd DOUBLE`, `error_class STRING`, `write_mode STRING`, `sla_breached BOOLEAN`, `created_at TIMESTAMP`.
* **`agent_diagnoses` (SQLite):** `incident_id STRING`, `root_cause_category STRING`, `summary STRING`, `insurance_business_impact STRING`, `payout_exposure_usd DOUBLE`, `code_diff STRING`, `retry_safety STRING`, `retry_recommendation STRING`, `model_used STRING`, `created_at TIMESTAMP`.

---

## 5. Multi-Format Ingestion & Injectable Failure Scenarios

`scripts/generate_raw_data.py` writes actual raw files (`claims_raw.json`, `providers.csv`, `policy_tiers.csv`) into `data/raw/{scenario}/` (uploadable to a Unity Catalog Volume `/Volumes/workspace/claims_ops/raw_landing/` on Databricks Free Edition, with an automatic in-notebook file-generation fallback if the Volume is empty).

| Scenario | Affected Stage | Fault Injection Mechanism | Captured Telemetry & Reconciliation Signals | Expected Agent Output & Retry Safety |
| --- | --- | --- | --- | --- |
| **`NORMAL`** | All 3 | 50,000 uniform claims, clean numeric amounts, valid keys, idempotent `overwrite` | `status=SUCCESS`, `top_key_skew_ratio < 0.05`, `rejection_rate < 0.02`, `sla_breached=False` | No incident raised |
| **`A` (`DATA_SKEW`)** | `01` & `02` | 70% of raw JSON claims belong to a single hospital network (`provider_id="HOSP_999"`); `02_adjudicate_claims.py` executes an unsalted join | `status=DEGRADED`, `top_key_skew_ratio >= 0.65`, `error_class=DATA_SKEW`, `sla_breached=True`, `payout_exposure_usd` = delayed claims total | Unified diff introducing salted join keys (`concat(provider_id, '_', floor(rand() * 10))`) + exploded `dim_provider`; `SAFE_TO_RETRY` |
| **`B` (`SCHEMA_DRIFT`)** | `01` & `02` | Upstream claims JSON contains currency-formatted amounts (`"$1,450.50"`), 4% duplicate `claim_id`s and unmapped `policy_id`s; naive `.cast("double")` fails quality gate when `rejection_rate > 0.15` | `status=FAILED`, `error_class=SCHEMA_DRIFT`, full reconciliation breakdown (`malformed_amount_count`, `duplicate_count`, `invalid_policy_count`), quality-gate stack trace | Unified diff applying `regexp_replace(col("claim_amount"), "[^0-9.]", "").cast("double")` while keeping quarantine routing for true invalid IDs; `SAFE_TO_RETRY` |
| **`C` (`PARTIAL_PAYOUT_WRITE`)** | `03` | Splits `fact_claims_payouts` by region into 2 batches, commits batch 1 via `.mode("append")`, then raises `RuntimeError("Downstream clearinghouse ledger timeout")` before batch 2 | `status=FAILED`, `write_mode=append_non_idempotent`, `business_criticality=CRITICAL`, `payout_exposure_usd` = exact USD already committed in batch 1 | Unified diff switching to Delta `MERGE` or `replaceWhere` keyed by `run_id`; strictly flags `MANUAL_CLEANUP_REQUIRED` with `DELETE FROM workspace.claims_ops.fact_claims_payouts WHERE run_id = '...'` |

---

## 6. Business Analytical SQL Pack (`sql/business_analytics.sql`)

Provide 5 production-ready Databricks SQL queries against the Gold Star Schema and Reconciliation table:

1. **Top 10 Healthcare Providers by Approved Payout Volume** (`fact_claims_payouts` joined with `dim_provider`).
2. **Regional Claims Payout Distribution & Average Claim Value** (`fact_claims_payouts` grouped by `region`).
3. **Monthly Claims Volume & Payout Trend** (`fact_claims_payouts` joined with `dim_date`).
4. **Policy Tier Coverage Utilization & Deductible Impact** (`claims_silver` joined with `dim_policy_tier`).
5. **Pipeline Source-to-Target Reconciliation & Rejection Breakdown Audit** (`claims_reconciliation_report` ordered by `created_at DESC`).



---

## 7. Backend API, Autonomous Agent & AMOLED Dashboard

### 7.1 FastAPI Control Plane (`backend/main.py`)

* `GET /` — Serves `web/index.html` and static assets (`/static/styles.css`, `/static/app.js`).
* `GET /api/health` — Returns runtime mode (`MOCK_MODE` vs. `DATABRICKS_LIVE`), cache freshness and configured LLM provider.
* `GET /api/overview` — Returns KPIs (total runs, SLA breaches, critical payout exposure in USD, latest reconciliation pass rate and cache sync time).
* `GET /api/incidents` — Lists `obs_silver_incidents` joined with diagnosis state, sorted by `business_criticality` (`CRITICAL` first) then `created_at DESC`. Supports `?refresh=true` to pull fresh rows from Databricks SQL Warehouse.
* `GET /api/incidents/{incident_id}` — Returns Silver incident metrics, Bronze `query_plan_formatted`, `stack_trace`, matching `claims_reconciliation_report` row and cached `agent_diagnoses`.
* `POST /api/scenarios/trigger` — Accepts `{"scenario": "NORMAL" | "A" | "B" | "C"}`. Calls `WorkspaceClient().jobs.run_now(...)` in live mode or runs the local mock pipeline + reconciliation simulator in `MOCK_MODE=true`.
* `POST /api/incidents/{incident_id}/diagnose` — Executes the tool-calling agent, validates output via Pydantic, persists to `agent_diagnoses` and returns JSON.

### 7.2 Autonomous Tool-Calling Agent (`backend/agent/`)

* **Tools:**
1. `get_incident_and_reconciliation(incident_id: str)` — Fetches Silver incident metrics plus the matching `claims_reconciliation_report` breakdown.
2. `get_plan_and_traceback(incident_id: str)` — Fetches `query_plan_formatted`, `stack_trace` and `raw_metrics_json` from `obs_bronze_runs`.
3. `read_notebook_source(notebook_path: str)` — Reads the exact PySpark notebook file from `databricks_notebooks/`.


* **Pydantic Output Contract:**
```json
{
  "root_cause_category": "DATA_SKEW | SCHEMA_DRIFT | PARTIAL_PAYOUT_WRITE | OTHER",
  "summary": "string",
  "insurance_business_impact": "string",
  "payout_exposure_usd": 0.0,
  "code_diff": "unified git diff string",
  "retry_safety": "SAFE_TO_RETRY | MANUAL_CLEANUP_REQUIRED",
  "retry_recommendation": "string"
}

```


* **Guardrails:** Never output RDD APIs or classic cluster configs. Always enforce `MANUAL_CLEANUP_REQUIRED` with explicit `DELETE` SQL whenever `write_mode == "append_non_idempotent"` or partial payout batches exist.

### 7.3 AMOLED Web Dashboard (`web/`)

Single-page layout with zero build step and zero green styling:

1. **Header & KPI Strip:** Mode indicator (`MOCK` / `DATABRICKS LIVE`), manual **Sync Warehouse** button, Total Runs, SLA Breaches, Payout Exposure ($ USD) and Latest Reconciliation Rate (%).
2. **Scenario Trigger Bar:** 4 one-click scenario triggers (`NORMAL`, `A: Provider Skew`, `B: Schema Drift`, `C: Partial Payout Commit`).
3. **Split Workspace:**
* **Left Panel (Incident Queue):** Prioritized list with criticality tags, status dots (cyan/amber/crimson), error class and diagnosed badge.
* **Right Panel (Agent Triage & Reconciliation):** **Run Agent** button, prominent **Retry Safety Badge** (`SAFE_TO_RETRY` in cyan vs. `MANUAL_CLEANUP_REQUIRED` in crimson), Insurance Financial Impact card, Reconciliation breakdown bar (`Received / Valid / Rejected by Reason`), tabbed artifact viewer (**PySpark Diff** with cyan additions and crimson deletions, **Formatted Query Plan**, **Stack Trace**) and copyable **Recovery SQL / Command** box.



---

## 8. 24-Hour Execution Plan & Hard Verification Gates

Build strictly in **2 fast, verifiable phases** plus a mandatory **Submission Polish Pass** so the public GitHub repository is complete and demo-ready before tomorrow's deadline.

### Phase 1: Lakehouse Pipeline, Reconciliation, SQL Pack & Mock Verification (Target: 2–3 Hours)

* **Deliverables:** `AGENTS.md`, `.env.example`, `requirements.txt`, `scripts/generate_raw_data.py`, `databricks_notebooks/setup_tables.sql`, `databricks_notebooks/observability_helper.py`, all 3 Spark Connect-safe notebooks, `sql/business_analytics.sql`, `airflow/dags/insurance_claims_dag.py` and `scripts/test_local_mock.py`.
* **Verification Gate:** Running `python scripts/test_local_mock.py` generates raw JSON/CSV files, executes `NORMAL` plus Scenarios `A`, `B` and `C`, seeds `backend/local_cache.db` with Bronze, Silver and Reconciliation rows and asserts expected statuses, rejection breakdowns and `payout_exposure_usd`. Static scan confirms zero RDD calls and zero code comments.

### Phase 2: FastAPI Control Plane, Autonomous Agent & AMOLED Dashboard (Target: 3–4 Hours)

* **Deliverables:** `backend/` (`config.py`, `db.py`, `databricks_client.py`, `agent/*`, `main.py`) and `web/` (`index.html`, `styles.css`, `app.js`).
* **Verification Gate:** `uvicorn backend.main:app` boots cleanly in `MOCK_MODE=true`; automated test verifies `/api/overview`, `/api/incidents`, `/api/scenarios/trigger` and `/api/incidents/{id}/diagnose` across Scenarios A, B and C (confirming `MANUAL_CLEANUP_REQUIRED` + `DELETE` SQL on Scenario C); static CSS audit confirms zero green colors anywhere in `web/`.

### Phase 3: Live Databricks Free Edition Run & Public GitHub Polish (Target: 2–3 Hours)

* **Deliverables:** Execute the 3 notebooks as a real 3-task Databricks Free Edition Job (capturing screenshots of the Job DAG, Unity Catalog Bronze/Silver/Gold + Reconciliation tables and SQL Analytics query output), switch `.env` to live mode once to verify `databricks-sdk` + SQL Warehouse sync and finalize `README.md` with architecture diagram, Star Schema diagram, live screenshots, sample agent JSON/diff and 1-command local run instructions.


