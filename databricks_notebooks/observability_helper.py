from __future__ import annotations

import contextlib
import io
import json
import os
import sqlite3
import time
import traceback
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SQLITE_SCHEMA = """
CREATE TABLE IF NOT EXISTS obs_bronze_runs (
    event_id TEXT PRIMARY KEY,
    run_id TEXT,
    job_id TEXT,
    orchestrator TEXT,
    dag_id TEXT,
    task_key TEXT,
    scenario TEXT,
    notebook_path TEXT,
    status TEXT,
    query_plan_formatted TEXT,
    stack_trace TEXT,
    raw_metrics_json TEXT,
    created_at TEXT
);
CREATE TABLE IF NOT EXISTS obs_silver_incidents (
    incident_id TEXT PRIMARY KEY,
    event_id TEXT,
    run_id TEXT,
    task_key TEXT,
    scenario TEXT,
    status TEXT,
    severity TEXT,
    business_criticality TEXT,
    duration_sec REAL,
    row_count_in INTEGER,
    row_count_out INTEGER,
    rejected_row_count INTEGER,
    top_key_skew_ratio REAL,
    payout_exposure_usd REAL,
    error_class TEXT,
    write_mode TEXT,
    sla_breached INTEGER,
    created_at TEXT
);
CREATE TABLE IF NOT EXISTS claims_reconciliation_report (
    run_id TEXT PRIMARY KEY,
    scenario TEXT,
    records_received INTEGER,
    records_valid INTEGER,
    records_rejected INTEGER,
    duplicate_count INTEGER,
    malformed_amount_count INTEGER,
    invalid_provider_count INTEGER,
    invalid_policy_count INTEGER,
    rejection_rate REAL,
    total_payout_usd REAL,
    created_at TEXT
);
"""


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _local_cache_path() -> Path:
    raw = os.getenv("LOCAL_CACHE_DB") or os.getenv("LOCAL_CACHE_PATH") or "backend/local_cache.db"
    path = Path(raw)
    if not path.is_absolute():
        path = Path.cwd() / path
    return path


def _write_sqlite(table: str, row: dict[str, Any]) -> bool:
    path = _local_cache_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(str(path), timeout=5)
        try:
            connection.executescript(SQLITE_SCHEMA)
            columns = list(row.keys())
            placeholders = ", ".join("?" for _ in columns)
            column_sql = ", ".join(columns)
            values = [row[column] for column in columns]
            connection.execute(
                f"INSERT OR REPLACE INTO {table} ({column_sql}) VALUES ({placeholders})",
                values,
            )
            connection.commit()
        finally:
            connection.close()
        return True
    except Exception:
        return False


def _delta_row(row: dict[str, Any]) -> dict[str, Any]:
    converted: dict[str, Any] = {}
    for key, value in row.items():
        if key.endswith("_at") and isinstance(value, str):
            try:
                converted[key] = datetime.fromisoformat(value)
                continue
            except ValueError:
                converted[key] = value
                continue
        converted[key] = value
    return converted


def _delta_table(catalog: str, schema: str, table: str) -> str:
    return f"{catalog}.{schema}.{table}"


def persist_row(
    spark: Any,
    catalog: str,
    schema: str,
    table: str,
    row: dict[str, Any],
    sqlite_table: str,
) -> str:
    if spark is not None:
        try:
            payload = _delta_row(row)
            frame = spark.createDataFrame([payload])
            frame.write.mode("append").saveAsTable(_delta_table(catalog, schema, table))
            return "delta"
        except Exception:
            pass
    if _write_sqlite(sqlite_table, row):
        return "sqlite"
    return "none"


class observe_stage:
    def __init__(
        self,
        spark: Any,
        run_id: str,
        task_key: str,
        scenario: str,
        notebook_path: str,
        catalog: str = "workspace",
        schema: str = "claims_ops",
        job_id: str = "0",
        orchestrator: str = "databricks_job",
        dag_id: str = "insurance_claims_pipeline",
        write_mode: str = "overwrite",
        query_plan_df: Any = None,
        **metrics: Any,
    ) -> None:
        self.spark = spark
        self.run_id = run_id
        self.task_key = task_key
        self.scenario = scenario
        self.notebook_path = notebook_path
        self.catalog = catalog
        self.schema = schema
        self.job_id = job_id
        self.orchestrator = orchestrator
        self.dag_id = dag_id
        self.write_mode = write_mode
        self.query_plan_df = query_plan_df
        self.metrics = metrics
        self.event_id = ""
        self.status = "RUNNING"
        self.duration_sec = 0.0
        self.query_plan_formatted = ""
        self.stack_trace = ""
        self.error_class = ""
        self.degraded = False
        self.started_at = 0.0

    def __enter__(self) -> observe_stage:
        self.started_at = time.perf_counter()
        self.event_id = f"evt_{uuid.uuid4().hex[:12]}"
        if self.query_plan_df is not None:
            buffer = io.StringIO()
            try:
                with contextlib.redirect_stdout(buffer):
                    self.query_plan_df.explain(mode="formatted")
            except Exception as error:
                self.query_plan_formatted = f"Plan capture failed: {type(error).__name__}: {error}"
            else:
                self.query_plan_formatted = buffer.getvalue().strip()
        return self

    def mark_degraded(self, error_class: str | None = None, **metrics: Any) -> None:
        self.degraded = True
        if error_class:
            self.error_class = error_class
        self.metrics.update(metrics)

    def set_metrics(self, **metrics: Any) -> None:
        self.metrics.update(metrics)

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        self.duration_sec = round(time.perf_counter() - self.started_at, 3)
        if exc is not None:
            self.status = "FAILED"
            self.stack_trace = "".join(traceback.format_exception(exc_type, exc, tb))
        elif self.degraded:
            self.status = "DEGRADED"
            self.stack_trace = (
                f"No exception was raised. Stage DEGRADED after {self.duration_sec}s "
                f"with error_class={self.error_class or 'SLA_BREACH'}."
            )
        else:
            self.status = "SUCCESS"
        metrics = dict(self.metrics)
        metrics.update(
            {
                "run_id": self.run_id,
                "scenario": self.scenario,
                "task_key": self.task_key,
                "duration_sec": self.duration_sec,
                "status": self.status,
                "error_class": self.error_class,
                "write_mode": self.write_mode,
            }
        )
        created_at = _now().isoformat()
        row = {
            "event_id": self.event_id,
            "run_id": self.run_id,
            "job_id": self.job_id,
            "orchestrator": self.orchestrator,
            "dag_id": self.dag_id,
            "task_key": self.task_key,
            "scenario": self.scenario,
            "notebook_path": self.notebook_path,
            "status": self.status,
            "query_plan_formatted": self.query_plan_formatted,
            "stack_trace": self.stack_trace,
            "raw_metrics_json": json.dumps(metrics, default=str, sort_keys=True),
            "created_at": created_at,
        }
        persist_row(self.spark, self.catalog, self.schema, "obs_bronze_runs", row, "obs_bronze_runs")
        return False


def write_incident(
    spark: Any,
    catalog: str,
    schema: str,
    incident_id: str,
    event_id: str,
    run_id: str,
    task_key: str,
    scenario: str,
    status: str,
    severity: str,
    business_criticality: str,
    duration_sec: float,
    row_count_in: int,
    row_count_out: int,
    rejected_row_count: int,
    top_key_skew_ratio: float,
    payout_exposure_usd: float,
    error_class: str,
    write_mode: str,
    sla_breached: bool,
) -> str:
    row = {
        "incident_id": incident_id,
        "event_id": event_id,
        "run_id": run_id,
        "task_key": task_key,
        "scenario": scenario,
        "status": status,
        "severity": severity,
        "business_criticality": business_criticality,
        "duration_sec": round(float(duration_sec), 3),
        "row_count_in": int(row_count_in),
        "row_count_out": int(row_count_out),
        "rejected_row_count": int(rejected_row_count),
        "top_key_skew_ratio": round(float(top_key_skew_ratio), 6),
        "payout_exposure_usd": round(float(payout_exposure_usd), 2),
        "error_class": error_class,
        "write_mode": write_mode,
        "sla_breached": bool(sla_breached),
        "created_at": _now().isoformat(),
    }
    return persist_row(spark, catalog, schema, "obs_silver_incidents", row, "obs_silver_incidents")


def write_reconciliation(
    spark: Any,
    catalog: str,
    schema: str,
    run_id: str,
    scenario: str,
    records_received: int,
    records_valid: int,
    records_rejected: int,
    duplicate_count: int,
    malformed_amount_count: int,
    invalid_provider_count: int,
    invalid_policy_count: int,
    rejection_rate: float,
    total_payout_usd: float,
) -> str:
    row = {
        "run_id": run_id,
        "scenario": scenario,
        "records_received": int(records_received),
        "records_valid": int(records_valid),
        "records_rejected": int(records_rejected),
        "duplicate_count": int(duplicate_count),
        "malformed_amount_count": int(malformed_amount_count),
        "invalid_provider_count": int(invalid_provider_count),
        "invalid_policy_count": int(invalid_policy_count),
        "rejection_rate": round(float(rejection_rate), 6),
        "total_payout_usd": round(float(total_payout_usd), 2),
        "created_at": _now().isoformat(),
    }
    return persist_row(
        spark,
        catalog,
        schema,
        "claims_reconciliation_report",
        row,
        "claims_reconciliation_report",
    )
