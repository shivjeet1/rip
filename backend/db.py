from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from .config import get_settings

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS claims_bronze (
    claim_id TEXT,
    policy_id TEXT,
    provider_id TEXT,
    claim_amount TEXT,
    claim_date TEXT,
    claim_status TEXT,
    diagnosis_code TEXT,
    ingested_at TEXT,
    run_id TEXT
);
CREATE INDEX IF NOT EXISTS idx_bronze_run ON claims_bronze(run_id);
CREATE TABLE IF NOT EXISTS claims_silver (
    claim_id TEXT,
    policy_id TEXT,
    provider_id TEXT,
    claim_date TEXT,
    claim_amount REAL,
    approved_payout REAL,
    claim_status TEXT,
    diagnosis_code TEXT,
    adjudicated_at TEXT,
    run_id TEXT
);
CREATE INDEX IF NOT EXISTS idx_silver_run ON claims_silver(run_id);
CREATE TABLE IF NOT EXISTS claims_quarantine (
    claim_id TEXT,
    run_id TEXT,
    rejection_reason TEXT,
    raw_payload TEXT,
    quarantined_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_quarantine_run ON claims_quarantine(run_id);
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
CREATE TABLE IF NOT EXISTS dim_provider (
    provider_id TEXT PRIMARY KEY,
    provider_name TEXT,
    network_tier TEXT,
    region TEXT
);
CREATE TABLE IF NOT EXISTS dim_policy_tier (
    policy_id TEXT PRIMARY KEY,
    tier_name TEXT,
    coverage_pct REAL,
    deductible REAL
);
CREATE TABLE IF NOT EXISTS dim_date (
    date_id INTEGER PRIMARY KEY,
    full_date TEXT,
    month INTEGER,
    quarter INTEGER,
    year INTEGER
);
CREATE TABLE IF NOT EXISTS fact_claims_payouts (
    batch_id TEXT,
    run_id TEXT,
    provider_id TEXT,
    date_id INTEGER,
    region TEXT,
    claim_count INTEGER,
    total_claimed_usd REAL,
    total_payout_usd REAL,
    created_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_fact_run ON fact_claims_payouts(run_id);
CREATE INDEX IF NOT EXISTS idx_fact_batch ON fact_claims_payouts(batch_id);
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
CREATE INDEX IF NOT EXISTS idx_bronze_runs_run ON obs_bronze_runs(run_id);
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
CREATE INDEX IF NOT EXISTS idx_incidents_created ON obs_silver_incidents(created_at);
CREATE TABLE IF NOT EXISTS agent_diagnoses (
    incident_id TEXT PRIMARY KEY,
    root_cause_category TEXT,
    summary TEXT,
    insurance_business_impact TEXT,
    payout_exposure_usd REAL,
    code_diff TEXT,
    retry_safety TEXT,
    retry_recommendation TEXT,
    model_used TEXT,
    created_at TEXT
);
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id TEXT PRIMARY KEY,
    scenario TEXT,
    triggered_by TEXT,
    status TEXT,
    started_at TEXT,
    finished_at TEXT,
    bronze_rows INTEGER,
    silver_rows INTEGER,
    gold_rows INTEGER,
    incident_id TEXT,
    message TEXT
);
CREATE TABLE IF NOT EXISTS warehouse_cache (
    cache_key TEXT PRIMARY KEY,
    payload_json TEXT,
    created_at TEXT
);
CREATE TABLE IF NOT EXISTS app_meta (
    key TEXT PRIMARY KEY,
    value TEXT
);
"""

ARTIFACT_TABLES = (
    "claims_bronze",
    "claims_silver",
    "claims_quarantine",
    "claims_reconciliation_report",
    "obs_bronze_runs",
    "obs_silver_incidents",
    "fact_claims_payouts",
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _safe_identifier(value: str) -> str:
    if not value.replace("_", "").isalnum():
        raise ValueError(f"Invalid SQLite identifier: {value}")
    return value


def connect() -> sqlite3.Connection:
    settings = get_settings()
    connection = sqlite3.connect(str(settings.local_cache_db), timeout=30)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=NORMAL")
    return connection


@contextmanager
def session() -> Iterator[sqlite3.Connection]:
    connection = connect()
    try:
        yield connection
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def init_db() -> Path:
    settings = get_settings()
    settings.local_cache_db.parent.mkdir(parents=True, exist_ok=True)
    with session() as connection:
        connection.executescript(SCHEMA_SQL)
    return settings.local_cache_db


def reset_local_data() -> None:
    init_db()
    with session() as connection:
        for table in ARTIFACT_TABLES + (
            "dim_provider",
            "dim_policy_tier",
            "dim_date",
            "agent_diagnoses",
            "pipeline_runs",
            "warehouse_cache",
            "app_meta",
        ):
            connection.execute(f"DELETE FROM {_safe_identifier(table)}")


def rows_to_dicts(rows: list[sqlite3.Row]) -> list[dict[str, Any]]:
    return [dict(row) for row in rows]


def insert_rows(
    connection: sqlite3.Connection,
    table: str,
    columns: list[str],
    rows: list[dict[str, Any]],
) -> None:
    if not rows:
        return
    safe_table = _safe_identifier(table)
    safe_columns = [_safe_identifier(column) for column in columns]
    placeholder = ", ".join("?" for _ in safe_columns)
    column_sql = ", ".join(safe_columns)
    sql = f"INSERT INTO {safe_table} ({column_sql}) VALUES ({placeholder})"
    values = [tuple(row.get(column) for column in safe_columns) for row in rows]
    connection.executemany(sql, values)


def upsert_rows(
    connection: sqlite3.Connection,
    table: str,
    columns: list[str],
    rows: list[dict[str, Any]],
) -> None:
    if not rows:
        return
    safe_table = _safe_identifier(table)
    safe_columns = [_safe_identifier(column) for column in columns]
    placeholder = ", ".join("?" for _ in safe_columns)
    column_sql = ", ".join(safe_columns)
    sql = f"INSERT OR REPLACE INTO {safe_table} ({column_sql}) VALUES ({placeholder})"
    values = [tuple(row.get(column) for column in safe_columns) for row in rows]
    connection.executemany(sql, values)


def clear_scenario_artifacts(connection: sqlite3.Connection, scenario: str) -> list[str]:
    run_rows = connection.execute(
        "SELECT run_id FROM pipeline_runs WHERE scenario = ?",
        (scenario,),
    ).fetchall()
    run_ids = [str(row["run_id"]) for row in run_rows]
    if not run_ids:
        return []
    placeholders = ", ".join("?" for _ in run_ids)
    incident_rows = connection.execute(
        f"SELECT incident_id FROM obs_silver_incidents WHERE run_id IN ({placeholders})",
        run_ids,
    ).fetchall()
    incident_ids = [str(row["incident_id"]) for row in incident_rows]
    if incident_ids:
        incident_placeholders = ", ".join("?" for _ in incident_ids)
        connection.execute(
            f"DELETE FROM agent_diagnoses WHERE incident_id IN ({incident_placeholders})",
            incident_ids,
        )
    for table in ARTIFACT_TABLES:
        connection.execute(
            f"DELETE FROM {_safe_identifier(table)} WHERE run_id IN ({placeholders})",
            run_ids,
        )
    return run_ids


def set_meta(key: str, value: str) -> None:
    with session() as connection:
        connection.execute(
            "INSERT OR REPLACE INTO app_meta (key, value) VALUES (?, ?)",
            (key, value),
        )


def get_meta(key: str, default: str | None = None) -> str | None:
    with session() as connection:
        row = connection.execute("SELECT value FROM app_meta WHERE key = ?", (key,)).fetchone()
    return str(row["value"]) if row else default


def get_cache(cache_key: str) -> dict[str, Any] | None:
    init_db()
    with session() as connection:
        row = connection.execute(
            "SELECT payload_json, created_at FROM warehouse_cache WHERE cache_key = ?",
            (cache_key,),
        ).fetchone()
    if not row:
        return None
    return {
        "payload": json.loads(row["payload_json"]),
        "created_at": row["created_at"],
    }


def set_cache(cache_key: str, payload: Any) -> str:
    created_at = utc_now()
    init_db()
    with session() as connection:
        connection.execute(
            "INSERT OR REPLACE INTO warehouse_cache (cache_key, payload_json, created_at) VALUES (?, ?, ?)",
            (cache_key, json.dumps(payload, default=str), created_at),
        )
    return created_at


def list_incidents() -> list[dict[str, Any]]:
    init_db()
    with session() as connection:
        rows = connection.execute(
            """
            SELECT
                i.incident_id,
                i.event_id,
                i.run_id,
                i.task_key,
                i.scenario,
                i.status,
                i.severity,
                i.business_criticality,
                i.duration_sec,
                i.row_count_in,
                i.row_count_out,
                i.rejected_row_count,
                i.top_key_skew_ratio,
                i.payout_exposure_usd,
                i.error_class,
                i.write_mode,
                i.sla_breached,
                i.created_at,
                CASE WHEN d.incident_id IS NULL THEN 0 ELSE 1 END AS diagnosed,
                d.root_cause_category,
                d.retry_safety,
                d.summary AS diagnosis_summary
            FROM obs_silver_incidents i
            LEFT JOIN agent_diagnoses d ON d.incident_id = i.incident_id
            ORDER BY
                CASE i.business_criticality
                    WHEN 'CRITICAL' THEN 0
                    WHEN 'HIGH' THEN 1
                    WHEN 'MEDIUM' THEN 2
                    ELSE 3
                END,
                i.created_at DESC
            """
        ).fetchall()
    incidents = rows_to_dicts(rows)
    for incident in incidents:
        incident["sla_breached"] = bool(incident["sla_breached"])
        incident["diagnosed"] = bool(incident["diagnosed"])
    return incidents


def get_incident(incident_id: str) -> dict[str, Any] | None:
    init_db()
    with session() as connection:
        row = connection.execute(
            "SELECT * FROM obs_silver_incidents WHERE incident_id = ?",
            (incident_id,),
        ).fetchone()
    if not row:
        return None
    incident = dict(row)
    incident["sla_breached"] = bool(incident["sla_breached"])
    return incident


def get_incident_with_diagnosis(incident_id: str) -> dict[str, Any] | None:
    incident = get_incident(incident_id)
    if not incident:
        return None
    diagnosis = get_diagnosis(incident_id)
    incident["diagnosed"] = diagnosis is not None
    incident["diagnosis"] = diagnosis
    return incident


def get_bronze_event(incident: dict[str, Any]) -> dict[str, Any] | None:
    init_db()
    with session() as connection:
        row = connection.execute(
            "SELECT * FROM obs_bronze_runs WHERE event_id = ?",
            (incident.get("event_id"),),
        ).fetchone()
        if not row:
            row = connection.execute(
                """
                SELECT * FROM obs_bronze_runs
                WHERE run_id = ? AND task_key = ?
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (incident.get("run_id"), incident.get("task_key")),
            ).fetchone()
    return dict(row) if row else None


def get_reconciliation(run_id: str) -> dict[str, Any] | None:
    init_db()
    with session() as connection:
        row = connection.execute(
            "SELECT * FROM claims_reconciliation_report WHERE run_id = ?",
            (run_id,),
        ).fetchone()
    return dict(row) if row else None


def get_reconciliation_for_incident(incident: dict[str, Any]) -> dict[str, Any] | None:
    return get_reconciliation(str(incident.get("run_id", "")))


def get_diagnosis(incident_id: str) -> dict[str, Any] | None:
    init_db()
    with session() as connection:
        row = connection.execute(
            "SELECT * FROM agent_diagnoses WHERE incident_id = ?",
            (incident_id,),
        ).fetchone()
    return dict(row) if row else None


def save_diagnosis(diagnosis: dict[str, Any]) -> None:
    with session() as connection:
        upsert_rows(
            connection,
            "agent_diagnoses",
            [
                "incident_id",
                "root_cause_category",
                "summary",
                "insurance_business_impact",
                "payout_exposure_usd",
                "code_diff",
                "retry_safety",
                "retry_recommendation",
                "model_used",
                "created_at",
            ],
            [diagnosis],
        )


def overview() -> dict[str, Any]:
    init_db()
    with session() as connection:
        total_runs = int(connection.execute("SELECT COUNT(*) AS n FROM pipeline_runs").fetchone()["n"])
        sla_breaches = int(
            connection.execute(
                "SELECT COUNT(*) AS n FROM obs_silver_incidents WHERE sla_breached = 1"
            ).fetchone()["n"]
        )
        exposure_row = connection.execute(
            """
            SELECT
                COALESCE(SUM(payout_exposure_usd), 0) AS total_exposure,
                COALESCE(SUM(CASE WHEN business_criticality = 'CRITICAL' THEN payout_exposure_usd ELSE 0 END), 0) AS critical_exposure
            FROM obs_silver_incidents
            WHERE business_criticality IN ('HIGH', 'CRITICAL')
            """
        ).fetchone()
        incident_count = int(
            connection.execute("SELECT COUNT(*) AS n FROM obs_silver_incidents").fetchone()["n"]
        )
        diagnosed_count = int(
            connection.execute("SELECT COUNT(*) AS n FROM agent_diagnoses").fetchone()["n"]
        )
        latest_recon = connection.execute(
            "SELECT * FROM claims_reconciliation_report ORDER BY created_at DESC, rowid DESC LIMIT 1"
        ).fetchone()
        latest_run = connection.execute(
            "SELECT * FROM pipeline_runs ORDER BY finished_at DESC, rowid DESC LIMIT 1"
        ).fetchone()
    reconciliation = dict(latest_recon) if latest_recon else None
    latest_run_row = dict(latest_run) if latest_run else None
    received = int(reconciliation["records_received"]) if reconciliation else 0
    valid = int(reconciliation["records_valid"]) if reconciliation else 0
    pass_rate = round((valid / received) * 100, 2) if received else 0.0
    return {
        "total_runs": total_runs,
        "sla_breaches": sla_breaches,
        "payout_exposure_usd": round(float(exposure_row["total_exposure"] or 0), 2),
        "critical_payout_exposure_usd": round(float(exposure_row["critical_exposure"] or 0), 2),
        "latest_reconciliation_rate": pass_rate,
        "latest_reconciliation": reconciliation,
        "cache_synced_at": get_meta("cache_synced_at"),
        "open_incidents": incident_count,
        "diagnosed_incidents": diagnosed_count,
        "latest_run": latest_run_row,
    }


def committed_fact_summary(run_id: str) -> dict[str, Any]:
    init_db()
    with session() as connection:
        row = connection.execute(
            """
            SELECT
                COUNT(*) AS fact_rows,
                COUNT(DISTINCT batch_id) AS batches,
                COALESCE(SUM(total_payout_usd), 0) AS payout_usd
            FROM fact_claims_payouts
            WHERE run_id = ?
            """,
            (run_id,),
        ).fetchone()
    return dict(row)


def recent_runs(limit: int = 10) -> list[dict[str, Any]]:
    init_db()
    with session() as connection:
        rows = connection.execute(
            "SELECT * FROM pipeline_runs ORDER BY finished_at DESC, rowid DESC LIMIT ?",
            (limit,),
        ).fetchall()
    return rows_to_dicts(rows)
