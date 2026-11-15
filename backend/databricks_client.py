from __future__ import annotations

import csv
import json
import re
import uuid
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from scripts.generate_raw_data import SCENARIOS, ensure_scenario_files

from . import db
from .config import get_settings


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _uuid(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


def _round_money(value: float) -> float:
    return round(float(value), 2)


def _safe_name(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_]+", value):
        raise ValueError(f"Unsafe Unity Catalog identifier: {value}")
    return value


def _read_claims(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if stripped:
                records.append(json.loads(stripped))
    return records


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _strict_amount(raw: Any) -> float | None:
    text = str(raw).strip()
    if not text:
        return None
    try:
        value = float(text)
    except ValueError:
        return None
    if value <= 0:
        return None
    return value


def _loose_amount(raw: Any) -> float:
    cleaned = re.sub(r"[^0-9.]", "", str(raw))
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def _parse_date(raw: Any) -> date | None:
    text = str(raw).strip()
    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError:
        return None


def _classify_claims(
    records: list[dict[str, Any]],
    providers: dict[str, dict[str, str]],
    policies: dict[str, dict[str, str]],
    run_id: str,
    now: str,
) -> dict[str, Any]:
    seen: set[str] = set()
    reason_counts: Counter[str] = Counter()
    provider_counts: Counter[str] = Counter()
    bronze_rows: list[dict[str, Any]] = []
    silver_rows: list[dict[str, Any]] = []
    quarantine_rows: list[dict[str, Any]] = []
    delayed_total = 0.0
    rejected_risk = 0.0
    for record in records:
        claim_id = str(record.get("claim_id", "")).strip()
        policy_id = str(record.get("policy_id", "")).strip()
        provider_id = str(record.get("provider_id", "")).strip()
        claim_amount_raw = record.get("claim_amount", "")
        claim_date_raw = record.get("claim_date", "")
        provider_counts[provider_id] += 1
        bronze_rows.append(
            {
                "claim_id": claim_id,
                "policy_id": policy_id,
                "provider_id": provider_id,
                "claim_amount": str(claim_amount_raw),
                "claim_date": str(claim_date_raw),
                "claim_status": str(record.get("claim_status", "")),
                "diagnosis_code": str(record.get("diagnosis_code", "")),
                "ingested_at": now,
                "run_id": run_id,
            }
        )
        amount = _strict_amount(claim_amount_raw)
        parsed_date = _parse_date(claim_date_raw)
        reason: str | None = None
        if claim_id in seen:
            reason = "DUPLICATE_CLAIM_ID"
        else:
            seen.add(claim_id)
            if amount is None:
                reason = "MALFORMED_AMOUNT"
            elif provider_id not in providers:
                reason = "INVALID_PROVIDER_ID"
            elif policy_id not in policies:
                reason = "INVALID_POLICY_ID"
            elif parsed_date is None:
                reason = "INVALID_DATE"
        if reason is not None:
            reason_counts[reason] += 1
            if reason != "DUPLICATE_CLAIM_ID":
                rejected_risk += _loose_amount(claim_amount_raw)
            quarantine_rows.append(
                {
                    "claim_id": claim_id,
                    "run_id": run_id,
                    "rejection_reason": reason,
                    "raw_payload": json.dumps(record, ensure_ascii=False, separators=(",", ":")),
                    "quarantined_at": now,
                }
            )
            continue
        coverage = float(policies[policy_id]["coverage_pct"])
        deductible = float(policies[policy_id]["deductible"])
        approved_payout = _round_money(max(0.0, float(amount) * coverage - deductible))
        if provider_id == "HOSP_999":
            delayed_total += float(amount)
        silver_rows.append(
            {
                "claim_id": claim_id,
                "policy_id": policy_id,
                "provider_id": provider_id,
                "claim_date": str(parsed_date.isoformat()),
                "claim_amount": _round_money(float(amount)),
                "approved_payout": approved_payout,
                "claim_status": str(record.get("claim_status", "")),
                "diagnosis_code": str(record.get("diagnosis_code", "")),
                "adjudicated_at": now,
                "run_id": run_id,
            }
        )
    received = len(records)
    valid = len(silver_rows)
    rejected = len(quarantine_rows)
    rejection_rate = round(rejected / received, 6) if received else 0.0
    top_provider_count = max(provider_counts.values()) if provider_counts else 0
    skew_ratio = round(top_provider_count / received, 6) if received else 0.0
    total_payout = _round_money(sum(row["approved_payout"] for row in silver_rows))
    return {
        "bronze_rows": bronze_rows,
        "silver_rows": silver_rows,
        "quarantine_rows": quarantine_rows,
        "reason_counts": dict(reason_counts),
        "records_received": received,
        "records_valid": valid,
        "records_rejected": rejected,
        "rejection_rate": rejection_rate,
        "top_key_skew_ratio": skew_ratio,
        "total_payout_usd": total_payout,
        "delayed_payout_usd": _round_money(delayed_total),
        "rejected_risk_usd": _round_money(rejected_risk),
        "top_provider_id": provider_counts.most_common(1)[0][0] if provider_counts else "",
    }


def _formatted_plan(task_key: str, scenario: str, metrics: dict[str, Any]) -> str:
    if task_key.startswith("01"):
        return "\n".join(
            [
                "== Physical Plan ==",
                "AdaptiveSparkPlan isFinalPlan=true",
                "+- == Final Plan ==",
                "   +- * ColumnarToRow",
                "      +- FileScan json [claim_amount#12,claim_date#13,claim_id#14,claim_status#15,diagnosis_code#16,policy_id#17,provider_id#18]",
                f"         Batched: true, DataFilters: [], Format: JSON, Location: {scenario} raw landing volume, PartitionFilters: [], PushedFilters: [], ReadSchema: struct<claim_amount:string,claim_date:string,claim_id:string,claim_status:string,diagnosis_code:string,policy_id:string,provider_id:string>",
                "   +- * Project [cast(claim_id as string) AS claim_id, cast(policy_id as string) AS policy_id, cast(provider_id as string) AS provider_id, run_id]",
                "      +- * Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=27]",
                f"         +- * LocalTableScan [records_received={metrics.get('records_received', 0)}]",
            ]
        )
    if task_key.startswith("02"):
        return "\n".join(
            [
                "== Physical Plan ==",
                "AdaptiveSparkPlan isFinalPlan=true",
                "+- == Final Plan ==",
                "   * Project [claim_id#41, policy_id#42, provider_id#43, claim_date#44, claim_amount_numeric#45, approved_payout#46]",
                "   +- * SortMergeJoin [provider_id#43], [provider_id#91], LeftOuter, (provider_id#43 = provider_id#91)",
                "      :- * Sort [provider_id#43 ASC NULLS FIRST], false, 0",
                "      :  +- Exchange hashpartitioning(provider_id#43, 200), ENSURE_REQUIREMENTS, [plan_id=118]",
                "      :     +- * Filter (isnotnull(provider_id#43) AND isnotnull(claim_amount_numeric#45))",
                "      :        +- * ColumnarToRow",
                "      :           +- FileScan delta [claim_id#41, policy_id#42, provider_id#43, claim_date#44, claim_amount#12]",
                f"      :              PartitionFilters: [run_id#47 = {metrics.get('run_id', '')}]",
                "      +- * Sort [provider_id#91 ASC NULLS FIRST], false, 0",
                "         +- Exchange hashpartitioning(provider_id#91, 200), ENSURE_REQUIREMENTS, [plan_id=121]",
                "            +- * ColumnarToRow",
                "               +- FileScan delta [provider_id#91, provider_name#92, network_tier#93, region#94]",
                f"   Partition skew observed on provider_id: max_partition_share={metrics.get('top_key_skew_ratio', 0.0)}",
                f"   Quality gate rejection_rate: {metrics.get('rejection_rate', 0.0)}",
            ]
        )
    return "\n".join(
        [
            "== Physical Plan ==",
            "AdaptiveSparkPlan isFinalPlan=true",
            "+- == Final Plan ==",
            "   +- * ColumnarToRow",
            "      +- WrittenTable workspace.claims_ops.fact_claims_payouts",
            "         +- * HashAggregate(keys=[provider_id#43, claim_date#44, region#94], functions=[count(1), sum(claim_amount#45), sum(approved_payout#46)])",
            "            +- Exchange hashpartitioning(provider_id#43, claim_date#44, region#94, 200), ENSURE_REQUIREMENTS",
            "               +- * Project [provider_id#43, claim_date#44, region#94, claim_amount#45, approved_payout#46]",
            "                  +- * SortMergeJoin [provider_id#43], [provider_id#91], Inner",
            "                     :- * Filter isnotnull(provider_id#43)",
            "                     :  +- * ColumnarToRow",
            "                     :     +- FileScan delta [claim_id#41, provider_id#43, claim_date#44, claim_amount#45, approved_payout#46]",
            "                     +- Exchange hashpartitioning(provider_id#91, 200), ENSURE_REQUIREMENTS",
            "                        +- * ColumnarToRow",
            "                           +- FileScan delta [provider_id#91, region#94]",
            f"   write_mode: {metrics.get('write_mode', 'overwrite')}",
            f"   payout_exposure_usd: {metrics.get('payout_exposure_usd', 0.0)}",
        ]
    )


def _stack_trace(task_key: str, scenario: str, metrics: dict[str, Any]) -> str:
    if scenario == "B" and task_key.startswith("02"):
        return "\n".join(
            [
                "Traceback (most recent call last):",
                '  File "/Workspace/claims_ops/databricks_notebooks/02_adjudicate_claims.py", line 249, in <module>',
                "    enforce_quality_gate(rejection_rate, MAX_REJECTION_RATE)",
                '  File "/Workspace/claims_ops/databricks_notebooks/02_adjudicate_claims.py", line 207, in enforce_quality_gate',
                "    raise ValueError(",
                f"ValueError: QUALITY_GATE_FAILED: rejection_rate={metrics.get('rejection_rate', 0.0):.4f} exceeded threshold=0.1500",
            ]
        )
    if scenario == "C" and task_key.startswith("03"):
        return "\n".join(
            [
                "Traceback (most recent call last):",
                '  File "/Workspace/claims_ops/databricks_notebooks/03_generate_payout_batch.py", line 188, in <module>',
                "    write_region_batch(batch_one, fact_table, run_id, write_mode=\"append\")",
                '  File "/Workspace/claims_ops/databricks_notebooks/03_generate_payout_batch.py", line 142, in write_region_batch',
                "    raise RuntimeError(\"Downstream clearinghouse ledger timeout\")",
                "RuntimeError: Downstream clearinghouse ledger timeout",
            ]
        )
    if scenario == "A" and task_key.startswith("02"):
        return (
            "No Python exception was raised. Stage DEGRADED after 142.4s while the unsalted "
            "provider_id join concentrated 70% of partitions on HOSP_999."
        )
    return ""


def _build_fact_rows(
    silver_rows: list[dict[str, Any]],
    providers: dict[str, dict[str, str]],
    run_id: str,
    created_at: str,
) -> list[dict[str, Any]]:
    aggregates: dict[tuple[str, str], list[float]] = defaultdict(lambda: [0, 0.0, 0.0])
    for row in silver_rows:
        key = (str(row["provider_id"]), str(row["claim_date"]))
        aggregates[key][0] += 1
        aggregates[key][1] += float(row["claim_amount"])
        aggregates[key][2] += float(row["approved_payout"])
    token = run_id.split("_")[-1]
    fact_rows: list[dict[str, Any]] = []
    for (provider_id, claim_date), values in aggregates.items():
        region = providers.get(provider_id, {}).get("region", "UNKNOWN")
        fact_rows.append(
            {
                "batch_id": f"batch_{token}_01",
                "run_id": run_id,
                "provider_id": provider_id,
                "date_id": int(claim_date.replace("-", "")),
                "region": region,
                "claim_count": int(values[0]),
                "total_claimed_usd": _round_money(values[1]),
                "total_payout_usd": _round_money(values[2]),
                "created_at": created_at,
            }
        )
    return fact_rows


def _dim_date_rows(silver_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    unique_dates = sorted({str(row["claim_date"]) for row in silver_rows})
    rows: list[dict[str, Any]] = []
    for text in unique_dates:
        parsed = datetime.strptime(text, "%Y-%m-%d").date()
        rows.append(
            {
                "date_id": int(text.replace("-", "")),
                "full_date": text,
                "month": parsed.month,
                "quarter": ((parsed.month - 1) // 3) + 1,
                "year": parsed.year,
            }
        )
    return rows


def _make_event(
    run_id: str,
    scenario: str,
    task_key: str,
    notebook_path: str,
    status: str,
    plan: str,
    stack_trace: str,
    metrics: dict[str, Any],
    created_at: str,
) -> dict[str, Any]:
    settings = get_settings()
    event_id = _uuid("evt")
    return {
        "event_id": event_id,
        "run_id": run_id,
        "job_id": str(settings.databricks_job_id),
        "orchestrator": "airflow",
        "dag_id": "insurance_claims_pipeline",
        "task_key": task_key,
        "scenario": scenario,
        "notebook_path": notebook_path,
        "status": status,
        "query_plan_formatted": plan,
        "stack_trace": stack_trace,
        "raw_metrics_json": json.dumps(metrics, sort_keys=True),
        "created_at": created_at,
    }


def run_mock_scenario(scenario: str) -> dict[str, Any]:
    if scenario not in SCENARIOS:
        raise ValueError(f"Unsupported scenario: {scenario}")
    settings = get_settings()
    ensure_scenario_files(scenario, settings.raw_data_dir)
    scenario_root = settings.raw_data_dir / scenario
    records = _read_claims(scenario_root / "claims_raw.json")
    providers_list = _read_csv(scenario_root / "providers.csv")
    policies_list = _read_csv(scenario_root / "policy_tiers.csv")
    providers = {row["provider_id"]: row for row in providers_list}
    policies = {row["policy_id"]: row for row in policies_list}
    now = utc_now()
    run_id = _uuid(f"run_{scenario.lower()}")
    classified = _classify_claims(records, providers, policies, run_id, now)
    rejection_rate = float(classified["rejection_rate"])
    skew_ratio = float(classified["top_key_skew_ratio"])
    quality_failed = rejection_rate > 0.15
    degraded = skew_ratio >= 0.65
    stage_two_status = "FAILED" if quality_failed else "DEGRADED" if degraded else "SUCCESS"
    stage_one_status = "DEGRADED" if degraded else "SUCCESS"
    silver_rows = [] if quality_failed else classified["silver_rows"]
    fact_rows = [] if quality_failed else _build_fact_rows(
        silver_rows,
        providers,
        run_id,
        now,
    )
    incident_id: str | None = None
    incident_row: dict[str, Any] | None = None
    committed_rows = fact_rows
    uncommitted_rows: list[dict[str, Any]] = []
    payout_exposure = 0.0
    scenario_status = stage_two_status
    if scenario == "A":
        payout_exposure = float(classified["delayed_payout_usd"])
        scenario_status = "DEGRADED"
    elif scenario == "B":
        payout_exposure = float(classified["rejected_risk_usd"])
        scenario_status = "FAILED"
    elif scenario == "NORMAL":
        scenario_status = "SUCCESS"
    if scenario == "C":
        regions = sorted({str(row["region"]) for row in fact_rows})
        split_index = max(1, len(regions) // 2)
        committed_regions = set(regions[:split_index])
        for row in fact_rows:
            row["batch_id"] = (
                f"batch_{run_id.split('_')[-1]}_01"
                if row["region"] in committed_regions
                else f"batch_{run_id.split('_')[-1]}_02"
            )
        committed_rows = [row for row in fact_rows if row["region"] in committed_regions]
        uncommitted_rows = [row for row in fact_rows if row["region"] not in committed_regions]
        payout_exposure = _round_money(sum(float(row["total_payout_usd"]) for row in committed_rows))
        scenario_status = "FAILED"
    stage_one_metrics = {
        "run_id": run_id,
        "scenario": scenario,
        "records_received": classified["records_received"],
        "top_provider_id": classified["top_provider_id"],
        "top_key_skew_ratio": skew_ratio,
        "write_mode": "overwrite",
    }
    stage_two_metrics = {
        "run_id": run_id,
        "scenario": scenario,
        "records_received": classified["records_received"],
        "records_valid": classified["records_valid"],
        "records_rejected": classified["records_rejected"],
        "rejection_rate": rejection_rate,
        "top_key_skew_ratio": skew_ratio,
        "write_mode": "overwrite",
    }
    stage_three_metrics = {
        "run_id": run_id,
        "scenario": scenario,
        "fact_rows": len(fact_rows),
        "committed_rows": len(committed_rows),
        "payout_exposure_usd": payout_exposure,
        "write_mode": "append_non_idempotent" if scenario == "C" else "overwrite",
    }
    event_one = _make_event(
        run_id,
        scenario,
        "01_ingest_raw_claims",
        "databricks_notebooks/01_ingest_raw_claims.py",
        stage_one_status,
        _formatted_plan("01_ingest_raw_claims", scenario, stage_one_metrics),
        _stack_trace("01_ingest_raw_claims", scenario, stage_two_metrics),
        stage_one_metrics,
        now,
    )
    event_two = _make_event(
        run_id,
        scenario,
        "02_adjudicate_claims",
        "databricks_notebooks/02_adjudicate_claims.py",
        stage_two_status,
        _formatted_plan("02_adjudicate_claims", scenario, stage_two_metrics),
        _stack_trace("02_adjudicate_claims", scenario, stage_two_metrics),
        stage_two_metrics,
        now,
    )
    events = [event_one, event_two]
    if not quality_failed:
        event_three = _make_event(
            run_id,
            scenario,
            "03_generate_payout_batch",
            "databricks_notebooks/03_generate_payout_batch.py",
            "FAILED" if scenario == "C" else "SUCCESS",
            _formatted_plan("03_generate_payout_batch", scenario, stage_three_metrics),
            _stack_trace("03_generate_payout_batch", scenario, stage_three_metrics),
            stage_three_metrics,
            now,
        )
        events.append(event_three)
    incident_event_id: str | None = None
    if scenario == "A":
        incident_event_id = event_two["event_id"]
        incident_id = _uuid("inc_a")
        incident_row = {
            "incident_id": incident_id,
            "event_id": incident_event_id,
            "run_id": run_id,
            "task_key": "02_adjudicate_claims",
            "scenario": scenario,
            "status": "DEGRADED",
            "severity": "HIGH",
            "business_criticality": "HIGH",
            "duration_sec": 142.4,
            "row_count_in": classified["records_received"],
            "row_count_out": classified["records_valid"],
            "rejected_row_count": classified["records_rejected"],
            "top_key_skew_ratio": skew_ratio,
            "payout_exposure_usd": payout_exposure,
            "error_class": "DATA_SKEW",
            "write_mode": "overwrite",
            "sla_breached": 1,
            "created_at": now,
        }
    elif scenario == "B":
        incident_event_id = event_two["event_id"]
        incident_id = _uuid("inc_b")
        incident_row = {
            "incident_id": incident_id,
            "event_id": incident_event_id,
            "run_id": run_id,
            "task_key": "02_adjudicate_claims",
            "scenario": scenario,
            "status": "FAILED",
            "severity": "CRITICAL",
            "business_criticality": "HIGH",
            "duration_sec": 88.4,
            "row_count_in": classified["records_received"],
            "row_count_out": 0,
            "rejected_row_count": classified["records_rejected"],
            "top_key_skew_ratio": skew_ratio,
            "payout_exposure_usd": payout_exposure,
            "error_class": "SCHEMA_DRIFT",
            "write_mode": "overwrite",
            "sla_breached": 1,
            "created_at": now,
        }
    elif scenario == "C":
        incident_event_id = event_three["event_id"]
        incident_id = _uuid("inc_c")
        incident_row = {
            "incident_id": incident_id,
            "event_id": incident_event_id,
            "run_id": run_id,
            "task_key": "03_generate_payout_batch",
            "scenario": scenario,
            "status": "FAILED",
            "severity": "CRITICAL",
            "business_criticality": "CRITICAL",
            "duration_sec": 96.8,
            "row_count_in": len(fact_rows),
            "row_count_out": len(committed_rows),
            "rejected_row_count": 0,
            "top_key_skew_ratio": 0.18,
            "payout_exposure_usd": payout_exposure,
            "error_class": "PARTIAL_PAYOUT_WRITE",
            "write_mode": "append_non_idempotent",
            "sla_breached": 1,
            "created_at": now,
        }
    reconciliation = {
        "run_id": run_id,
        "scenario": scenario,
        "records_received": classified["records_received"],
        "records_valid": classified["records_valid"],
        "records_rejected": classified["records_rejected"],
        "duplicate_count": classified["reason_counts"].get("DUPLICATE_CLAIM_ID", 0),
        "malformed_amount_count": classified["reason_counts"].get("MALFORMED_AMOUNT", 0),
        "invalid_provider_count": classified["reason_counts"].get("INVALID_PROVIDER_ID", 0),
        "invalid_policy_count": classified["reason_counts"].get("INVALID_POLICY_ID", 0),
        "rejection_rate": rejection_rate,
        "total_payout_usd": classified["total_payout_usd"],
        "created_at": now,
    }
    dim_dates = _dim_date_rows(silver_rows)
    db.init_db()
    with db.session() as connection:
        db.clear_scenario_artifacts(connection, scenario)
        db.upsert_rows(
            connection,
            "dim_provider",
            ["provider_id", "provider_name", "network_tier", "region"],
            providers_list,
        )
        db.upsert_rows(
            connection,
            "dim_policy_tier",
            ["policy_id", "tier_name", "coverage_pct", "deductible"],
            [
                {
                    "policy_id": row["policy_id"],
                    "tier_name": row["tier_name"],
                    "coverage_pct": float(row["coverage_pct"]),
                    "deductible": float(row["deductible"]),
                }
                for row in policies_list
            ],
        )
        db.upsert_rows(
            connection,
            "dim_date",
            ["date_id", "full_date", "month", "quarter", "year"],
            dim_dates,
        )
        db.insert_rows(
            connection,
            "claims_bronze",
            [
                "claim_id",
                "policy_id",
                "provider_id",
                "claim_amount",
                "claim_date",
                "claim_status",
                "diagnosis_code",
                "ingested_at",
                "run_id",
            ],
            classified["bronze_rows"],
        )
        db.insert_rows(
            connection,
            "claims_silver",
            [
                "claim_id",
                "policy_id",
                "provider_id",
                "claim_date",
                "claim_amount",
                "approved_payout",
                "claim_status",
                "diagnosis_code",
                "adjudicated_at",
                "run_id",
            ],
            silver_rows,
        )
        db.insert_rows(
            connection,
            "claims_quarantine",
            ["claim_id", "run_id", "rejection_reason", "raw_payload", "quarantined_at"],
            classified["quarantine_rows"],
        )
        db.insert_rows(
            connection,
            "fact_claims_payouts",
            [
                "batch_id",
                "run_id",
                "provider_id",
                "date_id",
                "region",
                "claim_count",
                "total_claimed_usd",
                "total_payout_usd",
                "created_at",
            ],
            committed_rows,
        )
        db.insert_rows(
            connection,
            "obs_bronze_runs",
            [
                "event_id",
                "run_id",
                "job_id",
                "orchestrator",
                "dag_id",
                "task_key",
                "scenario",
                "notebook_path",
                "status",
                "query_plan_formatted",
                "stack_trace",
                "raw_metrics_json",
                "created_at",
            ],
            events,
        )
        if incident_row:
            db.insert_rows(
                connection,
                "obs_silver_incidents",
                [
                    "incident_id",
                    "event_id",
                    "run_id",
                    "task_key",
                    "scenario",
                    "status",
                    "severity",
                    "business_criticality",
                    "duration_sec",
                    "row_count_in",
                    "row_count_out",
                    "rejected_row_count",
                    "top_key_skew_ratio",
                    "payout_exposure_usd",
                    "error_class",
                    "write_mode",
                    "sla_breached",
                    "created_at",
                ],
                [incident_row],
            )
        db.insert_rows(
            connection,
            "claims_reconciliation_report",
            [
                "run_id",
                "scenario",
                "records_received",
                "records_valid",
                "records_rejected",
                "duplicate_count",
                "malformed_amount_count",
                "invalid_provider_count",
                "invalid_policy_count",
                "rejection_rate",
                "total_payout_usd",
                "created_at",
            ],
            [reconciliation],
        )
        db.upsert_rows(
            connection,
            "pipeline_runs",
            [
                "run_id",
                "scenario",
                "triggered_by",
                "status",
                "started_at",
                "finished_at",
                "bronze_rows",
                "silver_rows",
                "gold_rows",
                "incident_id",
                "message",
            ],
            [
                {
                    "run_id": run_id,
                    "scenario": scenario,
                    "triggered_by": "local_mock_executor",
                    "status": scenario_status,
                    "started_at": now,
                    "finished_at": utc_now(),
                    "bronze_rows": classified["records_received"],
                    "silver_rows": len(silver_rows),
                    "gold_rows": len(committed_rows),
                    "incident_id": incident_id,
                    "message": _run_message(scenario, classified, payout_exposure, len(committed_rows)),
                }
            ],
        )
    db.set_meta("cache_synced_at", now)
    return {
        "run_id": run_id,
        "scenario": scenario,
        "status": scenario_status,
        "incident_id": incident_id,
        "incident": incident_row,
        "reconciliation": reconciliation,
        "fact_rows_committed": len(committed_rows),
        "fact_rows_uncommitted": len(uncommitted_rows),
        "payout_exposure_usd": payout_exposure,
        "mode": settings.mode_label,
        "message": _run_message(scenario, classified, payout_exposure, len(committed_rows)),
    }


def _run_message(
    scenario: str,
    classified: dict[str, Any],
    payout_exposure: float,
    committed_rows: int,
) -> str:
    if scenario == "NORMAL":
        return (
            f"Pipeline completed with {classified['records_valid']} valid claims and "
            f"a {classified['rejection_rate'] * 100:.2f}% rejection rate."
        )
    if scenario == "A":
        return (
            f"Provider skew reached {classified['top_key_skew_ratio'] * 100:.1f}% on HOSP_999; "
            f"${payout_exposure:,.2f} of claims are delayed."
        )
    if scenario == "B":
        return (
            f"Quality gate rejected {classified['records_rejected']} records at a "
            f"{classified['rejection_rate'] * 100:.2f}% rejection rate."
        )
    return (
        f"Payout batch committed {committed_rows} fact rows and ${payout_exposure:,.2f} "
        "before the clearinghouse timeout."
    )


def _cache_is_fresh(created_at: str, ttl_seconds: int) -> bool:
    try:
        timestamp = datetime.fromisoformat(created_at)
    except ValueError:
        return False
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - timestamp).total_seconds() <= ttl_seconds


def _serialize_value(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, bool):
        return int(value)
    if hasattr(value, "to_eng_string"):
        return float(value)
    return value


def _query_warehouse(cache_key: str, sql_text: str, force: bool = False) -> list[dict[str, Any]]:
    settings = get_settings()
    if not force:
        cached = db.get_cache(cache_key)
        if cached and _cache_is_fresh(str(cached["created_at"]), settings.cache_ttl_seconds):
            return list(cached["payload"])
    if not settings.databricks_host or not settings.databricks_token or not settings.sql_http_path:
        raise RuntimeError("Databricks SQL Warehouse credentials are incomplete")
    try:
        from databricks import sql
    except ImportError as error:
        raise RuntimeError("databricks-sql-connector is not installed") from error
    connection = sql.connect(
        server_hostname=settings.sql_server_hostname,
        http_path=settings.sql_http_path,
        access_token=settings.databricks_token,
    )
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql_text)
            columns = [description[0] for description in cursor.description]
            raw_rows = cursor.fetchall()
    finally:
        connection.close()
    payload = [
        {column: _serialize_value(value) for column, value in zip(columns, row)}
        for row in raw_rows
    ]
    db.set_cache(cache_key, payload)
    return payload


def _live_table(name: str) -> str:
    settings = get_settings()
    catalog = _safe_name(settings.databricks_catalog)
    schema = _safe_name(settings.databricks_schema)
    return f"{catalog}.{schema}.{_safe_name(name)}"


def sync_observability(force: bool = False) -> dict[str, Any]:
    settings = get_settings()
    db.init_db()
    if settings.mock_mode:
        synced_at = utc_now()
        db.set_meta("cache_synced_at", synced_at)
        return {"mode": settings.mode_label, "synced_at": synced_at, "rows": 0, "cached": True}
    bronze_sql = (
        "SELECT event_id, run_id, job_id, orchestrator, dag_id, task_key, scenario, notebook_path, "
        "status, query_plan_formatted, stack_trace, raw_metrics_json, created_at "
        f"FROM {_live_table('obs_bronze_runs')} ORDER BY created_at DESC LIMIT 500"
    )
    incident_sql = (
        "SELECT incident_id, event_id, run_id, task_key, scenario, status, severity, business_criticality, "
        "duration_sec, row_count_in, row_count_out, rejected_row_count, top_key_skew_ratio, "
        "payout_exposure_usd, error_class, write_mode, sla_breached, created_at "
        f"FROM {_live_table('obs_silver_incidents')} ORDER BY created_at DESC LIMIT 500"
    )
    reconciliation_sql = (
        "SELECT run_id, scenario, records_received, records_valid, records_rejected, duplicate_count, "
        "malformed_amount_count, invalid_provider_count, invalid_policy_count, rejection_rate, "
        "total_payout_usd, created_at "
        f"FROM {_live_table('claims_reconciliation_report')} ORDER BY created_at DESC LIMIT 500"
    )
    bronze_rows = _query_warehouse("obs_bronze_runs", bronze_sql, force)
    incident_rows = _query_warehouse("obs_silver_incidents", incident_sql, force)
    reconciliation_rows = _query_warehouse("claims_reconciliation_report", reconciliation_sql, force)
    synced_at = utc_now()
    with db.session() as connection:
        db.upsert_rows(
            connection,
            "obs_bronze_runs",
            [
                "event_id",
                "run_id",
                "job_id",
                "orchestrator",
                "dag_id",
                "task_key",
                "scenario",
                "notebook_path",
                "status",
                "query_plan_formatted",
                "stack_trace",
                "raw_metrics_json",
                "created_at",
            ],
            bronze_rows,
        )
        for row in incident_rows:
            row["sla_breached"] = 1 if row.get("sla_breached") else 0
        db.upsert_rows(
            connection,
            "obs_silver_incidents",
            [
                "incident_id",
                "event_id",
                "run_id",
                "task_key",
                "scenario",
                "status",
                "severity",
                "business_criticality",
                "duration_sec",
                "row_count_in",
                "row_count_out",
                "rejected_row_count",
                "top_key_skew_ratio",
                "payout_exposure_usd",
                "error_class",
                "write_mode",
                "sla_breached",
                "created_at",
            ],
            incident_rows,
        )
        db.upsert_rows(
            connection,
            "claims_reconciliation_report",
            [
                "run_id",
                "scenario",
                "records_received",
                "records_valid",
                "records_rejected",
                "duplicate_count",
                "malformed_amount_count",
                "invalid_provider_count",
                "invalid_policy_count",
                "rejection_rate",
                "total_payout_usd",
                "created_at",
            ],
            reconciliation_rows,
        )
        incidents_by_run = {str(row["run_id"]): row for row in incident_rows}
        pipeline_rows: list[dict[str, Any]] = []
        for reconciliation in reconciliation_rows:
            run_id = str(reconciliation["run_id"])
            existing = connection.execute(
                "SELECT run_id FROM pipeline_runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            if existing:
                continue
            incident = incidents_by_run.get(run_id)
            pipeline_rows.append(
                {
                    "run_id": run_id,
                    "scenario": reconciliation.get("scenario"),
                    "triggered_by": "databricks_warehouse_sync",
                    "status": (incident or {}).get("status", "SUCCESS"),
                    "started_at": reconciliation.get("created_at"),
                    "finished_at": reconciliation.get("created_at"),
                    "bronze_rows": reconciliation.get("records_received", 0),
                    "silver_rows": reconciliation.get("records_valid", 0),
                    "gold_rows": 0,
                    "incident_id": (incident or {}).get("incident_id"),
                    "message": "Synchronized from Unity Catalog observability tables.",
                }
            )
        db.upsert_rows(
            connection,
            "pipeline_runs",
            [
                "run_id",
                "scenario",
                "triggered_by",
                "status",
                "started_at",
                "finished_at",
                "bronze_rows",
                "silver_rows",
                "gold_rows",
                "incident_id",
                "message",
            ],
            pipeline_rows,
        )
    db.set_meta("cache_synced_at", synced_at)
    return {
        "mode": settings.mode_label,
        "synced_at": synced_at,
        "rows": len(bronze_rows) + len(incident_rows) + len(reconciliation_rows),
        "cached": False,
    }


def _trigger_live_job(scenario: str) -> dict[str, Any]:
    settings = get_settings()
    if not settings.databricks_host or not settings.databricks_token:
        raise RuntimeError("Databricks host and token are required in live mode")
    try:
        job_id = int(settings.databricks_job_id)
    except ValueError as error:
        raise RuntimeError("DATABRICKS_JOB_ID must be a numeric job id") from error
    if job_id <= 0:
        raise RuntimeError("DATABRICKS_JOB_ID is not configured")
    try:
        from databricks.sdk import WorkspaceClient
    except ImportError as error:
        raise RuntimeError("databricks-sdk is not installed") from error
    client = WorkspaceClient(host=settings.databricks_host, token=settings.databricks_token)
    run_id = _uuid(f"dbx_{scenario.lower()}")
    response = client.jobs.run_now(
        job_id=job_id,
        notebook_params={
            "scenario": scenario,
            "run_id": run_id,
            "repo_root": "/Workspace/claims_ops",
        },
    )
    return {
        "run_id": run_id,
        "scenario": scenario,
        "status": "SUBMITTED",
        "incident_id": None,
        "incident": None,
        "reconciliation": None,
        "fact_rows_committed": 0,
        "fact_rows_uncommitted": 0,
        "payout_exposure_usd": 0.0,
        "mode": settings.mode_label,
        "message": f"Databricks job run submitted with run id {response.run_id}.",
        "databricks_run_id": str(response.run_id),
    }


def run_scenario(scenario: str) -> dict[str, Any]:
    normalized = scenario.strip().upper()
    if normalized not in SCENARIOS:
        raise ValueError(f"Unsupported scenario: {scenario}")
    settings = get_settings()
    if settings.mock_mode:
        return run_mock_scenario(normalized)
    return _trigger_live_job(normalized)


def fetch_incidents(refresh: bool = False) -> list[dict[str, Any]]:
    settings = get_settings()
    if refresh or not settings.mock_mode:
        sync_observability(force=refresh)
    return db.list_incidents()


def health_payload() -> dict[str, Any]:
    settings = get_settings()
    db.init_db()
    return {
        "status": "ok",
        "mode": settings.mode_label,
        "mock_mode": settings.mock_mode,
        "cache_synced_at": db.get_meta("cache_synced_at"),
        "cache_ttl_seconds": settings.cache_ttl_seconds,
        "llm_provider": settings.llm_provider,
        "llm_model": settings.llm_model,
        "llm_ready": settings.llm_ready,
        "llm_label": settings.llm_label,
        "fallback_analyzer": True,
        "local_cache_db": str(settings.local_cache_db),
        "namespace": f"{settings.databricks_catalog}.{settings.databricks_schema}",
    }


def ensure_demo_seeded() -> None:
    settings = get_settings()
    if not settings.mock_mode:
        return
    db.init_db()
    with db.session() as connection:
        existing = connection.execute("SELECT COUNT(*) AS n FROM pipeline_runs").fetchone()["n"]
    if int(existing) > 0:
        return
    for scenario in SCENARIOS:
        run_mock_scenario(scenario)
