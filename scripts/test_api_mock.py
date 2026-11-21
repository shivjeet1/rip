from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from fastapi.testclient import TestClient

from backend.main import (
    HealthResponse,
    IncidentDetailResponse,
    IncidentListResponse,
    OverviewResponse,
    ScenarioTriggerResponse,
)
from backend.agent.schemas import DiagnosisRecord
from backend.main import app
from scripts.test_local_mock import run_green_audit

EXPECTED_CATEGORY = {"A": "DATA_SKEW", "B": "SCHEMA_DRIFT", "C": "PARTIAL_PAYOUT_WRITE"}
EXPECTED_NOTEBOOK = {
    "A": "02_adjudicate_claims.py",
    "B": "02_adjudicate_claims.py",
    "C": "03_generate_payout_batch.py",
}
EXPECTED_TRACE = {
    "A": "No Python exception",
    "B": "QUALITY_GATE_FAILED",
    "C": "clearinghouse ledger timeout",
}
EXPECTED_ADDITION = {
    "A": ("provider_join_key", "rand()"),
    "B": ("regexp_replace",),
    "C": ("DeltaTable", ".merge("),
}


def _check(condition: bool, message: str, detail: str = "") -> None:
    if not condition:
        suffix = f" :: {detail}" if detail else ""
        raise AssertionError(f"{message}{suffix}")
    print(f"[PASS] {message}")


def _added_lines(diff: str) -> str:
    return "\n".join(
        line for line in diff.splitlines() if line.startswith("+") and not line.startswith("+++")
    )


def _trigger(client: TestClient, scenario: str) -> ScenarioTriggerResponse:
    response = client.post("/api/scenarios/trigger", json={"scenario": scenario})
    _check(response.status_code == 200, f"POST /api/scenarios/trigger {scenario} returns 200", response.text)
    payload = ScenarioTriggerResponse.model_validate(response.json())
    _check(payload.scenario == scenario, f"Scenario {scenario} trigger echoes the requested scenario")
    _check(payload.mode == "MOCK", f"Scenario {scenario} trigger reports MOCK mode")
    _check(payload.reconciliation is not None, f"Scenario {scenario} trigger returns reconciliation")
    return payload


def _assert_trigger_contract(payload: ScenarioTriggerResponse) -> None:
    scenario = payload.scenario
    reconciliation = payload.reconciliation
    if scenario == "A":
        _check(payload.status == "DEGRADED", "Scenario A trigger status is DEGRADED")
        _check(bool(payload.incident_id), "Scenario A trigger returns an incident id")
        _check(payload.payout_exposure_usd > 0, "Scenario A trigger returns delayed payout exposure")
        _check(payload.fact_rows_committed > 0, "Scenario A trigger still commits Gold rows")
    elif scenario == "B":
        _check(payload.status == "FAILED", "Scenario B trigger status is FAILED")
        _check(bool(payload.incident_id), "Scenario B trigger returns an incident id")
        _check(reconciliation is not None, "Scenario B trigger returns reconciliation")
        _check(reconciliation.rejection_rate > 0.15, "Scenario B trigger rejection rate exceeds 0.15")
        _check(reconciliation.duplicate_count > 0, "Scenario B trigger exposes duplicate count")
        _check(reconciliation.malformed_amount_count > 0, "Scenario B trigger exposes malformed amount count")
        _check(reconciliation.invalid_policy_count > 0, "Scenario B trigger exposes invalid policy count")
        _check(payload.payout_exposure_usd > 0, "Scenario B trigger quantifies rejected claim risk")
    elif scenario == "C":
        _check(payload.status == "FAILED", "Scenario C trigger status is FAILED")
        _check(bool(payload.incident_id), "Scenario C trigger returns an incident id")
        _check(payload.payout_exposure_usd > 0, "Scenario C trigger returns committed payout exposure")
        _check(payload.fact_rows_committed > 0, "Scenario C trigger committed region batch 01")
        _check(payload.fact_rows_uncommitted > 0, "Scenario C trigger left region batch 02 uncommitted")


def _assert_detail(client: TestClient, incident_id: str, scenario: str) -> IncidentDetailResponse:
    response = client.get(f"/api/incidents/{incident_id}")
    _check(response.status_code == 200, f"GET /api/incidents/{incident_id} returns 200", response.text)
    detail = IncidentDetailResponse.model_validate(response.json())
    _check(detail.incident.incident_id == incident_id, f"Scenario {scenario} detail returns the requested incident")
    _check(detail.incident.scenario == scenario, f"Scenario {scenario} detail preserves scenario")
    _check("Physical Plan" in detail.query_plan_formatted, f"Scenario {scenario} detail includes a formatted plan")
    _check(bool(detail.raw_metrics), f"Scenario {scenario} detail includes raw telemetry metrics")
    _check(
        detail.notebook_path.endswith(EXPECTED_NOTEBOOK[scenario]),
        f"Scenario {scenario} detail references the failing notebook",
        detail.notebook_path,
    )
    _check(detail.reconciliation is not None, f"Scenario {scenario} detail includes reconciliation")
    _check(
        EXPECTED_TRACE[scenario] in detail.stack_trace,
        f"Scenario {scenario} detail includes the expected traceback context",
        detail.stack_trace,
    )
    _check(detail.diagnosis is None, f"Scenario {scenario} starts without a cached diagnosis")
    return detail


def _diagnose(client: TestClient, incident_id: str, scenario: str, detail: IncidentDetailResponse) -> DiagnosisRecord:
    response = client.post(f"/api/incidents/{incident_id}/diagnose")
    _check(response.status_code == 200, f"POST /api/incidents/{incident_id}/diagnose returns 200", response.text)
    diagnosis = DiagnosisRecord.model_validate(response.json())
    _check(
        diagnosis.root_cause_category.value == EXPECTED_CATEGORY[scenario],
        f"Scenario {scenario} diagnosis root cause is {EXPECTED_CATEGORY[scenario]}",
    )
    _check(
        abs(diagnosis.payout_exposure_usd - detail.incident.payout_exposure_usd) <= 0.011,
        f"Scenario {scenario} diagnosis preserves payout exposure",
    )
    diff = diagnosis.code_diff
    _check(diff.startswith("--- a/"), f"Scenario {scenario} diff has a unified from-file header")
    _check("\n+++ b/" in diff, f"Scenario {scenario} diff has a unified to-file header")
    _check("@@ " in diff, f"Scenario {scenario} diff contains a hunk header")
    additions = _added_lines(diff)
    for token in EXPECTED_ADDITION[scenario]:
        _check(token in additions, f"Scenario {scenario} diff adds {token}")
    _check(len(diagnosis.summary) >= 20, f"Scenario {scenario} summary is substantive")
    _check(len(diagnosis.insurance_business_impact) >= 20, f"Scenario {scenario} business impact is substantive")
    _check(len(diagnosis.retry_recommendation) >= 20, f"Scenario {scenario} retry recommendation is substantive")
    if scenario == "C":
        _check(
            diagnosis.retry_safety.value == "MANUAL_CLEANUP_REQUIRED",
            "Scenario C diagnosis strictly requires manual cleanup",
        )
        recommendation = diagnosis.retry_recommendation
        _check("DELETE FROM" in recommendation.upper(), "Scenario C recommendation includes DELETE FROM")
        _check(
            "workspace.claims_ops.fact_claims_payouts" in recommendation,
            "Scenario C recommendation targets the Gold payout fact table",
        )
        _check(
            detail.incident.run_id in recommendation,
            "Scenario C recommendation scopes cleanup to the failed run id",
        )
    else:
        _check(
            diagnosis.retry_safety.value == "SAFE_TO_RETRY",
            f"Scenario {scenario} diagnosis is safe to retry",
        )
    return diagnosis


def _assert_persisted_diagnosis(client: TestClient, incident_id: str, scenario: str) -> None:
    response = client.get(f"/api/incidents/{incident_id}")
    detail = IncidentDetailResponse.model_validate(response.json())
    _check(detail.diagnosis is not None, f"Scenario {scenario} diagnosis persists to SQLite")
    _check(detail.incident.diagnosed, f"Scenario {scenario} detail marks the incident diagnosed")
    _check(
        detail.diagnosis.model_used == "deterministic-rule-engine",
        f"Scenario {scenario} uses the deterministic fallback analyzer",
    )


def main() -> int:
    print("[INFO] Phase 2 verification: FastAPI control plane, Pydantic contracts and agent guardrails")
    try:
        with TestClient(app) as client:
            root = client.get("/")
            _check(root.status_code == 200, "GET / returns the dashboard")
            _check("text/html" in root.headers.get("content-type", ""), "GET / serves HTML")
            _check("Reliability flight deck" in root.text, "GET / renders the AMOLED dashboard shell")
            _check("/static/styles.css" in root.text, "GET / references the zero-build stylesheet")
            _check("/static/app.js" in root.text, "GET / references the zero-build client script")

            styles = client.get("/static/styles.css")
            _check(styles.status_code == 200, "GET /static/styles.css returns the stylesheet")
            _check("--canvas: #000000" in styles.text, "Stylesheet pins the pure-black AMOLED canvas")

            script = client.get("/static/app.js")
            _check(script.status_code == 200, "GET /static/app.js returns the client script")

            health_response = client.get("/api/health")
            _check(health_response.status_code == 200, "GET /api/health returns 200")
            health = HealthResponse.model_validate(health_response.json())
            _check(health.status == "ok", "GET /api/health reports ok")
            _check(health.mode == "MOCK", "GET /api/health reports MOCK mode")
            _check(health.mock_mode is True, "GET /api/health reports mock_mode")
            _check(health.fallback_analyzer is True, "GET /api/health advertises the deterministic fallback")
            _check(health.llm_provider != "", "GET /api/health reports the configured LLM provider")

            overview_response = client.get("/api/overview")
            _check(overview_response.status_code == 200, "GET /api/overview returns 200")
            overview = OverviewResponse.model_validate(overview_response.json())
            _check(overview.total_runs >= 4, "GET /api/overview counts seeded pipeline runs")
            _check(overview.sla_breaches >= 3, "GET /api/overview counts SLA breaches")
            _check(overview.payout_exposure_usd > 0, "GET /api/overview quantifies payout exposure")
            _check(
                0.0 <= overview.latest_reconciliation_rate <= 100.0,
                "GET /api/overview reports a valid reconciliation pass rate",
            )
            _check(overview.cache_synced_at is not None, "GET /api/overview reports cache freshness")

            initial_incidents_response = client.get("/api/incidents")
            _check(initial_incidents_response.status_code == 200, "GET /api/incidents returns 200")
            initial_incidents = IncidentListResponse.model_validate(initial_incidents_response.json())
            _check(initial_incidents.count >= 3, "GET /api/incidents lists seeded incidents")
            _check(
                initial_incidents.incidents[0].business_criticality == "CRITICAL",
                "GET /api/incidents sorts CRITICAL business incidents first",
            )

            triggers = {scenario: _trigger(client, scenario) for scenario in ("A", "B", "C")}
            for payload in triggers.values():
                _assert_trigger_contract(payload)

            incident_response = client.get("/api/incidents")
            _check(incident_response.status_code == 200, "GET /api/incidents returns 200 after triggers")
            incident_list = IncidentListResponse.model_validate(incident_response.json())
            _check(incident_list.count == 3, "GET /api/incidents returns one incident per failing scenario")
            by_scenario = {incident.scenario: incident for incident in incident_list.incidents}
            _check(set(by_scenario.keys()) == {"A", "B", "C"}, "Incident queue contains scenarios A, B and C")
            _check(
                incident_list.incidents[0].scenario == "C",
                "Incident queue prioritizes the CRITICAL partial payout incident",
            )

            details: dict[str, IncidentDetailResponse] = {}
            diagnoses: dict[str, DiagnosisRecord] = {}
            for scenario in ("A", "B", "C"):
                incident_id = by_scenario[scenario].incident_id
                details[scenario] = _assert_detail(client, incident_id, scenario)
                diagnoses[scenario] = _diagnose(client, incident_id, scenario, details[scenario])
                _assert_persisted_diagnosis(client, incident_id, scenario)

            refreshed_response = client.get("/api/incidents", params={"refresh": "true"})
            _check(refreshed_response.status_code == 200, "GET /api/incidents?refresh=true returns 200")
            refreshed = IncidentListResponse.model_validate(refreshed_response.json())
            _check(refreshed.count == 3, "GET /api/incidents?refresh=true preserves the incident queue")
            for incident in refreshed.incidents:
                _check(incident.diagnosed, f"Incident {incident.incident_id} is marked diagnosed after agent run")
                _check(incident.retry_safety is not None, f"Incident {incident.scenario} exposes retry safety")

            missing_incident = client.get("/api/incidents/inc_missing")
            _check(missing_incident.status_code == 404, "GET /api/incidents/{unknown} returns 404")
            missing_diagnosis = client.post("/api/incidents/inc_missing/diagnose")
            _check(missing_diagnosis.status_code == 404, "POST /api/incidents/{unknown}/diagnose returns 404")
            invalid_scenario = client.post("/api/scenarios/trigger", json={"scenario": "Z"})
            _check(invalid_scenario.status_code == 422, "POST /api/scenarios/trigger rejects an unsupported scenario")

            c_diagnosis = diagnoses["C"]
            _check(
                c_diagnosis.retry_safety.value == "MANUAL_CLEANUP_REQUIRED",
                "Scenario C final guardrail is MANUAL_CLEANUP_REQUIRED",
            )
            _check(
                "DELETE FROM" in c_diagnosis.retry_recommendation.upper(),
                "Scenario C final recovery SQL includes DELETE FROM",
            )

            print("[INFO] Sample diagnosis payload for Scenario C")
            print(json.dumps(diagnoses["C"].model_dump(mode="json"), indent=2)[:2400])

        run_green_audit()
        print("[PASS] Phase 2 verification gate completed successfully")
        return 0
    except Exception as error:
        print(f"[FAIL] Phase 2 verification gate failed: {type(error).__name__}: {error}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
