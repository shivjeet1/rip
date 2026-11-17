from __future__ import annotations

import json
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Literal

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict, Field

from . import databricks_client, db
from .agent.engine import DiagnosisError, get_agent_engine
from .agent.schemas import DiagnosisRecord
from .config import get_settings


class HealthResponse(BaseModel):
    status: str
    mode: str
    mock_mode: bool
    cache_synced_at: str | None
    cache_age_seconds: float | None
    cache_ttl_seconds: int
    llm_provider: str
    llm_model: str
    llm_ready: bool
    llm_label: str
    fallback_analyzer: bool
    local_cache_db: str
    namespace: str


class ReconciliationResponse(BaseModel):
    model_config = ConfigDict(extra="ignore")

    run_id: str
    scenario: str
    records_received: int
    records_valid: int
    records_rejected: int
    duplicate_count: int
    malformed_amount_count: int
    invalid_provider_count: int
    invalid_policy_count: int
    rejection_rate: float
    total_payout_usd: float
    created_at: str


class PipelineRunSummary(BaseModel):
    model_config = ConfigDict(extra="ignore")

    run_id: str
    scenario: str
    triggered_by: str | None = None
    status: str
    started_at: str | None = None
    finished_at: str | None = None
    bronze_rows: int = 0
    silver_rows: int = 0
    gold_rows: int = 0
    incident_id: str | None = None
    message: str | None = None


class OverviewResponse(BaseModel):
    mode: str
    generated_at: str
    total_runs: int
    sla_breaches: int
    payout_exposure_usd: float
    critical_payout_exposure_usd: float
    latest_reconciliation_rate: float
    latest_reconciliation: ReconciliationResponse | None
    cache_synced_at: str | None
    open_incidents: int
    diagnosed_incidents: int
    latest_run: PipelineRunSummary | None


class IncidentSummary(BaseModel):
    model_config = ConfigDict(extra="ignore")

    incident_id: str
    event_id: str | None = None
    run_id: str
    task_key: str
    scenario: str
    status: str
    severity: str
    business_criticality: str
    duration_sec: float
    row_count_in: int
    row_count_out: int
    rejected_row_count: int
    top_key_skew_ratio: float
    payout_exposure_usd: float
    error_class: str | None = None
    write_mode: str | None = None
    sla_breached: bool
    created_at: str
    diagnosed: bool = False
    root_cause_category: str | None = None
    retry_safety: str | None = None
    diagnosis_summary: str | None = None


class IncidentListResponse(BaseModel):
    mode: str
    cache_synced_at: str | None
    count: int
    incidents: list[IncidentSummary]


class IncidentDetailResponse(BaseModel):
    mode: str
    incident: IncidentSummary
    query_plan_formatted: str
    stack_trace: str
    raw_metrics: dict[str, Any]
    notebook_path: str
    event_status: str | None = None
    reconciliation: ReconciliationResponse | None = None
    diagnosis: DiagnosisRecord | None = None


class ScenarioRequest(BaseModel):
    scenario: Literal["NORMAL", "A", "B", "C"]


class ScenarioTriggerResponse(BaseModel):
    mode: str
    scenario: str
    run_id: str
    status: str
    incident_id: str | None = None
    payout_exposure_usd: float = 0.0
    fact_rows_committed: int = 0
    fact_rows_uncommitted: int = 0
    message: str
    reconciliation: ReconciliationResponse | None = None
    databricks_run_id: str | None = None


class DiagnoseResponse(DiagnosisRecord):
    pass


def _cache_age_seconds(value: str | None) -> float | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return round((datetime.now(timezone.utc) - parsed).total_seconds(), 2)


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    db.init_db()
    if settings.mock_mode:
        databricks_client.ensure_demo_seeded()
    yield


settings = get_settings()
db.init_db()
app = FastAPI(
    title="Insurance Claims Reliability Control Plane",
    version="1.0.0",
    lifespan=lifespan,
)
app.mount("/static", StaticFiles(directory=str(settings.web_dir)), name="static")


@app.get("/", include_in_schema=False)
def dashboard() -> FileResponse:
    return FileResponse(settings.web_dir / "index.html", media_type="text/html")


@app.get("/api/health", response_model=HealthResponse)
def health() -> HealthResponse:
    payload = databricks_client.health_payload()
    payload["cache_age_seconds"] = _cache_age_seconds(payload.get("cache_synced_at"))
    return HealthResponse.model_validate(payload)


@app.get("/api/overview", response_model=OverviewResponse)
def overview() -> OverviewResponse:
    try:
        payload = db.overview()
    except Exception as error:
        raise HTTPException(status_code=500, detail=f"Overview read failed: {error}") from error
    payload["mode"] = settings.mode_label
    payload["generated_at"] = db.utc_now()
    return OverviewResponse.model_validate(payload)


@app.get("/api/incidents", response_model=IncidentListResponse)
def incidents(refresh: bool = Query(default=False)) -> IncidentListResponse:
    try:
        rows = databricks_client.fetch_incidents(refresh=refresh)
    except RuntimeError as error:
        raise HTTPException(status_code=502, detail=str(error)) from error
    incident_models = [IncidentSummary.model_validate(row) for row in rows]
    return IncidentListResponse(
        mode=settings.mode_label,
        cache_synced_at=db.get_meta("cache_synced_at"),
        count=len(incident_models),
        incidents=incident_models,
    )


@app.get("/api/incidents/{incident_id}", response_model=IncidentDetailResponse)
def incident_detail(incident_id: str) -> IncidentDetailResponse:
    incident = db.get_incident_with_diagnosis(incident_id)
    if not incident:
        raise HTTPException(status_code=404, detail="Incident not found")
    event = db.get_bronze_event(incident) or {}
    try:
        raw_metrics = json.loads(event.get("raw_metrics_json") or "{}")
    except json.JSONDecodeError:
        raw_metrics = {"raw": event.get("raw_metrics_json")}
    reconciliation_row = db.get_reconciliation_for_incident(incident)
    diagnosis_row = db.get_diagnosis(incident_id)
    incident_summary = IncidentSummary.model_validate(incident)
    return IncidentDetailResponse(
        mode=settings.mode_label,
        incident=incident_summary,
        query_plan_formatted=str(event.get("query_plan_formatted") or ""),
        stack_trace=str(event.get("stack_trace") or ""),
        raw_metrics=raw_metrics,
        notebook_path=str(event.get("notebook_path") or ""),
        event_status=event.get("status"),
        reconciliation=ReconciliationResponse.model_validate(reconciliation_row) if reconciliation_row else None,
        diagnosis=DiagnosisRecord.model_validate(diagnosis_row) if diagnosis_row else None,
    )


@app.post("/api/scenarios/trigger", response_model=ScenarioTriggerResponse)
def trigger_scenario(request: ScenarioRequest) -> ScenarioTriggerResponse:
    try:
        result = databricks_client.run_scenario(request.scenario)
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error
    except RuntimeError as error:
        raise HTTPException(status_code=502, detail=str(error)) from error
    payload = dict(result)
    payload["mode"] = settings.mode_label
    if payload.get("reconciliation") is not None:
        payload["reconciliation"] = ReconciliationResponse.model_validate(payload["reconciliation"])
    return ScenarioTriggerResponse.model_validate(payload)


@app.post("/api/incidents/{incident_id}/diagnose", response_model=DiagnoseResponse)
def diagnose_incident(incident_id: str) -> DiagnoseResponse:
    incident = db.get_incident(incident_id)
    if not incident:
        raise HTTPException(status_code=404, detail="Incident not found")
    try:
        result = get_agent_engine().diagnose(incident_id)
    except DiagnosisError as error:
        raise HTTPException(status_code=422, detail=str(error)) from error
    except Exception as error:
        raise HTTPException(status_code=500, detail=f"Diagnosis failed: {error}") from error
    return DiagnoseResponse.model_validate(result)


@app.get("/api/config", include_in_schema=False)
def config_summary() -> dict[str, Any]:
    return {
        "mode": settings.mode_label,
        "mock_mode": settings.mock_mode,
        "llm_label": settings.llm_label,
        "llm_ready": settings.llm_ready,
        "cache_ttl_seconds": settings.cache_ttl_seconds,
        "namespace": f"{settings.databricks_catalog}.{settings.databricks_schema}",
    }
