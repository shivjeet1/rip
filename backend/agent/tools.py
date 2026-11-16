from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .. import db
from ..config import get_settings


class AgentToolError(RuntimeError):
    pass


class AgentToolbox:
    def __init__(self) -> None:
        settings = get_settings()
        self.notebooks_root = settings.notebooks_dir.resolve()

    def get_incident_and_reconciliation(self, incident_id: str) -> dict[str, Any]:
        incident = db.get_incident(incident_id)
        if not incident:
            raise AgentToolError(f"Incident not found: {incident_id}")
        reconciliation = db.get_reconciliation_for_incident(incident)
        committed = db.committed_fact_summary(str(incident["run_id"]))
        return {
            "incident": incident,
            "reconciliation": reconciliation,
            "committed_facts": committed,
        }

    def get_plan_and_traceback(self, incident_id: str) -> dict[str, Any]:
        incident = db.get_incident(incident_id)
        if not incident:
            raise AgentToolError(f"Incident not found: {incident_id}")
        event = db.get_bronze_event(incident)
        if not event:
            raise AgentToolError(f"Bronze observability event not found for incident: {incident_id}")
        raw_metrics: Any
        try:
            raw_metrics = json.loads(event.get("raw_metrics_json") or "{}")
        except json.JSONDecodeError:
            raw_metrics = {"unparsed": event.get("raw_metrics_json")}
        return {
            "event_id": event.get("event_id"),
            "run_id": event.get("run_id"),
            "task_key": event.get("task_key"),
            "scenario": event.get("scenario"),
            "status": event.get("status"),
            "notebook_path": event.get("notebook_path"),
            "query_plan_formatted": event.get("query_plan_formatted") or "",
            "stack_trace": event.get("stack_trace") or "",
            "raw_metrics": raw_metrics,
        }

    def read_notebook_source(self, notebook_path: str) -> dict[str, Any]:
        raw_path = str(notebook_path).strip()
        if not raw_path:
            raise AgentToolError("notebook_path is required")
        candidate = Path(raw_path)
        if candidate.is_absolute():
            resolved = candidate.resolve()
        else:
            parts = candidate.parts
            if parts and parts[0] == "databricks_notebooks":
                candidate = Path(*parts[1:])
            resolved = (self.notebooks_root / candidate).resolve()
        if not resolved.is_relative_to(self.notebooks_root):
            raise AgentToolError("Notebook path escapes databricks_notebooks")
        if resolved.suffix != ".py" or not resolved.is_file():
            raise AgentToolError(f"Notebook source is unavailable: {raw_path}")
        source = resolved.read_text(encoding="utf-8")
        return {
            "notebook_path": str(resolved.relative_to(self.notebooks_root.parent)),
            "line_count": len(source.splitlines()),
            "source": source,
        }

    def tool_definitions(self) -> list[dict[str, Any]]:
        return [
            {
                "type": "function",
                "function": {
                    "name": "get_incident_and_reconciliation",
                    "description": "Return Silver incident metrics, the matching source-to-target reconciliation breakdown, and committed Gold fact totals.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "incident_id": {"type": "string", "description": "Silver incident identifier"}
                        },
                        "required": ["incident_id"],
                        "additionalProperties": False,
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "get_plan_and_traceback",
                    "description": "Return the formatted Spark plan, stack trace, raw telemetry metrics, and notebook path for an incident.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "incident_id": {"type": "string", "description": "Silver incident identifier"}
                        },
                        "required": ["incident_id"],
                        "additionalProperties": False,
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "read_notebook_source",
                    "description": "Read the exact PySpark notebook source referenced by an incident so the remediation can be expressed as a unified diff.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "notebook_path": {
                                "type": "string",
                                "description": "Path such as databricks_notebooks/02_adjudicate_claims.py",
                            }
                        },
                        "required": ["notebook_path"],
                        "additionalProperties": False,
                    },
                },
            },
        ]

    def dispatch(self, name: str, arguments: dict[str, Any] | str) -> dict[str, Any]:
        try:
            parsed = json.loads(arguments) if isinstance(arguments, str) else dict(arguments or {})
        except json.JSONDecodeError as error:
            return {"ok": False, "error": f"Invalid tool arguments: {error}"}
        try:
            if name == "get_incident_and_reconciliation":
                data = self.get_incident_and_reconciliation(str(parsed.get("incident_id", "")))
            elif name == "get_plan_and_traceback":
                data = self.get_plan_and_traceback(str(parsed.get("incident_id", "")))
            elif name == "read_notebook_source":
                data = self.read_notebook_source(str(parsed.get("notebook_path", "")))
            else:
                return {"ok": False, "error": f"Unknown tool: {name}"}
        except Exception as error:
            return {"ok": False, "error": f"{type(error).__name__}: {error}"}
        return {"ok": True, "data": data}
