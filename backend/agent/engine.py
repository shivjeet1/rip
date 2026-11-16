from __future__ import annotations

import difflib
import json
import logging
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any

import httpx

from .. import db
from ..config import get_settings
from .prompts import SYSTEM_PROMPT, build_user_prompt
from .schemas import AgentDiagnosis, RetrySafety, RootCauseCategory
from .tools import AgentToolbox

LOGGER = logging.getLogger(__name__)

ERROR_CATEGORY = {
    "DATA_SKEW": RootCauseCategory.DATA_SKEW,
    "SCHEMA_DRIFT": RootCauseCategory.SCHEMA_DRIFT,
    "PARTIAL_PAYOUT_WRITE": RootCauseCategory.PARTIAL_PAYOUT_WRITE,
}

DEFAULT_NOTEBOOK = {
    RootCauseCategory.DATA_SKEW: "databricks_notebooks/02_adjudicate_claims.py",
    RootCauseCategory.SCHEMA_DRIFT: "databricks_notebooks/02_adjudicate_claims.py",
    RootCauseCategory.PARTIAL_PAYOUT_WRITE: "databricks_notebooks/03_generate_payout_batch.py",
    RootCauseCategory.OTHER: "databricks_notebooks/02_adjudicate_claims.py",
}


class DiagnosisError(RuntimeError):
    pass


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _replace_block(source: str, old_block: list[str], new_block: list[str]) -> str:
    lines = source.splitlines()
    normalized = [line.strip() for line in lines]
    target = [line.strip() for line in old_block]
    start = -1
    for index in range(0, len(normalized) - len(target) + 1):
        if normalized[index : index + len(target)] == target:
            start = index
            break
    if start < 0:
        raise DiagnosisError("Notebook anchor for remediation diff was not found")
    original_line = lines[start]
    base_indent = original_line[: len(original_line) - len(original_line.lstrip())]
    replacement = [f"{base_indent}{line}" if line else "" for line in new_block]
    updated = lines[:start] + replacement + lines[start + len(target) :]
    trailing = "\n" if source.endswith("\n") else ""
    return "\n".join(updated) + trailing


def _unified_diff(original: str, updated: str, path: str) -> str:
    diff_lines = list(
        difflib.unified_diff(
            original.splitlines(),
            updated.splitlines(),
            fromfile=f"a/{path}",
            tofile=f"b/{path}",
            lineterm="",
            n=3,
        )
    )
    if not diff_lines:
        raise DiagnosisError("Remediation produced an empty diff")
    return "\n".join(diff_lines) + "\n"


def _salt_join_remediation(source: str) -> str:
    old_block = [
        'provider_enriched = date_normalized.join(provider_dim, on="provider_id", how="left")',
    ]
    new_block = [
        "from pyspark.sql.functions import concat, floor, rand",
        "provider_dim_salted = provider_dim.withColumn(",
        '    "provider_join_key",',
        '    concat(col("provider_id"), lit("_"), floor(rand() * 10).cast("string")),',
        ').drop("provider_id")',
        "claims_salted = date_normalized.withColumn(",
        '    "provider_join_key",',
        '    concat(col("provider_id"), lit("_"), floor(rand() * 10).cast("string")),',
        ")",
        "provider_enriched = claims_salted.join(",
        "    provider_dim_salted,",
        '    on="provider_join_key",',
        '    how="left",',
        ').drop("provider_join_key")',
    ]
    return _replace_block(source, old_block, new_block)


def _amount_normalization_remediation(source: str) -> str:
    old_block = [
        'amount_normalized = claims_deduped.withColumn("claim_amount_numeric", col("claim_amount").cast("double"))',
    ]
    new_block = [
        "amount_cleaned = claims_deduped.withColumn(",
        '    "claim_amount_clean",',
        '    regexp_replace(col("claim_amount"), "[^0-9.]", ""),',
        ")",
        "amount_normalized = amount_cleaned.withColumn(",
        '    "claim_amount_numeric",',
        '    col("claim_amount_clean").cast("double"),',
        ').drop("claim_amount_clean")',
    ]
    return _replace_block(source, old_block, new_block)


def _payout_merge_remediation(source: str) -> str:
    old_block = [
        'batch_one.write.mode("append").saveAsTable(fact_table)',
        "stage.set_metrics(row_count_in=fact_count, row_count_out=batch_one_count)",
        'raise RuntimeError("Downstream clearinghouse ledger timeout")',
    ]
    new_block = [
        "from delta.tables import DeltaTable",
        "target = DeltaTable.forName(spark, fact_table)",
        "(",
        '    target.alias("target")',
        "    .merge(",
        '        batch_one.alias("source"),',
        '        "target.run_id = source.run_id AND target.provider_id = source.provider_id AND target.date_id = source.date_id",',
        "    )",
        "    .whenMatchedUpdateAll()",
        "    .whenNotMatchedInsertAll()",
        "    .execute()",
        ")",
        "(",
        '    target.alias("target")',
        "    .merge(",
        '        batch_two.alias("source"),',
        '        "target.run_id = source.run_id AND target.provider_id = source.provider_id AND target.date_id = source.date_id",',
        "    )",
        "    .whenMatchedUpdateAll()",
        "    .whenNotMatchedInsertAll()",
        "    .execute()",
        ")",
        "stage.set_metrics(row_count_in=fact_count, row_count_out=batch_one_count + batch_two_count)",
    ]
    return _replace_block(source, old_block, new_block)


def _count_guard_remediation(source: str) -> str:
    old_block = [
        'silver.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.claims_silver")',
    ]
    new_block = [
        "pre_write_count = silver.count()",
        "if pre_write_count != records_valid:",
        '    raise RuntimeError(f"SILVER_COUNT_MISMATCH: expected={records_valid} actual={pre_write_count}")',
        'silver.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.claims_silver")',
    ]
    return _replace_block(source, old_block, new_block)


def build_remediation_diff(category: RootCauseCategory, source: str, path: str) -> str:
    if category == RootCauseCategory.DATA_SKEW:
        updated = _salt_join_remediation(source)
    elif category == RootCauseCategory.SCHEMA_DRIFT:
        updated = _amount_normalization_remediation(source)
    elif category == RootCauseCategory.PARTIAL_PAYOUT_WRITE:
        updated = _payout_merge_remediation(source)
    else:
        updated = _count_guard_remediation(source)
    return _unified_diff(source, updated, path)


def _extract_json(content: str) -> dict[str, Any]:
    start = content.find("{")
    finish = content.rfind("}")
    if start < 0 or finish <= start:
        raise DiagnosisError("Model response did not contain a JSON object")
    try:
        return json.loads(content[start : finish + 1])
    except json.JSONDecodeError as error:
        raise DiagnosisError(f"Model response JSON was invalid: {error}") from error


class AgentEngine:
    def __init__(self, toolbox: AgentToolbox | None = None) -> None:
        self.toolbox = toolbox or AgentToolbox()
        self.settings = get_settings()

    def diagnose(self, incident_id: str) -> dict[str, Any]:
        incident = db.get_incident(incident_id)
        if not incident:
            raise DiagnosisError(f"Incident not found: {incident_id}")
        if self.settings.llm_ready:
            try:
                diagnosis = self._diagnose_with_llm(incident_id)
                diagnosis = self._enforce_guardrails(diagnosis, incident)
                return self._persist(incident_id, diagnosis, self.settings.llm_label)
            except Exception as error:
                LOGGER.warning("LLM diagnosis unavailable for %s: %s", incident_id, error)
        diagnosis = self._fallback_diagnosis(incident)
        return self._persist(incident_id, diagnosis, "deterministic-rule-engine")

    def _diagnose_with_llm(self, incident_id: str) -> AgentDiagnosis:
        messages: list[dict[str, Any]] = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": build_user_prompt(incident_id)},
        ]
        tools = self.toolbox.tool_definitions()
        headers = {"Content-Type": "application/json"}
        if self.settings.llm_api_key:
            headers["Authorization"] = f"Bearer {self.settings.llm_api_key}"
        endpoint = f"{self.settings.llm_base_url}/chat/completions"
        with httpx.Client(timeout=self.settings.llm_timeout_seconds) as client:
            for _ in range(self.settings.agent_max_tool_rounds):
                response = client.post(
                    endpoint,
                    headers=headers,
                    json={
                        "model": self.settings.llm_model,
                        "messages": messages,
                        "tools": tools,
                        "tool_choice": "auto",
                        "temperature": 0.1,
                    },
                )
                response.raise_for_status()
                body = response.json()
                choices = body.get("choices") or []
                if not choices:
                    raise DiagnosisError("Model returned no choices")
                message = choices[0].get("message") or {}
                tool_calls = message.get("tool_calls") or []
                if tool_calls:
                    messages.append(
                        {
                            "role": "assistant",
                            "content": message.get("content"),
                            "tool_calls": tool_calls,
                        }
                    )
                    for call in tool_calls:
                        function = call.get("function") or {}
                        name = str(function.get("name", ""))
                        result = self.toolbox.dispatch(name, function.get("arguments") or "{}")
                        messages.append(
                            {
                                "role": "tool",
                                "tool_call_id": call.get("id", "tool_call"),
                                "name": name,
                                "content": json.dumps(result, default=str),
                            }
                        )
                    continue
                content = message.get("content") or ""
                return AgentDiagnosis.model_validate(_extract_json(content))
        raise DiagnosisError("Model exhausted the tool-call round limit")

    def _enforce_guardrails(self, diagnosis: AgentDiagnosis, incident: dict[str, Any]) -> AgentDiagnosis:
        diagnosis.payout_exposure_usd = round(float(incident.get("payout_exposure_usd") or 0.0), 2)
        expected_category = ERROR_CATEGORY.get(str(incident.get("error_class", "")))
        if expected_category is not None:
            diagnosis.root_cause_category = expected_category
        if str(incident.get("write_mode", "")) == "append_non_idempotent":
            diagnosis.retry_safety = RetrySafety.MANUAL_CLEANUP_REQUIRED
            diagnosis.retry_recommendation = (
                self._manual_cleanup_instruction(incident) + "\n\n" + diagnosis.retry_recommendation
            )
        return diagnosis

    def _manual_cleanup_instruction(self, incident: dict[str, Any]) -> str:
        run_id = str(incident.get("run_id", ""))
        return (
            "MANUAL CLEANUP REQUIRED. Do not retry the append writer. Remove the partial payout batch first:\n\n"
            "```sql\n"
            f"DELETE FROM workspace.claims_ops.fact_claims_payouts WHERE run_id = '{run_id}';\n"
            "```\n\n"
            "Confirm that the run has zero committed fact rows before deploying the MERGE diff and rerunning "
            "03_generate_payout_batch with a new run_id."
        )

    def _fallback_diagnosis(self, incident: dict[str, Any]) -> AgentDiagnosis:
        error_class = str(incident.get("error_class", ""))
        category = ERROR_CATEGORY.get(error_class, RootCauseCategory.OTHER)
        event = db.get_bronze_event(incident) or {}
        reconciliation = db.get_reconciliation_for_incident(incident) or {}
        path = str(event.get("notebook_path") or DEFAULT_NOTEBOOK[category])
        read_result = self.toolbox.read_notebook_source(path)
        normalized_path = str(read_result["notebook_path"])
        source = str(read_result["source"])
        code_diff = build_remediation_diff(category, source, normalized_path)
        exposure = round(float(incident.get("payout_exposure_usd") or 0.0), 2)
        run_id = str(incident.get("run_id", ""))
        received = int(reconciliation.get("records_received") or incident.get("row_count_in") or 0)
        rejected = int(reconciliation.get("records_rejected") or incident.get("rejected_row_count") or 0)
        duplicates = int(reconciliation.get("duplicate_count") or 0)
        malformed = int(reconciliation.get("malformed_amount_count") or 0)
        invalid_policy = int(reconciliation.get("invalid_policy_count") or 0)
        rejection_rate = float(reconciliation.get("rejection_rate") or 0.0)
        skew_ratio = float(incident.get("top_key_skew_ratio") or 0.0)
        duration = float(incident.get("duration_sec") or 0.0)
        committed = db.committed_fact_summary(run_id)
        if category == RootCauseCategory.DATA_SKEW:
            summary = (
                f"Provider HOSP_999 concentrated {skew_ratio * 100:.1f}% of {received:,} claims, stretching "
                f"02_adjudicate_claims to {duration:.1f}s. The unsalted provider join creates one hot shuffle "
                f"partition and delays ${exposure:,.2f} of adjudication while the Gold payout batch waits."
            )
            impact = (
                "Claims from the dominant hospital network are adjudicated behind the SLA. Members and providers "
                "wait for payout confirmation, reserve estimates become stale, and the delayed Gold batch increases "
                "the risk of state claim-timeliness penalties and provider-relations escalation."
            )
            retry_safety = RetrySafety.SAFE_TO_RETRY
            recommendation = (
                "Deploy the salted-join diff, then rerun 02_adjudicate_claims and 03_generate_payout_batch with "
                f"run_id {run_id}. Verify the hot-key share is below 0.05 before accepting the batch.\n\n"
                "```sql\n"
                "SELECT provider_id, COUNT(*) AS claim_count FROM workspace.claims_ops.claims_silver "
                f"WHERE run_id = '{run_id}' GROUP BY provider_id ORDER BY claim_count DESC LIMIT 5;\n"
                "```\n\n"
                "The Silver and Gold writers are overwrite-based, so this retry does not duplicate payouts."
            )
        elif category == RootCauseCategory.SCHEMA_DRIFT:
            summary = (
                f"Currency-formatted amounts rejected {malformed:,} claims while {duplicates:,} duplicate claim IDs "
                f"and {invalid_policy:,} unmapped policy IDs raised the rejection rate to {rejection_rate * 100:.2f}%. "
                f"The 15% quality gate stopped Silver publication and blocked ${exposure:,.2f} of potential payouts."
            )
            impact = (
                "Members with affected claims remain in quarantine, creating a visible claims backlog and a "
                "regulatory timeliness risk. Duplicate submissions inflate exposure if reprocessed naively, while "
                "unmapped policies must be corrected upstream rather than silently paid."
            )
            retry_safety = RetrySafety.SAFE_TO_RETRY
            recommendation = (
                "Deploy the amount-normalization diff, preserve duplicate and unmapped-policy quarantine routing, "
                f"then rerun the job with run_id {run_id}. Confirm the published rejection rate is below 0.15.\n\n"
                "```sql\n"
                "SELECT rejection_rate, records_valid, records_rejected FROM "
                f"workspace.claims_ops.claims_reconciliation_report WHERE run_id = '{run_id}';\n"
                "```\n\n"
                "Because the Silver and Gold writers overwrite by run, the retry is idempotent once the gate passes."
            )
        elif category == RootCauseCategory.PARTIAL_PAYOUT_WRITE:
            summary = (
                f"03_generate_payout_batch appended region batch 01 for run {run_id}, committing ${exposure:,.2f} "
                f"across {incident.get('row_count_out', 0)} fact rows, then failed before region batch 02. The ledger "
                "now holds a partial payout batch while the orchestrator records the run as failed."
            )
            impact = (
                "A blind append retry would duplicate the already committed region payouts and create a double-payment "
                "risk in the clearinghouse. Finance cannot close the payout cycle until the partial batch is reconciled "
                "and removed, and providers in the held regions face delayed settlement."
            )
            retry_safety = RetrySafety.MANUAL_CLEANUP_REQUIRED
            recommendation = (
                self._manual_cleanup_instruction(incident)
                + "\n\n```sql\n"
                + "SELECT COUNT(*) AS committed_rows FROM workspace.claims_ops.fact_claims_payouts "
                + f"WHERE run_id = '{run_id}';\n"
                + "```\n\n"
                + "After cleanup, deploy the MERGE diff, rerun 03_generate_payout_batch with a new run_id, and verify "
                + "that each provider/date key is unique."
            )
        else:
            summary = (
                f"Run {run_id} recorded {incident.get('status', 'FAILED')} with {error_class or 'UNCLASSIFIED'} and "
                f"${exposure:,.2f} of payout exposure. Telemetry is insufficient to attribute a known insurance "
                "failure class, so the remediation applies a Silver row-count guard before publication."
            )
            impact = (
                "The claims pipeline cannot certify the Silver output against the reconciliation contract. Paying "
                "from an unverified batch risks incorrect member benefits and weakens the audit trail."
            )
            retry_safety = RetrySafety.SAFE_TO_RETRY
            recommendation = (
                "Deploy the row-count guard diff, rerun 02_adjudicate_claims, and reconcile the Silver count against "
                f"records_valid for run {run_id} before allowing the Gold payout batch to proceed.\n\n"
                "```sql\n"
                f"SELECT records_valid FROM workspace.claims_ops.claims_reconciliation_report WHERE run_id = '{run_id}';\n"
                "```"
            )
        return AgentDiagnosis(
            root_cause_category=category,
            summary=summary,
            insurance_business_impact=impact,
            payout_exposure_usd=exposure,
            code_diff=code_diff,
            retry_safety=retry_safety,
            retry_recommendation=recommendation,
        )

    def _persist(self, incident_id: str, diagnosis: AgentDiagnosis, model_used: str) -> dict[str, Any]:
        created_at = _utc_now()
        record = diagnosis.model_dump(mode="json")
        record["incident_id"] = incident_id
        record["model_used"] = model_used
        record["created_at"] = created_at
        db.save_diagnosis(record)
        return record


@lru_cache(maxsize=1)
def get_agent_engine() -> AgentEngine:
    return AgentEngine()
