from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta
from urllib.request import Request, urlopen

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.utils.trigger_rule import TriggerRule

LOGGER = logging.getLogger(__name__)


def _job_id() -> int:
    return int(os.getenv("DATABRICKS_JOB_ID", "0"))


def _scenario() -> str:
    return os.getenv("CLAIMS_SCENARIO", "NORMAL").upper()


def _post_alert(payload: dict) -> None:
    webhook_url = os.getenv("ALERT_WEBHOOK_URL", "").strip()
    if not webhook_url:
        LOGGER.warning("Insurance claims alert without webhook: %s", json.dumps(payload, sort_keys=True))
        return
    request = Request(
        webhook_url,
        data=json.dumps(payload, sort_keys=True).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=10) as response:
            LOGGER.info("Insurance claims alert delivered with status %s", response.status)
    except Exception as error:
        LOGGER.error("Insurance claims alert delivery failed: %s", error)


def _on_pipeline_failure(context: dict) -> None:
    task_instance = context.get("task_instance")
    _post_alert(
        {
            "event": "pipeline_failure",
            "dag_id": context.get("dag").dag_id if context.get("dag") else "insurance_claims_pipeline",
            "task_id": getattr(task_instance, "task_id", "run_claims_pipeline"),
            "run_id": context.get("run_id"),
            "scenario": _scenario(),
            "severity": "CRITICAL",
        }
    )


def _on_sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis) -> None:
    LOGGER.error(
        "Insurance claims SLA missed for dag=%s tasks=%s blocking=%s",
        dag.dag_id,
        [task.task_id for task in task_list],
        [task.task_id for task in blocking_task_list],
    )
    _post_alert(
        {
            "event": "sla_miss",
            "dag_id": dag.dag_id,
            "tasks": [task.task_id for task in task_list],
            "scenario": _scenario(),
            "severity": "HIGH",
        }
    )


def _notify_completion(**context: dict) -> None:
    dag_run = context.get("dag_run")
    task_state = "unknown"
    if dag_run is not None:
        task_instance = dag_run.get_task_instance("run_claims_pipeline")
        if task_instance is not None and task_instance.state is not None:
            task_state = str(task_instance.state)
    _post_alert(
        {
            "event": "pipeline_completion",
            "dag_id": "insurance_claims_pipeline",
            "airflow_run_id": context.get("run_id"),
            "scenario": _scenario(),
            "pipeline_state": task_state,
            "severity": "CRITICAL" if task_state != "success" else "LOW",
        }
    )


default_args = {
    "owner": "claims-platform",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id="insurance_claims_pipeline",
    description="Databricks Free Edition insurance claims medallion pipeline and payout SLA monitor",
    default_args=default_args,
    schedule="0 6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    on_sla_miss_callback=_on_sla_miss,
    tags=["insurance", "claims", "databricks", "medallion"],
) as dag:
    run_claims_pipeline = DatabricksRunNowOperator(
        task_id="run_claims_pipeline",
        job_id=_job_id(),
        databricks_conn_id=os.getenv("DATABRICKS_CONN_ID", "databricks_default"),
        notebook_params={
            "scenario": _scenario(),
            "run_id": "airflow_{{ ds_nodash }}_{{ run_id }}",
            "repo_root": os.getenv("CLAIMS_REPO_ROOT", "/Workspace/claims_ops"),
        },
        polling_period_seconds=20,
        retries=1,
        retry_delay=timedelta(minutes=2),
        sla=timedelta(minutes=8),
        on_failure_callback=_on_pipeline_failure,
    )

    notify_reconciliation_sla = PythonOperator(
        task_id="notify_reconciliation_sla",
        python_callable=_notify_completion,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    run_claims_pipeline >> notify_reconciliation_sla
