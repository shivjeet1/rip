import os
import sys
import uuid
from datetime import datetime, timezone

from pyspark.sql.functions import (
    coalesce,
    col,
    current_timestamp,
    desc,
    greatest,
    lit,
    regexp_replace,
    round as spark_round,
    row_number,
    struct,
    sum as spark_sum,
    to_date,
    to_json,
    trim,
    when,
)
from pyspark.sql.window import Window

dbutils.widgets.text("scenario", "NORMAL")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "claims_ops")
dbutils.widgets.text("repo_root", "/Workspace/claims_ops")
dbutils.widgets.text("job_id", "0")

scenario = dbutils.widgets.get("scenario").strip().upper() or "NORMAL"
run_id = dbutils.widgets.get("run_id").strip() or f"run_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
catalog = dbutils.widgets.get("catalog").strip() or "workspace"
schema = dbutils.widgets.get("schema").strip() or "claims_ops"
repo_root = dbutils.widgets.get("repo_root").strip() or "/Workspace/claims_ops"
job_id = dbutils.widgets.get("job_id").strip() or "0"

if repo_root not in sys.path:
    sys.path.append(repo_root)
notebook_root = os.path.join(repo_root, "databricks_notebooks")
if notebook_root not in sys.path:
    sys.path.append(notebook_root)

from observability_helper import observe_stage, write_incident, write_reconciliation

MAX_REJECTION_RATE = 0.15
SLA_SECONDS = 120.0

claims = spark.table(f"{catalog}.{schema}.claims_bronze").filter(col("run_id") == lit(run_id))
provider_dim = spark.table(f"{catalog}.{schema}.dim_provider")
policy_dim = spark.table(f"{catalog}.{schema}.dim_policy_tier")

claims_with_payload = claims.withColumn(
    "raw_payload",
    to_json(
        struct(
            col("claim_id"),
            col("policy_id"),
            col("provider_id"),
            col("claim_amount"),
            col("claim_date"),
            col("claim_status"),
            col("diagnosis_code"),
        )
    ),
)

claims_ranked = claims_with_payload.withColumn(
    "claim_rank",
    row_number().over(Window.partitionBy("claim_id").orderBy(col("ingested_at").asc())),
)

duplicate_rows = claims_ranked.filter(col("claim_rank") > 1)
claims_deduped = claims_ranked.filter(col("claim_rank") == 1)

amount_normalized = claims_deduped.withColumn("claim_amount_numeric", col("claim_amount").cast("double"))
date_normalized = amount_normalized.withColumn("claim_date_parsed", to_date(col("claim_date"), "yyyy-MM-dd"))

provider_enriched = date_normalized.join(provider_dim, on="provider_id", how="left")
policy_enriched = provider_enriched.join(policy_dim, on="policy_id", how="left")

classified = policy_enriched.withColumn(
    "rejection_reason",
    when(
        col("claim_amount_numeric").isNull() | (col("claim_amount_numeric") <= 0),
        lit("MALFORMED_AMOUNT"),
    )
    .when(col("provider_name").isNull(), lit("INVALID_PROVIDER_ID"))
    .when(col("coverage_pct").isNull(), lit("INVALID_POLICY_ID"))
    .when(col("claim_date_parsed").isNull(), lit("INVALID_DATE")),
)

invalid_rows = classified.filter(col("rejection_reason").isNotNull())
valid_rows = classified.filter(col("rejection_reason").isNull())

duplicate_quarantine = duplicate_rows.select(
    col("claim_id"),
    col("run_id"),
    lit("DUPLICATE_CLAIM_ID").alias("rejection_reason"),
    col("raw_payload"),
)

invalid_quarantine = invalid_rows.select(
    col("claim_id"),
    col("run_id"),
    col("rejection_reason"),
    col("raw_payload"),
)

quarantine = (
    duplicate_quarantine.unionByName(invalid_quarantine)
    .withColumn("quarantined_at", current_timestamp())
    .select("claim_id", "run_id", "rejection_reason", "raw_payload", "quarantined_at")
)

silver = valid_rows.select(
    col("claim_id").cast("string").alias("claim_id"),
    col("policy_id").cast("string").alias("policy_id"),
    col("provider_id").cast("string").alias("provider_id"),
    col("claim_date_parsed").cast("date").alias("claim_date"),
    col("claim_amount_numeric").cast("double").alias("claim_amount"),
    spark_round(greatest(lit(0.0), col("claim_amount_numeric") * col("coverage_pct") - col("deductible")), 2).alias("approved_payout"),
    col("claim_status").cast("string").alias("claim_status"),
    col("diagnosis_code").cast("string").alias("diagnosis_code"),
    current_timestamp().alias("adjudicated_at"),
    col("run_id").cast("string").alias("run_id"),
)

records_received = claims.count()
duplicate_count = duplicate_rows.count()
malformed_amount_count = invalid_rows.filter(col("rejection_reason") == lit("MALFORMED_AMOUNT")).count()
invalid_provider_count = invalid_rows.filter(col("rejection_reason") == lit("INVALID_PROVIDER_ID")).count()
invalid_policy_count = invalid_rows.filter(col("rejection_reason") == lit("INVALID_POLICY_ID")).count()
invalid_date_count = invalid_rows.filter(col("rejection_reason") == lit("INVALID_DATE")).count()
invalid_count = invalid_rows.count()
records_rejected = duplicate_count + invalid_count
records_valid = records_received - records_rejected
rejection_rate = round(records_rejected / records_received, 6) if records_received else 0.0

provider_distribution = valid_rows.groupBy("provider_id").count()
top_provider = provider_distribution.orderBy(desc("count")).limit(1).collect()
top_provider_count = int(top_provider[0]["count"]) if top_provider else 0
top_key_skew_ratio = round(top_provider_count / records_valid, 6) if records_valid else 0.0
degraded = top_key_skew_ratio >= 0.65
quality_gate_failed = rejection_rate > MAX_REJECTION_RATE

payout_row = silver.agg(spark_round(spark_sum("approved_payout"), 2).alias("total_payout")).collect()
total_payout_usd = float(payout_row[0]["total_payout"] or 0.0) if payout_row else 0.0

delayed_row = (
    valid_rows.filter(col("provider_id") == lit("HOSP_999"))
    .agg(spark_round(spark_sum("claim_amount_numeric"), 2).alias("delayed_payout"))
    .collect()
)
delayed_payout_usd = float(delayed_row[0]["delayed_payout"] or 0.0) if delayed_row else 0.0

rejected_risk_row = (
    invalid_rows.withColumn("risk_amount", coalesce(regexp_replace(col("claim_amount"), "[^0-9.]", "").cast("double"), lit(0.0)))
    .agg(spark_round(spark_sum("risk_amount"), 2).alias("rejected_risk"))
    .collect()
)
rejected_risk_usd = float(rejected_risk_row[0]["rejected_risk"] or 0.0) if rejected_risk_row else 0.0

quarantine.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.claims_quarantine")

reconciliation_id = write_reconciliation(
    spark=spark,
    catalog=catalog,
    schema=schema,
    run_id=run_id,
    scenario=scenario,
    records_received=records_received,
    records_valid=records_valid,
    records_rejected=records_rejected,
    duplicate_count=duplicate_count,
    malformed_amount_count=malformed_amount_count,
    invalid_provider_count=invalid_provider_count,
    invalid_policy_count=invalid_policy_count,
    rejection_rate=rejection_rate,
    total_payout_usd=total_payout_usd,
)

stage = observe_stage(
    spark=spark,
    run_id=run_id,
    task_key="02_adjudicate_claims",
    scenario=scenario,
    notebook_path="databricks_notebooks/02_adjudicate_claims.py",
    catalog=catalog,
    schema=schema,
    job_id=job_id,
    orchestrator="databricks_job",
    dag_id="insurance_claims_pipeline",
    write_mode="overwrite",
    query_plan_df=provider_enriched,
    records_received=records_received,
    records_valid=records_valid,
    records_rejected=records_rejected,
    duplicate_count=duplicate_count,
    malformed_amount_count=malformed_amount_count,
    invalid_provider_count=invalid_provider_count,
    invalid_policy_count=invalid_policy_count,
    invalid_date_count=invalid_date_count,
    rejection_rate=rejection_rate,
    top_key_skew_ratio=top_key_skew_ratio,
    total_payout_usd=total_payout_usd,
    delayed_payout_usd=delayed_payout_usd,
    rejected_risk_usd=rejected_risk_usd,
    reconciliation_persistence=reconciliation_id,
)

try:
    with stage:
        if quality_gate_failed:
            raise ValueError(
                f"QUALITY_GATE_FAILED: rejection_rate={rejection_rate:.4f} exceeded threshold={MAX_REJECTION_RATE:.4f}"
            )
        silver.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.claims_silver")
        stage.set_metrics(row_count_in=records_received, row_count_out=records_valid)
        if degraded:
            stage.mark_degraded("DATA_SKEW", hot_provider_id=str(top_provider[0]["provider_id"]))
except Exception:
    incident_id = f"inc_{scenario.lower()}_{uuid.uuid4().hex[:10]}"
    write_incident(
        spark=spark,
        catalog=catalog,
        schema=schema,
        incident_id=incident_id,
        event_id=stage.event_id,
        run_id=run_id,
        task_key="02_adjudicate_claims",
        scenario=scenario,
        status="FAILED",
        severity="CRITICAL",
        business_criticality="HIGH",
        duration_sec=stage.duration_sec,
        row_count_in=records_received,
        row_count_out=0,
        rejected_row_count=records_rejected,
        top_key_skew_ratio=top_key_skew_ratio,
        payout_exposure_usd=rejected_risk_usd,
        error_class="SCHEMA_DRIFT",
        write_mode="overwrite",
        sla_breached=True,
    )
    raise
else:
    if degraded:
        incident_id = f"inc_{scenario.lower()}_{uuid.uuid4().hex[:10]}"
        write_incident(
            spark=spark,
            catalog=catalog,
            schema=schema,
            incident_id=incident_id,
            event_id=stage.event_id,
            run_id=run_id,
            task_key="02_adjudicate_claims",
            scenario=scenario,
            status="DEGRADED",
            severity="HIGH",
            business_criticality="HIGH",
            duration_sec=stage.duration_sec,
            row_count_in=records_received,
            row_count_out=records_valid,
            rejected_row_count=records_rejected,
            top_key_skew_ratio=top_key_skew_ratio,
            payout_exposure_usd=delayed_payout_usd,
            error_class="DATA_SKEW",
            write_mode="overwrite",
            sla_breached=stage.duration_sec > SLA_SECONDS or degraded,
        )

print(
    {
        "stage": "02_adjudicate_claims",
        "scenario": scenario,
        "run_id": run_id,
        "records_received": records_received,
        "records_valid": records_valid,
        "records_rejected": records_rejected,
        "duplicate_count": duplicate_count,
        "malformed_amount_count": malformed_amount_count,
        "invalid_policy_count": invalid_policy_count,
        "top_key_skew_ratio": top_key_skew_ratio,
        "rejection_rate": rejection_rate,
        "status": stage.status,
        "event_id": stage.event_id,
    }
)
