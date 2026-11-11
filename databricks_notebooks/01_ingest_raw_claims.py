import os
import sys
from datetime import datetime, timezone

from pyspark.sql.functions import col, current_timestamp, desc, lit

dbutils.widgets.text("scenario", "NORMAL")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "claims_ops")
dbutils.widgets.text("volume_root", "/Volumes/workspace/claims_ops/raw_landing")
dbutils.widgets.text("repo_root", "/Workspace/claims_ops")
dbutils.widgets.text("job_id", "0")

scenario = dbutils.widgets.get("scenario").strip().upper() or "NORMAL"
run_id = dbutils.widgets.get("run_id").strip() or f"run_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
catalog = dbutils.widgets.get("catalog").strip() or "workspace"
schema = dbutils.widgets.get("schema").strip() or "claims_ops"
volume_root = dbutils.widgets.get("volume_root").strip() or "/Volumes/workspace/claims_ops/raw_landing"
repo_root = dbutils.widgets.get("repo_root").strip() or "/Workspace/claims_ops"
job_id = dbutils.widgets.get("job_id").strip() or "0"

if repo_root not in sys.path:
    sys.path.append(repo_root)
notebook_root = os.path.join(repo_root, "databricks_notebooks")
if notebook_root not in sys.path:
    sys.path.append(notebook_root)

from observability_helper import observe_stage
from scripts.generate_raw_data import ensure_scenario_files

scenario_root = os.path.join(volume_root, scenario)
claims_path = os.path.join(scenario_root, "claims_raw.json")
providers_path = os.path.join(scenario_root, "providers.csv")
policies_path = os.path.join(scenario_root, "policy_tiers.csv")

if not all(os.path.exists(path) for path in (claims_path, providers_path, policies_path)):
    ensure_scenario_files(scenario, volume_root, count=50000)

claims_raw = spark.read.option("multiLine", "false").json(claims_path)
providers_raw = spark.read.option("header", "true").option("inferSchema", "true").csv(providers_path)
policies_raw = spark.read.option("header", "true").option("inferSchema", "true").csv(policies_path)

bronze = claims_raw.select(
    col("claim_id").cast("string").alias("claim_id"),
    col("policy_id").cast("string").alias("policy_id"),
    col("provider_id").cast("string").alias("provider_id"),
    col("claim_amount").cast("string").alias("claim_amount"),
    col("claim_date").cast("string").alias("claim_date"),
    col("claim_status").cast("string").alias("claim_status"),
    col("diagnosis_code").cast("string").alias("diagnosis_code"),
    current_timestamp().alias("ingested_at"),
    lit(run_id).alias("run_id"),
)

providers_dim = providers_raw.select(
    col("provider_id").cast("string").alias("provider_id"),
    col("provider_name").cast("string").alias("provider_name"),
    col("network_tier").cast("string").alias("network_tier"),
    col("region").cast("string").alias("region"),
)

policy_tiers_dim = policies_raw.select(
    col("policy_id").cast("string").alias("policy_id"),
    col("tier_name").cast("string").alias("tier_name"),
    col("coverage_pct").cast("double").alias("coverage_pct"),
    col("deductible").cast("double").alias("deductible"),
)

records_received = claims_raw.count()
provider_distribution = bronze.groupBy("provider_id").count()
top_provider = provider_distribution.orderBy(desc("count")).limit(1).collect()
top_provider_count = int(top_provider[0]["count"]) if top_provider else 0
top_key_skew_ratio = round(top_provider_count / records_received, 6) if records_received else 0.0

with observe_stage(
    spark=spark,
    run_id=run_id,
    task_key="01_ingest_raw_claims",
    scenario=scenario,
    notebook_path="databricks_notebooks/01_ingest_raw_claims.py",
    catalog=catalog,
    schema=schema,
    job_id=job_id,
    orchestrator="databricks_job",
    dag_id="insurance_claims_pipeline",
    write_mode="overwrite",
    query_plan_df=bronze,
    records_received=records_received,
    row_count_in=records_received,
    row_count_out=records_received,
    top_key_skew_ratio=top_key_skew_ratio,
    volume_root=volume_root,
) as stage:
    bronze.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.claims_bronze")
    providers_dim.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.dim_provider")
    policy_tiers_dim.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.dim_policy_tier")
    stage.set_metrics(row_count_out=records_received)
    if top_key_skew_ratio >= 0.65:
        stage.mark_degraded("DATA_SKEW", hot_provider_id=str(top_provider[0]["provider_id"]))

print(
    {
        "stage": "01_ingest_raw_claims",
        "scenario": scenario,
        "run_id": run_id,
        "records_received": records_received,
        "top_key_skew_ratio": top_key_skew_ratio,
        "status": stage.status,
        "event_id": stage.event_id,
    }
)
