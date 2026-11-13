import os
import sys
import uuid
from datetime import datetime, timezone

from pyspark.sql.functions import (
    col,
    count,
    current_timestamp,
    date_format,
    lit,
    month,
    quarter,
    round as spark_round,
    sum as spark_sum,
    year,
)

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

from observability_helper import observe_stage, write_incident

fact_table = f"{catalog}.{schema}.fact_claims_payouts"
silver_table = f"{catalog}.{schema}.claims_silver"
provider_table = f"{catalog}.{schema}.dim_provider"

silver = spark.table(silver_table).filter(col("run_id") == lit(run_id))
provider_dim = spark.table(provider_table)

dim_date = (
    silver.select(col("claim_date").alias("full_date"))
    .distinct()
    .withColumn("date_id", date_format(col("full_date"), "yyyyMMdd").cast("int"))
    .withColumn("month", month(col("full_date")))
    .withColumn("quarter", quarter(col("full_date")))
    .withColumn("year", year(col("full_date")))
    .select("date_id", "full_date", "month", "quarter", "year")
)

fact = (
    silver.join(provider_dim.select("provider_id", "region"), on="provider_id", how="left")
    .withColumn("date_id", date_format(col("claim_date"), "yyyyMMdd").cast("int"))
    .groupBy("provider_id", "date_id", "region")
    .agg(
        count("*").alias("claim_count"),
        spark_round(spark_sum("claim_amount"), 2).alias("total_claimed_usd"),
        spark_round(spark_sum("approved_payout"), 2).alias("total_payout_usd"),
    )
    .withColumn("run_id", lit(run_id))
    .withColumn("created_at", current_timestamp())
)

run_token = run_id.split("_")[-1]
fact_count = fact.count()
payout_row = fact.agg(spark_round(spark_sum("total_payout_usd"), 2).alias("payout")).collect()
total_fact_payout = float(payout_row[0]["payout"] or 0.0) if payout_row else 0.0

if scenario == "C":
    regions = [row["region"] for row in fact.select("region").distinct().orderBy("region").collect()]
    split_index = max(1, len(regions) // 2)
    committed_regions = regions[:split_index]
    batch_one = fact.filter(col("region").isin(committed_regions)).withColumn("batch_id", lit(f"batch_{run_token}_01"))
    batch_two = fact.filter(~col("region").isin(committed_regions)).withColumn("batch_id", lit(f"batch_{run_token}_02"))
    batch_one_count = batch_one.count()
    batch_two_count = batch_two.count()
    uncommitted_row = batch_two.agg(spark_round(spark_sum("total_payout_usd"), 2).alias("payout")).collect()
    uncommitted_payout = float(uncommitted_row[0]["payout"] or 0.0) if uncommitted_row else 0.0
    stage = observe_stage(
        spark=spark,
        run_id=run_id,
        task_key="03_generate_payout_batch",
        scenario=scenario,
        notebook_path="databricks_notebooks/03_generate_payout_batch.py",
        catalog=catalog,
        schema=schema,
        job_id=job_id,
        orchestrator="databricks_job",
        dag_id="insurance_claims_pipeline",
        write_mode="append_non_idempotent",
        query_plan_df=fact,
        fact_rows=fact_count,
        committed_regions=committed_regions,
        committed_rows=batch_one_count,
        uncommitted_rows=batch_two_count,
        total_fact_payout=total_fact_payout,
        uncommitted_payout_usd=uncommitted_payout,
    )
    try:
        with stage:
            dim_date.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.dim_date")
            batch_one.write.mode("append").saveAsTable(fact_table)
            stage.set_metrics(row_count_in=fact_count, row_count_out=batch_one_count)
            raise RuntimeError("Downstream clearinghouse ledger timeout")
    except Exception:
        committed_row = (
            spark.table(fact_table)
            .filter(col("run_id") == lit(run_id))
            .agg(
                count("*").alias("committed_rows"),
                spark_round(spark_sum("total_payout_usd"), 2).alias("committed_payout"),
            )
            .collect()
        )
        committed_rows = int(committed_row[0]["committed_rows"] or 0) if committed_row else 0
        committed_payout = float(committed_row[0]["committed_payout"] or 0.0) if committed_row else 0.0
        incident_id = f"inc_{scenario.lower()}_{uuid.uuid4().hex[:10]}"
        write_incident(
            spark=spark,
            catalog=catalog,
            schema=schema,
            incident_id=incident_id,
            event_id=stage.event_id,
            run_id=run_id,
            task_key="03_generate_payout_batch",
            scenario=scenario,
            status="FAILED",
            severity="CRITICAL",
            business_criticality="CRITICAL",
            duration_sec=stage.duration_sec,
            row_count_in=fact_count,
            row_count_out=committed_rows,
            rejected_row_count=0,
            top_key_skew_ratio=0.0,
            payout_exposure_usd=committed_payout,
            error_class="PARTIAL_PAYOUT_WRITE",
            write_mode="append_non_idempotent",
            sla_breached=True,
        )
        raise
else:
    final_fact = fact.withColumn("batch_id", lit(f"batch_{run_token}_01"))
    with observe_stage(
        spark=spark,
        run_id=run_id,
        task_key="03_generate_payout_batch",
        scenario=scenario,
        notebook_path="databricks_notebooks/03_generate_payout_batch.py",
        catalog=catalog,
        schema=schema,
        job_id=job_id,
        orchestrator="databricks_job",
        dag_id="insurance_claims_pipeline",
        write_mode="overwrite",
        query_plan_df=final_fact,
        fact_rows=fact_count,
        total_fact_payout=total_fact_payout,
    ) as stage:
        dim_date.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.dim_date")
        final_fact.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(fact_table)
        stage.set_metrics(row_count_in=fact_count, row_count_out=fact_count)

print(
    {
        "stage": "03_generate_payout_batch",
        "scenario": scenario,
        "run_id": run_id,
        "fact_rows": fact_count,
        "total_fact_payout_usd": total_fact_payout,
        "status": stage.status,
        "event_id": stage.event_id,
    }
)
