CREATE SCHEMA IF NOT EXISTS workspace.claims_ops;
CREATE VOLUME IF NOT EXISTS workspace.claims_ops.raw_landing;
USE CATALOG workspace;
USE SCHEMA claims_ops;

CREATE TABLE IF NOT EXISTS claims_bronze (
    claim_id STRING,
    policy_id STRING,
    provider_id STRING,
    claim_amount STRING,
    claim_date STRING,
    claim_status STRING,
    diagnosis_code STRING,
    ingested_at TIMESTAMP,
    run_id STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS claims_silver (
    claim_id STRING,
    policy_id STRING,
    provider_id STRING,
    claim_date DATE,
    claim_amount DOUBLE,
    approved_payout DOUBLE,
    claim_status STRING,
    diagnosis_code STRING,
    adjudicated_at TIMESTAMP,
    run_id STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS claims_quarantine (
    claim_id STRING,
    run_id STRING,
    rejection_reason STRING,
    raw_payload STRING,
    quarantined_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS claims_reconciliation_report (
    run_id STRING,
    scenario STRING,
    records_received BIGINT,
    records_valid BIGINT,
    records_rejected BIGINT,
    duplicate_count BIGINT,
    malformed_amount_count BIGINT,
    invalid_provider_count BIGINT,
    invalid_policy_count BIGINT,
    rejection_rate DOUBLE,
    total_payout_usd DOUBLE,
    created_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS dim_provider (
    provider_id STRING,
    provider_name STRING,
    network_tier STRING,
    region STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS dim_policy_tier (
    policy_id STRING,
    tier_name STRING,
    coverage_pct DOUBLE,
    deductible DOUBLE
) USING DELTA;

CREATE TABLE IF NOT EXISTS dim_date (
    date_id INT,
    full_date DATE,
    month INT,
    quarter INT,
    year INT
) USING DELTA;

CREATE TABLE IF NOT EXISTS fact_claims_payouts (
    batch_id STRING,
    run_id STRING,
    provider_id STRING,
    date_id INT,
    region STRING,
    claim_count BIGINT,
    total_claimed_usd DOUBLE,
    total_payout_usd DOUBLE,
    created_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS obs_bronze_runs (
    event_id STRING,
    run_id STRING,
    job_id STRING,
    orchestrator STRING,
    dag_id STRING,
    task_key STRING,
    scenario STRING,
    notebook_path STRING,
    status STRING,
    query_plan_formatted STRING,
    stack_trace STRING,
    raw_metrics_json STRING,
    created_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS obs_silver_incidents (
    incident_id STRING,
    event_id STRING,
    run_id STRING,
    task_key STRING,
    scenario STRING,
    status STRING,
    severity STRING,
    business_criticality STRING,
    duration_sec DOUBLE,
    row_count_in BIGINT,
    row_count_out BIGINT,
    rejected_row_count BIGINT,
    top_key_skew_ratio DOUBLE,
    payout_exposure_usd DOUBLE,
    error_class STRING,
    write_mode STRING,
    sla_breached BOOLEAN,
    created_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS agent_diagnoses (
    incident_id STRING,
    root_cause_category STRING,
    summary STRING,
    insurance_business_impact STRING,
    payout_exposure_usd DOUBLE,
    code_diff STRING,
    retry_safety STRING,
    retry_recommendation STRING,
    model_used STRING,
    created_at TIMESTAMP
) USING DELTA;
