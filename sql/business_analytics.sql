SELECT
    p.provider_id,
    p.provider_name,
    p.network_tier,
    p.region,
    SUM(f.claim_count) AS claim_count,
    ROUND(SUM(f.total_claimed_usd), 2) AS total_claimed_usd,
    ROUND(SUM(f.total_payout_usd), 2) AS total_payout_usd,
    ROUND(SUM(f.total_payout_usd) / SUM(f.claim_count), 2) AS avg_payout_per_claim
FROM workspace.claims_ops.fact_claims_payouts f
JOIN workspace.claims_ops.dim_provider p
    ON f.provider_id = p.provider_id
GROUP BY
    p.provider_id,
    p.provider_name,
    p.network_tier,
    p.region
ORDER BY total_payout_usd DESC
LIMIT 10;

SELECT
    f.region,
    SUM(f.claim_count) AS claim_count,
    ROUND(SUM(f.total_claimed_usd), 2) AS total_claimed_usd,
    ROUND(SUM(f.total_payout_usd), 2) AS total_payout_usd,
    ROUND(SUM(f.total_payout_usd) / SUM(f.claim_count), 2) AS avg_claim_value_usd,
    ROUND(100 * SUM(f.total_payout_usd) / SUM(SUM(f.total_payout_usd)) OVER (), 2) AS payout_share_pct
FROM workspace.claims_ops.fact_claims_payouts f
GROUP BY f.region
ORDER BY total_payout_usd DESC;

SELECT
    d.year,
    d.month,
    SUM(f.claim_count) AS claim_count,
    ROUND(SUM(f.total_claimed_usd), 2) AS total_claimed_usd,
    ROUND(SUM(f.total_payout_usd), 2) AS total_payout_usd,
    ROUND(SUM(f.total_payout_usd) / SUM(f.claim_count), 2) AS avg_payout_per_claim,
    ROUND(SUM(f.total_payout_usd) - LAG(SUM(f.total_payout_usd)) OVER (ORDER BY d.year, d.month), 2) AS month_over_month_payout_change
FROM workspace.claims_ops.fact_claims_payouts f
JOIN workspace.claims_ops.dim_date d
    ON f.date_id = d.date_id
GROUP BY d.year, d.month
ORDER BY d.year, d.month;

SELECT
    t.tier_name,
    t.coverage_pct,
    t.deductible,
    COUNT(s.claim_id) AS claim_count,
    ROUND(SUM(s.claim_amount), 2) AS total_claimed_usd,
    ROUND(SUM(s.approved_payout), 2) AS total_payout_usd,
    ROUND(SUM(s.approved_payout) / NULLIF(SUM(s.claim_amount), 0), 4) AS effective_coverage_ratio,
    ROUND(AVG(s.claim_amount), 2) AS avg_claim_amount,
    ROUND(AVG(GREATEST(0.0, s.claim_amount * t.coverage_pct - t.deductible)), 2) AS avg_approved_payout,
    ROUND(SUM(GREATEST(0.0, s.claim_amount * t.coverage_pct - t.deductible)), 2) AS modeled_covered_payout_usd
FROM workspace.claims_ops.claims_silver s
JOIN workspace.claims_ops.dim_policy_tier t
    ON s.policy_id = t.policy_id
GROUP BY t.tier_name, t.coverage_pct, t.deductible
ORDER BY total_payout_usd DESC;

SELECT
    run_id,
    scenario,
    records_received,
    records_valid,
    records_rejected,
    duplicate_count,
    malformed_amount_count,
    invalid_provider_count,
    invalid_policy_count,
    ROUND(rejection_rate * 100, 2) AS rejection_rate_pct,
    ROUND(total_payout_usd, 2) AS total_payout_usd,
    records_received - records_valid - records_rejected AS unclassified_delta,
    created_at
FROM workspace.claims_ops.claims_reconciliation_report
ORDER BY created_at DESC;
