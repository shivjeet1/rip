from __future__ import annotations

import argparse
import csv
import json
import random
from datetime import date, timedelta
from pathlib import Path
from typing import Any

SCENARIOS = ("NORMAL", "A", "B", "C")
DEFAULT_COUNT = 50000
BASE_DATE = date(2025, 1, 1)
SCENARIO_SEEDS = {"NORMAL": 1101, "A": 1201, "B": 1301, "C": 1401}
REGIONS = ("NORTHEAST", "SOUTHEAST", "MIDWEST", "SOUTHWEST", "WEST")
NETWORK_TIERS = ("IN_NETWORK", "PREFERRED", "OUT_OF_NETWORK")
CLAIM_STATUSES = ("APPROVED", "PENDING", "UNDER_REVIEW", "DENIED")
DIAGNOSIS_CODES = (
    "M54.5",
    "I10",
    "S72.001A",
    "E11.9",
    "J45.909",
    "K21.9",
    "G43.909",
    "M17.11",
    "F41.1",
    "N39.0",
    "R07.9",
    "S93.401A",
)
TIER_PROFILE = {
    "BRONZE": (0.60, 2500.00),
    "SILVER": (0.75, 1500.00),
    "GOLD": (0.85, 750.00),
    "PLATINUM": (0.92, 250.00),
}
HOSPITAL_STEMS = (
    "Northstar",
    "Lakeside",
    "Summit Ridge",
    "Cedar Valley",
    "Harbor Point",
    "Redwood",
    "Silver Creek",
    "Granite Peak",
    "Blue Mesa",
    "Union Square",
    "Riverside",
    "Crescent",
)
HOSPITAL_SUFFIXES = (
    "Medical Center",
    "Regional Hospital",
    "Health Pavilion",
    "Surgical Institute",
    "Community Hospital",
    "Care Network",
)


def _build_providers() -> list[dict[str, str]]:
    providers: list[dict[str, str]] = []
    for index in range(1, 61):
        stem = HOSPITAL_STEMS[(index - 1) % len(HOSPITAL_STEMS)]
        suffix = HOSPITAL_SUFFIXES[(index - 1) % len(HOSPITAL_SUFFIXES)]
        providers.append(
            {
                "provider_id": f"HOSP_{index:03d}",
                "provider_name": f"{stem} {suffix} {index:02d}",
                "network_tier": NETWORK_TIERS[index % 3],
                "region": REGIONS[index % len(REGIONS)],
            }
        )
    providers.append(
        {
            "provider_id": "HOSP_999",
            "provider_name": "Meridian Central Hospital Network",
            "network_tier": "PREFERRED",
            "region": "MIDWEST",
        }
    )
    return providers


def _build_policies() -> list[dict[str, Any]]:
    policies: list[dict[str, Any]] = []
    tiers = ("BRONZE", "SILVER", "GOLD", "PLATINUM")
    for index in range(1, 241):
        tier = tiers[(index - 1) % len(tiers)]
        coverage, deductible = TIER_PROFILE[tier]
        policies.append(
            {
                "policy_id": f"POL_{index:04d}",
                "tier_name": tier,
                "coverage_pct": coverage,
                "deductible": deductible,
            }
        )
    return policies


def _claim_amount(rng: random.Random) -> float:
    magnitude = rng.random()
    if magnitude < 0.55:
        return round(rng.uniform(180.0, 1800.0), 2)
    if magnitude < 0.88:
        return round(rng.uniform(1800.0, 5200.0), 2)
    return round(rng.uniform(5200.0, 9800.0), 2)


def _currency(amount: float) -> str:
    return f"${amount:,.2f}"


def _build_claims(scenario: str, count: int) -> tuple[list[dict[str, str]], list[dict[str, str]], list[dict[str, Any]]]:
    rng = random.Random(SCENARIO_SEEDS[scenario])
    providers = _build_providers()
    policies = _build_policies()
    provider_pool = [row["provider_id"] for row in providers]
    policy_pool = [row["policy_id"] for row in policies]
    claims: list[dict[str, str]] = []
    for index in range(count):
        if scenario == "A" and index < int(count * 0.70):
            provider_id = "HOSP_999"
        else:
            provider_id = rng.choice(provider_pool)
        claim_date = BASE_DATE + timedelta(days=rng.randint(0, 640))
        amount = _claim_amount(rng)
        claims.append(
            {
                "claim_id": f"CLM_{index:08d}",
                "policy_id": rng.choice(policy_pool),
                "provider_id": provider_id,
                "claim_amount": f"{amount:.2f}",
                "claim_date": claim_date.isoformat(),
                "claim_status": rng.choices(CLAIM_STATUSES, weights=(62, 18, 14, 6), k=1)[0],
                "diagnosis_code": rng.choice(DIAGNOSIS_CODES),
            }
        )
    if scenario == "B":
        duplicate_total = max(1, int(count * 0.04))
        malformed_total = int(count * 0.18)
        invalid_policy_total = int(count * 0.06)
        duplicate_start = max(1, int(count * 0.04))
        for index in range(duplicate_start, duplicate_start + duplicate_total):
            claims[index]["claim_id"] = claims[index - duplicate_start]["claim_id"]
        malformed_start = duplicate_start + duplicate_total + int(count * 0.02)
        for index in range(malformed_start, malformed_start + malformed_total):
            amount = float(claims[index]["claim_amount"])
            claims[index]["claim_amount"] = _currency(amount)
        policy_start = malformed_start + malformed_total + int(count * 0.02)
        for index in range(policy_start, policy_start + invalid_policy_total):
            claims[index]["policy_id"] = f"POLICY_UNMAPPED_{index:05d}"
    rng.shuffle(claims)
    return claims, providers, policies


def _write_jsonl(path: Path, claims: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for claim in claims:
            handle.write(json.dumps(claim, ensure_ascii=False, separators=(",", ":")))
            handle.write("\n")


def generate_scenario(
    scenario: str,
    output_root: str | Path,
    count: int = DEFAULT_COUNT,
    force: bool = False,
) -> dict[str, Any]:
    if scenario not in SCENARIOS:
        raise ValueError(f"Unsupported scenario: {scenario}")
    root = Path(output_root) / scenario
    root.mkdir(parents=True, exist_ok=True)
    claims_path = root / "claims_raw.json"
    providers_path = root / "providers.csv"
    policies_path = root / "policy_tiers.csv"
    if not force and _scenario_files_ready(root):
        return {
            "scenario": scenario,
            "output_dir": str(root),
            "claims": _line_count(claims_path),
            "reused": True,
        }
    claims, providers, policies = _build_claims(scenario, count)
    _write_jsonl(claims_path, claims)
    with providers_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=("provider_id", "provider_name", "network_tier", "region"))
        writer.writeheader()
        writer.writerows(providers)
    with policies_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=("policy_id", "tier_name", "coverage_pct", "deductible"))
        writer.writeheader()
        for policy in policies:
            writer.writerow(
                {
                    "policy_id": policy["policy_id"],
                    "tier_name": policy["tier_name"],
                    "coverage_pct": f"{policy['coverage_pct']:.2f}",
                    "deductible": f"{policy['deductible']:.2f}",
                }
            )
    return {
        "scenario": scenario,
        "output_dir": str(root),
        "claims": len(claims),
        "providers": len(providers),
        "policies": len(policies),
        "reused": False,
    }


def _line_count(path: Path) -> int:
    if not path.exists():
        return 0
    total = 0
    with path.open("r", encoding="utf-8") as handle:
        for _ in handle:
            total += 1
    return total


def _scenario_files_ready(root: Path) -> bool:
    files = (root / "claims_raw.json", root / "providers.csv", root / "policy_tiers.csv")
    return all(path.exists() and path.stat().st_size > 128 for path in files)


def ensure_scenario_files(
    scenario: str,
    output_root: str | Path,
    count: int = DEFAULT_COUNT,
) -> dict[str, Any]:
    root = Path(output_root) / scenario
    if _scenario_files_ready(root):
        claims_path = root / "claims_raw.json"
        return {"scenario": scenario, "output_dir": str(root), "claims": _line_count(claims_path), "reused": True}
    return generate_scenario(scenario, output_root, count=count, force=True)


def generate_all(output_root: str | Path, count: int = DEFAULT_COUNT, force: bool = False) -> list[dict[str, Any]]:
    return [generate_scenario(scenario, output_root, count=count, force=force) for scenario in SCENARIOS]


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic insurance claims landing files")
    parser.add_argument("--scenario", default="ALL", choices=("ALL",) + SCENARIOS)
    parser.add_argument("--count", type=int, default=DEFAULT_COUNT)
    parser.add_argument("--output", default="data/raw")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    if args.scenario == "ALL":
        results = generate_all(args.output, count=args.count, force=args.force)
    else:
        results = [generate_scenario(args.scenario, args.output, count=args.count, force=args.force)]
    for result in results:
        print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
