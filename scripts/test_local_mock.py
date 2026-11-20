from __future__ import annotations

import argparse
import ast
import csv
import io
import json
import re
import sys
import time
import tokenize
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend import databricks_client, db
from scripts.generate_raw_data import SCENARIOS, generate_all

CODE_SUFFIXES = {".py", ".sql", ".html", ".css", ".js"}
WEB_SUFFIXES = {".html", ".css", ".js", ".svg"}
FORBIDDEN_TOKENS = (
    "sc." + "parallelize",
    "." + "rdd",
    "." + "_jdf",
    "spark" + "context",
)
GREEN_NAMES = re.compile(r"\b(green|emerald|lime|teal)\b", re.IGNORECASE)
HEX_COLOR = re.compile(r"#[0-9A-Fa-f]{3,8}\b")
RGB_COLOR = re.compile(r"rgba?\(\s*(\d{1,3})\s*,\s*(\d{1,3})\s*,\s*(\d{1,3})")
HSL_COLOR = re.compile(r"hsla?\(\s*([0-9.]+)")


def _check(condition: bool, message: str, detail: str = "") -> None:
    if not condition:
        suffix = f" :: {detail}" if detail else ""
        raise AssertionError(f"{message}{suffix}")
    print(f"[PASS] {message}")


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if stripped:
                records.append(json.loads(stripped))
    return records


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _loose_amount(raw: Any) -> float:
    try:
        return float(re.sub(r"[^0-9.]", "", str(raw)))
    except ValueError:
        return 0.0


def _expected_classification(scenario: str, raw_root: Path) -> dict[str, Any]:
    scenario_root = raw_root / scenario
    records = _read_jsonl(scenario_root / "claims_raw.json")
    providers = {row["provider_id"] for row in _read_csv(scenario_root / "providers.csv")}
    policies = {row["policy_id"] for row in _read_csv(scenario_root / "policy_tiers.csv")}
    seen: set[str] = set()
    reasons: Counter[str] = Counter()
    provider_counts: Counter[str] = Counter()
    delayed_total = 0.0
    rejected_risk = 0.0
    valid = 0
    for record in records:
        claim_id = str(record.get("claim_id", "")).strip()
        policy_id = str(record.get("policy_id", "")).strip()
        provider_id = str(record.get("provider_id", "")).strip()
        raw_amount = record.get("claim_amount", "")
        provider_counts[provider_id] += 1
        reason: str | None = None
        try:
            amount = float(str(raw_amount).strip())
            if amount <= 0:
                amount = None
        except ValueError:
            amount = None
        try:
            datetime.strptime(str(record.get("claim_date", "")).strip(), "%Y-%m-%d")
            valid_date = True
        except ValueError:
            valid_date = False
        if claim_id in seen:
            reason = "DUPLICATE_CLAIM_ID"
        else:
            seen.add(claim_id)
            if amount is None:
                reason = "MALFORMED_AMOUNT"
            elif provider_id not in providers:
                reason = "INVALID_PROVIDER_ID"
            elif policy_id not in policies:
                reason = "INVALID_POLICY_ID"
            elif not valid_date:
                reason = "INVALID_DATE"
        if reason is not None:
            reasons[reason] += 1
            if reason != "DUPLICATE_CLAIM_ID":
                rejected_risk += _loose_amount(raw_amount)
            continue
        valid += 1
        if provider_id == "HOSP_999":
            delayed_total += float(amount)
    received = len(records)
    rejected = sum(reasons.values())
    top_count = max(provider_counts.values()) if provider_counts else 0
    return {
        "received": received,
        "valid": valid,
        "rejected": rejected,
        "rejection_rate": round(rejected / received, 6) if received else 0.0,
        "top_provider_id": provider_counts.most_common(1)[0][0] if provider_counts else "",
        "top_key_skew_ratio": round(top_count / received, 6) if received else 0.0,
        "duplicate_count": reasons["DUPLICATE_CLAIM_ID"],
        "malformed_amount_count": reasons["MALFORMED_AMOUNT"],
        "invalid_provider_count": reasons["INVALID_PROVIDER_ID"],
        "invalid_policy_count": reasons["INVALID_POLICY_ID"],
        "delayed_payout_usd": round(delayed_total, 2),
        "rejected_risk_usd": round(rejected_risk, 2),
    }


def _table_counts() -> dict[str, int]:
    names = (
        "claims_bronze",
        "claims_silver",
        "claims_quarantine",
        "claims_reconciliation_report",
        "obs_bronze_runs",
        "obs_silver_incidents",
        "fact_claims_payouts",
        "agent_diagnoses",
        "pipeline_runs",
    )
    counts: dict[str, int] = {}
    with db.session() as connection:
        for name in names:
            counts[name] = int(connection.execute(f"SELECT COUNT(*) AS n FROM {name}").fetchone()["n"])
    return counts


def _iter_code_files() -> list[Path]:
    files: list[Path] = []
    for directory in ("backend", "scripts", "airflow", "databricks_notebooks", "sql", "web"):
        for path in sorted((ROOT / directory).rglob("*")):
            if path.is_file() and path.suffix in CODE_SUFFIXES and "__pycache__" not in path.parts:
                files.append(path)
    return files


def _iter_web_files() -> list[Path]:
    return [
        path
        for path in sorted((ROOT / "web").rglob("*"))
        if path.is_file() and path.suffix in WEB_SUFFIXES
    ]


def _rdd_hits(files: list[Path]) -> list[str]:
    hits: list[str] = []
    for path in files:
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            lowered = line.lower()
            for token in FORBIDDEN_TOKENS:
                if token in lowered:
                    hits.append(f"{path.relative_to(ROOT)}:{number}: {token}")
    return hits


def _python_comment_hits(path: Path) -> list[str]:
    source = path.read_text(encoding="utf-8")
    hits: list[str] = []
    try:
        tokens = tokenize.generate_tokens(io.StringIO(source).readline)
        for token in tokens:
            if token.type == tokenize.COMMENT:
                hits.append(f"{path.relative_to(ROOT)}:{token.start[0]}: comment")
    except tokenize.TokenError as error:
        hits.append(f"{path.relative_to(ROOT)}: tokenize error {error}")
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            body = getattr(node, "body", [])
            if body and isinstance(body[0], ast.Expr):
                value = body[0].value
                if isinstance(value, ast.Constant) and isinstance(value.value, str):
                    hits.append(f"{path.relative_to(ROOT)}:{body[0].lineno}: docstring")
    return hits


def _script_comment_hits(path: Path) -> list[str]:
    source = path.read_text(encoding="utf-8")
    hits: list[str] = []
    index = 0
    line = 1
    quote = ""
    while index < len(source):
        character = source[index]
        if quote:
            if character == "\\":
                index += 2
                continue
            if character == quote:
                quote = ""
            if character == "\n":
                line += 1
            index += 1
            continue
        if character in ("'", '"', "`"):
            quote = character
            index += 1
            continue
        if character == "/" and index + 1 < len(source):
            following = source[index + 1]
            if following == "/":
                hits.append(f"{path.relative_to(ROOT)}:{line}: line comment")
                index += 2
                continue
            if following == "*":
                hits.append(f"{path.relative_to(ROOT)}:{line}: block comment")
                index += 2
                while index + 1 < len(source) and not (source[index] == "*" and source[index + 1] == "/"):
                    if source[index] == "\n":
                        line += 1
                    index += 1
                index += 2
                continue
        if character == "\n":
            line += 1
        index += 1
    return hits


def _sql_comment_hits(path: Path) -> list[str]:
    source = path.read_text(encoding="utf-8")
    hits: list[str] = []
    index = 0
    line = 1
    quote = False
    while index < len(source):
        character = source[index]
        if character == "'":
            if quote and index + 1 < len(source) and source[index + 1] == "'":
                index += 2
                continue
            quote = not quote
        elif not quote and character == "-" and index + 1 < len(source) and source[index + 1] == "-":
            hits.append(f"{path.relative_to(ROOT)}:{line}: line comment")
        elif not quote and character == "/" and index + 1 < len(source) and source[index + 1] == "*":
            hits.append(f"{path.relative_to(ROOT)}:{line}: block comment")
        if character == "\n":
            line += 1
        index += 1
    return hits


def _comment_hits(files: list[Path]) -> list[str]:
    hits: list[str] = []
    for path in files:
        if path.suffix == ".py":
            hits.extend(_python_comment_hits(path))
        elif path.suffix == ".sql":
            hits.extend(_sql_comment_hits(path))
        elif path.suffix in (".js", ".css"):
            hits.extend(_script_comment_hits(path))
        elif path.suffix == ".html":
            for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
                if "<!--" in line:
                    hits.append(f"{path.relative_to(ROOT)}:{number}: html comment")
    return hits


def _hsl(r: int, g: int, b: int) -> tuple[float, float, float]:
    red = r / 255.0
    green = g / 255.0
    blue = b / 255.0
    maximum = max(red, green, blue)
    minimum = min(red, green, blue)
    delta = maximum - minimum
    lightness = (maximum + minimum) / 2
    if delta == 0:
        return 0.0, 0.0, lightness
    saturation = delta / (1 - abs(2 * lightness - 1)) if lightness not in (0.0, 1.0) else 0.0
    if maximum == red:
        hue = ((green - blue) / delta) % 6
    elif maximum == green:
        hue = ((blue - red) / delta) + 2
    else:
        hue = ((red - green) / delta) + 4
    return hue * 60, saturation, lightness


def _green_triplet(r: int, g: int, b: int) -> bool:
    hue, saturation, lightness = _hsl(r, g, b)
    return 60.0 <= hue < 180.0 and saturation >= 0.2 and 0.04 <= lightness <= 0.96


def _hex_rgb(value: str) -> tuple[int, int, int] | None:
    digits = value[1:]
    if len(digits) == 3:
        return tuple(int(character * 2, 16) for character in digits)
    if len(digits) == 4:
        return tuple(int(character * 2, 16) for character in digits[:3])
    if len(digits) == 6:
        return int(digits[0:2], 16), int(digits[2:4], 16), int(digits[4:6], 16)
    if len(digits) == 8:
        return int(digits[0:2], 16), int(digits[2:4], 16), int(digits[4:6], 16)
    return None


def _green_hits() -> list[str]:
    hits: list[str] = []
    for path in _iter_web_files():
        text = path.read_text(encoding="utf-8")
        for number, line in enumerate(text.splitlines(), start=1):
            if GREEN_NAMES.search(line):
                hits.append(f"{path.relative_to(ROOT)}:{number}: green color name")
            for match in HEX_COLOR.finditer(line):
                triplet = _hex_rgb(match.group(0))
                if triplet and _green_triplet(*triplet):
                    hits.append(f"{path.relative_to(ROOT)}:{number}: green hex {match.group(0)}")
            for match in RGB_COLOR.finditer(line):
                if _green_triplet(int(match.group(1)), int(match.group(2)), int(match.group(3))):
                    hits.append(f"{path.relative_to(ROOT)}:{number}: green rgb {match.group(0)}")
            for match in HSL_COLOR.finditer(line):
                hue = float(match.group(1))
                if 60.0 <= hue < 180.0:
                    hits.append(f"{path.relative_to(ROOT)}:{number}: green hsl {match.group(0)}")
    return hits


def _require_empty(hits: list[str], label: str, inspected: int) -> None:
    if hits:
        preview = "; ".join(hits[:12])
        raise AssertionError(f"{label} found {len(hits)} violation(s): {preview}")
    print(f"[PASS] {label} across {inspected} inspected file(s)")


def run_green_audit() -> None:
    _require_empty(_green_hits(), "zero green colors under web/", len(_iter_web_files()))


def run_static_audits() -> None:
    files = _iter_code_files()
    _require_empty(_rdd_hits(files), "zero RDD/JVM tokens across source files", len(files))
    _require_empty(_comment_hits(files), "zero comments or docstrings across source files", len(files))
    run_green_audit()


def _validate_breakdown(
    scenario: str,
    result: dict[str, Any],
    expected: dict[str, Any],
) -> None:
    reconciliation = result["reconciliation"]
    _check(
        reconciliation["records_received"] == expected["received"],
        f"Scenario {scenario} reconciliation records_received matches raw landing count",
    )
    _check(
        reconciliation["records_valid"] == expected["valid"],
        f"Scenario {scenario} reconciliation records_valid matches independent audit",
    )
    _check(
        reconciliation["records_rejected"] == expected["rejected"],
        f"Scenario {scenario} reconciliation records_rejected matches independent audit",
    )
    _check(
        reconciliation["duplicate_count"] == expected["duplicate_count"],
        f"Scenario {scenario} duplicate_count matches independent audit",
    )
    _check(
        reconciliation["malformed_amount_count"] == expected["malformed_amount_count"],
        f"Scenario {scenario} malformed_amount_count matches independent audit",
    )
    _check(
        reconciliation["invalid_provider_count"] == expected["invalid_provider_count"],
        f"Scenario {scenario} invalid_provider_count matches independent audit",
    )
    _check(
        reconciliation["invalid_policy_count"] == expected["invalid_policy_count"],
        f"Scenario {scenario} invalid_policy_count matches independent audit",
    )
    _check(
        abs(float(reconciliation["rejection_rate"]) - expected["rejection_rate"]) <= 0.000001,
        f"Scenario {scenario} rejection_rate matches independent audit",
    )


def _run_pipeline(count: int) -> dict[str, dict[str, Any]]:
    print(f"[INFO] Generating raw landing files for {len(SCENARIOS)} scenarios at {count:,} claims each")
    started = time.perf_counter()
    generated = generate_all(ROOT / "data" / "raw", count=count, force=True)
    elapsed = time.perf_counter() - started
    _check(len(generated) == len(SCENARIOS), f"generated all {len(SCENARIOS)} raw scenarios")
    for scenario in SCENARIOS:
        root = ROOT / "data" / "raw" / scenario
        claims = _read_jsonl(root / "claims_raw.json")
        providers = _read_csv(root / "providers.csv")
        policies = _read_csv(root / "policy_tiers.csv")
        _check(len(claims) == count, f"Scenario {scenario} raw JSON contains {count:,} claims")
        _check(len(providers) >= 61, f"Scenario {scenario} providers.csv includes the full network")
        _check(len(policies) >= 240, f"Scenario {scenario} policy_tiers.csv includes all tiers")
    print(f"[INFO] Raw generation completed in {elapsed:.2f}s")

    db.reset_local_data()
    results: dict[str, dict[str, Any]] = {}
    for scenario in SCENARIOS:
        scenario_started = time.perf_counter()
        result = databricks_client.run_mock_scenario(scenario)
        scenario_elapsed = time.perf_counter() - scenario_started
        results[scenario] = result
        print(
            f"[INFO] Scenario {scenario} executed in {scenario_elapsed:.2f}s "
            f"with status {result['status']}"
        )
    return results


def _assert_normal(result: dict[str, Any], expected: dict[str, Any]) -> None:
    _check(result["status"] == "SUCCESS", "Scenario NORMAL run status is SUCCESS")
    _check(result["incident_id"] is None, "Scenario NORMAL raises no incident")
    _check(expected["rejection_rate"] < 0.02, "Scenario NORMAL rejection_rate < 0.02")
    _check(expected["top_key_skew_ratio"] < 0.05, "Scenario NORMAL top_key_skew_ratio < 0.05")
    _check(result["payout_exposure_usd"] == 0.0, "Scenario NORMAL reports zero payout exposure")
    _check(result["fact_rows_committed"] > 0, "Scenario NORMAL writes Gold fact rows")
    _validate_breakdown("NORMAL", result, expected)


def _assert_skew(result: dict[str, Any], expected: dict[str, Any]) -> None:
    incident = result["incident"] or {}
    _check(result["status"] == "DEGRADED", "Scenario A run status is DEGRADED")
    _check(bool(result["incident_id"]), "Scenario A raises an incident")
    _check(incident.get("error_class") == "DATA_SKEW", "Scenario A error_class is DATA_SKEW")
    _check(incident.get("sla_breached") == 1, "Scenario A marks the SLA as breached")
    _check(expected["top_key_skew_ratio"] >= 0.65, "Scenario A top-key skew is at least 65%")
    _check(expected["top_provider_id"] == "HOSP_999", "Scenario A hot provider is HOSP_999")
    _check(
        abs(float(incident.get("top_key_skew_ratio", 0.0)) - expected["top_key_skew_ratio"]) <= 0.000001,
        "Scenario A incident skew ratio matches raw landing distribution",
    )
    _check(
        abs(float(result["payout_exposure_usd"]) - expected["delayed_payout_usd"]) <= 0.011,
        "Scenario A payout exposure equals delayed HOSP_999 claims total",
        f"actual={result['payout_exposure_usd']} expected={expected['delayed_payout_usd']}",
    )
    _check(result["fact_rows_committed"] > 0, "Scenario A still commits Gold fact rows")
    _validate_breakdown("A", result, expected)


def _assert_schema(result: dict[str, Any], expected: dict[str, Any]) -> None:
    incident = result["incident"] or {}
    reconciliation = result["reconciliation"]
    _check(result["status"] == "FAILED", "Scenario B run status is FAILED")
    _check(bool(result["incident_id"]), "Scenario B raises an incident")
    _check(incident.get("error_class") == "SCHEMA_DRIFT", "Scenario B error_class is SCHEMA_DRIFT")
    _check(incident.get("sla_breached") == 1, "Scenario B marks the SLA as breached")
    _check(reconciliation["rejection_rate"] > 0.15, "Scenario B rejection_rate exceeds the 0.15 gate")
    _check(reconciliation["duplicate_count"] > 0, "Scenario B records duplicate claim IDs")
    _check(reconciliation["malformed_amount_count"] > 0, "Scenario B records malformed currency amounts")
    _check(reconciliation["invalid_policy_count"] > 0, "Scenario B records unmapped policy IDs")
    _check(incident.get("row_count_out") == 0, "Scenario B quality gate blocks Silver publication")
    _check(
        abs(float(result["payout_exposure_usd"]) - expected["rejected_risk_usd"]) <= 0.011,
        "Scenario B payout exposure equals rejected claim risk",
        f"actual={result['payout_exposure_usd']} expected={expected['rejected_risk_usd']}",
    )
    _check(result["fact_rows_committed"] == 0, "Scenario B commits no Gold fact rows")
    _validate_breakdown("B", result, expected)


def _assert_partial(result: dict[str, Any], expected: dict[str, Any]) -> None:
    incident = result["incident"] or {}
    committed = db.committed_fact_summary(result["run_id"])
    _check(result["status"] == "FAILED", "Scenario C run status is FAILED")
    _check(bool(result["incident_id"]), "Scenario C raises an incident")
    _check(
        incident.get("error_class") == "PARTIAL_PAYOUT_WRITE",
        "Scenario C error_class is PARTIAL_PAYOUT_WRITE",
    )
    _check(
        incident.get("write_mode") == "append_non_idempotent",
        "Scenario C write_mode is append_non_idempotent",
    )
    _check(
        incident.get("business_criticality") == "CRITICAL",
        "Scenario C business_criticality is CRITICAL",
    )
    _check(result["fact_rows_committed"] > 0, "Scenario C commits region batch 01")
    _check(result["fact_rows_uncommitted"] > 0, "Scenario C leaves region batch 02 uncommitted")
    _check(
        int(committed["fact_rows"]) == result["fact_rows_committed"],
        "Scenario C committed fact count matches SQLite Gold table",
    )
    _check(
        int(committed["batches"]) == 1,
        "Scenario C Gold table contains exactly one committed payout batch",
    )
    _check(
        abs(float(result["payout_exposure_usd"]) - float(committed["payout_usd"])) <= 0.011,
        "Scenario C payout exposure equals committed batch 01 payout",
        f"actual={result['payout_exposure_usd']} expected={committed['payout_usd']}",
    )
    _validate_breakdown("C", result, expected)


def _assert_database(results: dict[str, dict[str, Any]], count: int) -> None:
    counts = _table_counts()
    expected_bronze = sum(int(result["reconciliation"]["records_received"]) for result in results.values())
    expected_quarantine = sum(int(result["reconciliation"]["records_rejected"]) for result in results.values())
    expected_silver = sum(
        int(result["reconciliation"]["records_valid"])
        for scenario, result in results.items()
        if scenario != "B"
    )
    expected_facts = sum(int(result["fact_rows_committed"]) for result in results.values())
    _check(db.get_settings().local_cache_db.exists(), "SQLite cache database exists at backend/local_cache.db")
    _check(counts["pipeline_runs"] == 4, "SQLite pipeline_runs contains all four scenario runs")
    _check(counts["claims_bronze"] == expected_bronze, "SQLite claims_bronze matches received claim rows")
    _check(counts["claims_silver"] == expected_silver, "SQLite claims_silver matches published Silver rows")
    _check(
        counts["claims_quarantine"] == expected_quarantine,
        "SQLite claims_quarantine matches rejected claim rows",
    )
    _check(
        counts["claims_reconciliation_report"] == 4,
        "SQLite claims_reconciliation_report holds all four reports",
    )
    _check(counts["obs_bronze_runs"] == 11, "SQLite obs_bronze_runs holds every stage event")
    _check(counts["obs_silver_incidents"] == 3, "SQLite obs_silver_incidents holds scenarios A, B and C")
    _check(counts["fact_claims_payouts"] == expected_facts, "SQLite fact_claims_payouts matches committed rows")
    _check(counts["agent_diagnoses"] == 0, "Scenario execution leaves diagnoses to the agent API")
    _check(expected_bronze == count * 4, "Bronze received all four 50,000-claim landing files")
    _check(
        results["B"]["reconciliation"]["malformed_amount_count"]
        == int(count * 0.18),
        "Scenario B malformed amount injection count matches the 18% fault budget",
    )
    _check(
        results["B"]["reconciliation"]["duplicate_count"] == int(count * 0.04),
        "Scenario B duplicate injection count matches the 4% fault budget",
    )
    _check(
        results["B"]["reconciliation"]["invalid_policy_count"] == int(count * 0.06),
        "Scenario B unmapped policy injection count matches the 6% fault budget",
    )
    _check(
        float(results["A"]["incident"]["top_key_skew_ratio"]) >= 0.70,
        "Scenario A hot provider owns at least 70% of raw claims",
    )
    with db.session() as connection:
        stored = connection.execute(
            """
            SELECT scenario, records_received, records_valid, records_rejected, rejection_rate
            FROM claims_reconciliation_report
            ORDER BY scenario
            """
        ).fetchall()
    scenarios = [str(row["scenario"]) for row in stored]
    _check(scenarios == ["A", "B", "C", "NORMAL"], "SQLite reconciliation reports are queryable per scenario")


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the offline mock claims pipeline and verification gate")
    parser.add_argument("--count", type=int, default=50000)
    args = parser.parse_args()
    if args.count <= 0:
        print("[FAIL] --count must be positive")
        return 2
    print("[INFO] Phase 1 verification: raw landing, Medallion mock pipeline, reconciliation and SQLite seed")
    try:
        results = _run_pipeline(args.count)
        expected = {scenario: _expected_classification(scenario, ROOT / "data" / "raw") for scenario in SCENARIOS}
        _assert_normal(results["NORMAL"], expected["NORMAL"])
        _assert_skew(results["A"], expected["A"])
        _assert_schema(results["B"], expected["B"])
        _assert_partial(results["C"], expected["C"])
        _assert_database(results, args.count)
        run_static_audits()
        print("[PASS] Phase 1 verification gate completed successfully")
        return 0
    except Exception as error:
        print(f"[FAIL] Phase 1 verification gate failed: {type(error).__name__}: {error}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
