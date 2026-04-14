import argparse
import json
import sys
import time
from collections import Counter, defaultdict
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import main_logic
from db import run_query_with_columns
from evals.benchmark_cases import BenchmarkCase, build_cases
from memory import Memory


RESULTS_DIR = Path(__file__).resolve().parent / "results"
FULL_AUDIT_CASE_IDS = {
    "C001",
    "C007",
    "C013",
    "C018",
    "C022",
    "C026",
    "C036",
    "C045",
    "C055",
    "C075",
    "C092",
    "C093",
    "C094",
    "C097",
    "C102",
    "C105",
    "C108",
    "C111",
    "C114",
}


def normalize_value(value):
    if isinstance(value, Decimal):
        return round(float(value), 2)
    if isinstance(value, float):
        return round(value, 2)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def normalize_rows(rows):
    return [tuple(normalize_value(value) for value in row) for row in rows]


def compare_rows(case: BenchmarkCase, actual_rows, expected_rows):
    actual = normalize_rows(actual_rows)
    expected = normalize_rows(expected_rows)

    if case.compare_mode == "exact":
        return actual == expected

    if case.compare_mode == "unordered":
        return sorted(actual) == sorted(expected)

    raise ValueError(f"Unknown compare mode: {case.compare_mode}")


def summarize_failure(case, response, expected_rows):
    if response.get("error"):
        return f"runtime_error: {response['error']}"

    actual = normalize_rows(response["results"])
    expected = normalize_rows(expected_rows)

    if not actual and expected:
        return "unexpected_empty_result"
    if actual and not expected:
        return "unexpected_nonempty_result"
    if len(actual) != len(expected):
        return f"row_count_mismatch: actual={len(actual)} expected={len(expected)}"
    return "content_mismatch"


def build_summary(rows):
    total = len(rows)
    passed = sum(1 for row in rows if row["passed"])
    failed = total - passed
    by_category = defaultdict(lambda: {"total": 0, "passed": 0, "failed": 0})
    failure_types = Counter()

    for row in rows:
        category_stats = by_category[row["category"]]
        category_stats["total"] += 1
        category_stats["passed"] += int(row["passed"])
        category_stats["failed"] += int(not row["passed"])
        if not row["passed"]:
            failure_types[row["failure_type"]] += 1

    return {
        "total": total,
        "passed": passed,
        "failed": failed,
        "pass_rate": round((passed / total) * 100, 2) if total else 0.0,
        "failure_types": dict(failure_types.most_common()),
        "by_category": {
            category: {
                **stats,
                "pass_rate": round((stats["passed"] / stats["total"]) * 100, 2)
                if stats["total"]
                else 0.0,
            }
            for category, stats in sorted(by_category.items())
        },
    }


def write_markdown_summary(path, summary, failures):
    lines = [
        "# Benchmark Summary",
        "",
        f"- Total cases: {summary['total']}",
        f"- Passed: {summary['passed']}",
        f"- Failed: {summary['failed']}",
        f"- Pass rate: {summary['pass_rate']}%",
        "",
        "## Category Breakdown",
        "",
    ]

    for category, stats in summary["by_category"].items():
        lines.append(
            f"- {category}: {stats['passed']}/{stats['total']} passed ({stats['pass_rate']}%)"
        )

    if summary["failure_types"]:
        lines.extend(["", "## Failure Types", ""])
        for failure_type, count in summary["failure_types"].items():
            lines.append(f"- {failure_type}: {count}")

    if failures:
        lines.extend(["", "## Sample Failures", ""])
        for failure in failures[:15]:
            lines.extend(
                [
                    f"### {failure['case_id']} - {failure['question']}",
                    f"- Category: {failure['category']}",
                    f"- Failure: {failure['failure_type']}",
                    f"- SQL: `{failure['actual_sql']}`",
                    f"- Expected sample: `{failure['expected_rows'][:3]}`",
                    f"- Actual sample: `{failure['actual_rows'][:3]}`",
                    "",
                ]
            )

    path.write_text("\n".join(lines) + "\n")


def run_fast_benchmark():
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    cases = build_cases()
    session_memories = {}
    original_ask_llm = main_logic.ask_llm
    original_generate_ai_insights = main_logic.generate_ai_insights

    # Skip answer generation and AI insights to keep the 100+ case benchmark tractable.
    main_logic.ask_llm = lambda _prompt: ""
    main_logic.generate_ai_insights = lambda *_args, **_kwargs: ""

    rows = []
    try:
        for index, case in enumerate(cases, start=1):
            memory = session_memories.setdefault(case.session_id, Memory()) if case.session_id else Memory()
            _, expected_rows = run_query_with_columns(case.expected_sql)

            started_at = time.time()
            response = main_logic.process_question(case.question, memory=memory, show_chart=False)
            duration_s = round(time.time() - started_at, 2)

            passed = compare_rows(case, response["results"], expected_rows) and response.get("error") is None
            failure_type = None if passed else summarize_failure(case, response, expected_rows)
            row = {
                "case_id": case.case_id,
                "category": case.category,
                "session_id": case.session_id,
                "question": case.question,
                "passed": passed,
                "failure_type": failure_type,
                "duration_s": duration_s,
                "actual_sql": response["sql_query"],
                "expected_sql": case.expected_sql,
                "actual_rows": normalize_rows(response["results"]),
                "expected_rows": normalize_rows(expected_rows),
                "error": response.get("error"),
            }
            rows.append(row)

            status = "PASS" if passed else "FAIL"
            print(f"[{index:03d}/{len(cases):03d}] {status} {case.case_id} {case.question} ({duration_s}s)", flush=True)
    finally:
        main_logic.ask_llm = original_ask_llm
        main_logic.generate_ai_insights = original_generate_ai_insights

    summary = build_summary(rows)
    json_path = RESULTS_DIR / "benchmark_fast.json"
    md_path = RESULTS_DIR / "benchmark_fast.md"
    json_path.write_text(json.dumps({"summary": summary, "results": rows}, indent=2))
    failures = [row for row in rows if not row["passed"]]
    write_markdown_summary(md_path, summary, failures)
    print(json.dumps(summary, indent=2))


def run_full_answer_audit():
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    all_cases = build_cases()
    session_memories = {}
    rows = []
    audited_index = 0

    for case in all_cases:
        memory = session_memories.setdefault(case.session_id, Memory()) if case.session_id else Memory()
        started_at = time.time()
        response = main_logic.process_question(case.question, memory=memory, show_chart=False)
        duration_s = round(time.time() - started_at, 2)

        if case.case_id not in FULL_AUDIT_CASE_IDS:
            continue

        audited_index += 1
        _, expected_rows = run_query_with_columns(case.expected_sql)
        row = {
            "case_id": case.case_id,
            "category": case.category,
            "session_id": case.session_id,
            "question": case.question,
            "duration_s": duration_s,
            "passed_result_check": compare_rows(case, response["results"], expected_rows) and response.get("error") is None,
            "actual_sql": response["sql_query"],
            "expected_sql": case.expected_sql,
            "answer": response["answer"],
            "basic_insights": response.get("basic_insights", []),
            "anomalies": response.get("anomalies", []),
            "ai_insights": response.get("ai_insights", ""),
            "actual_rows": normalize_rows(response["results"]),
            "expected_rows": normalize_rows(expected_rows),
            "error": response.get("error"),
        }
        rows.append(row)
        status = "PASS" if row["passed_result_check"] else "FAIL"
        print(
            f"[AUDIT {audited_index:02d}/{len(FULL_AUDIT_CASE_IDS):02d}] "
            f"{status} {case.case_id} {case.question} ({duration_s}s)",
            flush=True,
        )

    json_path = RESULTS_DIR / "answer_audit.json"
    md_path = RESULTS_DIR / "answer_audit.md"
    json_path.write_text(json.dumps(rows, indent=2))

    lines = ["# Answer Audit", ""]
    for row in rows:
        basic_lines = [f"- {item}" for item in row["basic_insights"]] or ["- (none)"]
        anomaly_lines = [f"- {item}" for item in row["anomalies"]] or ["- (none)"]
        lines.extend(
            [
                f"## {row['case_id']} - {row['question']}",
                f"- Category: {row['category']}",
                f"- Result check: {'PASS' if row['passed_result_check'] else 'FAIL'}",
                f"- Duration: {row['duration_s']}s",
                f"- SQL: `{row['actual_sql']}`",
                f"- Expected sample: `{row['expected_rows'][:3]}`",
                f"- Actual sample: `{row['actual_rows'][:3]}`",
                "",
                "### Answer",
                row["answer"] or "(empty)",
                "",
                "### Basic Insights",
                *basic_lines,
                "",
                "### Anomalies",
                *anomaly_lines,
                "",
                "### Deeper Insights",
                row["ai_insights"] or "(none)",
                "",
            ]
        )
    md_path.write_text("\n".join(lines) + "\n")
    print(f"Wrote audit results to {md_path}")


def main():
    parser = argparse.ArgumentParser(description="Run DataSage benchmark evaluations.")
    parser.add_argument(
        "--mode",
        choices=("fast", "audit"),
        default="fast",
        help="`fast` runs the 100+ case SQL/result benchmark. `audit` runs a smaller full-response audit.",
    )
    args = parser.parse_args()

    if args.mode == "fast":
        run_fast_benchmark()
    else:
        run_full_answer_audit()


if __name__ == "__main__":
    main()
