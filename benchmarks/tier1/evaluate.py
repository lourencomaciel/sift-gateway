"""Answer evaluation and report generation for Tier 1 benchmark."""

from __future__ import annotations

import json
import re
from typing import Any


def _extract_first_number(text: str) -> float | None:
    """Extract the first numeric token from text."""
    text = text.replace(",", "")
    match = re.search(r"-?\d+(?:\.\d+)?", text)
    if match:
        return float(match.group())
    return None


def match_number(
    llm_answer: str,
    gold_answer: str,
    *,
    tolerance: float = 0.01,
) -> bool:
    """Check if LLM answer matches gold numerically within tolerance."""
    try:
        gold_val = float(gold_answer.replace(",", ""))
    except ValueError:
        return False

    try:
        llm_val = float(llm_answer.strip().replace(",", ""))
    except ValueError:
        llm_val_maybe = _extract_first_number(llm_answer)
        if llm_val_maybe is None:
            return False
        llm_val = llm_val_maybe

    if gold_val == 0:
        return abs(llm_val) <= tolerance
    return abs(llm_val - gold_val) <= tolerance


def match_string(llm_answer: str, gold_answer: str) -> bool:
    """Check if LLM answer matches gold string."""
    llm_clean = llm_answer.strip().lower()
    gold_clean = gold_answer.strip().lower()
    if not llm_clean:
        return False
    if llm_clean == gold_clean:
        return True
    if gold_clean in llm_clean:
        return True
    return llm_clean in gold_clean


def match_list(llm_answer: str, gold_answer: str) -> bool:
    """Check if LLM answer matches gold list (set comparison)."""
    try:
        llm_list = json.loads(llm_answer)
    except json.JSONDecodeError:
        return False
    try:
        gold_list = json.loads(gold_answer)
    except json.JSONDecodeError:
        return False
    if not isinstance(llm_list, list) or not isinstance(gold_list, list):
        return False
    llm_set = {str(x).strip().lower() for x in llm_list}
    gold_set = {str(x).strip().lower() for x in gold_list}
    return llm_set == gold_set


def evaluate_answer(
    llm_answer: str,
    gold_answer: str,
    *,
    answer_type: str,
    tolerance: float = 0.01,
) -> bool:
    """Evaluate a single answer against gold."""
    if answer_type == "number":
        return match_number(llm_answer, gold_answer, tolerance=tolerance)
    if answer_type == "list":
        return match_list(llm_answer, gold_answer)
    return match_string(llm_answer, gold_answer)


def build_report(
    results: list[dict[str, Any]],
    *,
    model: str,
) -> dict[str, Any]:
    """Build a summary report from individual question results."""
    baseline_results = [r for r in results if r.get("condition") == "baseline"]
    sift_results = [r for r in results if r.get("condition") == "sift"]

    baseline_correct = sum(1 for r in baseline_results if r.get("correct"))
    sift_correct = sum(1 for r in sift_results if r.get("correct"))
    baseline_total = len(baseline_results)
    sift_total = len(sift_results)

    baseline_input_tokens = sum(
        r.get("input_tokens", 0) for r in baseline_results
    )
    baseline_output_tokens = sum(
        r.get("output_tokens", 0) for r in baseline_results
    )
    sift_input_tokens = sum(r.get("input_tokens", 0) for r in sift_results)
    sift_output_tokens = sum(r.get("output_tokens", 0) for r in sift_results)

    token_reduction_pct = (
        (1 - sift_input_tokens / baseline_input_tokens) * 100
        if baseline_input_tokens > 0
        else 0
    )

    # Per-dataset breakdown
    datasets: dict[str, dict[str, Any]] = {}
    for r in results:
        ds = r.get("dataset", "unknown")
        cond = r.get("condition", "unknown")
        if ds not in datasets:
            datasets[ds] = {
                "baseline_correct": 0,
                "baseline_total": 0,
                "sift_correct": 0,
                "sift_total": 0,
                "baseline_input_tokens": 0,
                "sift_input_tokens": 0,
            }
        entry = datasets[ds]
        if cond == "baseline":
            entry["baseline_total"] += 1
            if r.get("correct"):
                entry["baseline_correct"] += 1
            entry["baseline_input_tokens"] += r.get("input_tokens", 0)
        elif cond == "sift":
            entry["sift_total"] += 1
            if r.get("correct"):
                entry["sift_correct"] += 1
            entry["sift_input_tokens"] += r.get("input_tokens", 0)

    # Per-question-type breakdown
    qtypes: dict[str, dict[str, Any]] = {}
    for r in results:
        qt = r.get("question_type", "unknown")
        cond = r.get("condition", "unknown")
        if qt not in qtypes:
            qtypes[qt] = {
                "baseline_correct": 0,
                "baseline_total": 0,
                "sift_correct": 0,
                "sift_total": 0,
            }
        entry = qtypes[qt]
        if cond == "baseline":
            entry["baseline_total"] += 1
            if r.get("correct"):
                entry["baseline_correct"] += 1
        elif cond == "sift":
            entry["sift_total"] += 1
            if r.get("correct"):
                entry["sift_correct"] += 1

    return {
        "model": model,
        "summary": {
            "baseline_accuracy": (f"{baseline_correct}/{baseline_total}"),
            "baseline_accuracy_pct": round(
                baseline_correct / baseline_total * 100, 1
            )
            if baseline_total
            else 0,
            "sift_accuracy": f"{sift_correct}/{sift_total}",
            "sift_accuracy_pct": round(sift_correct / sift_total * 100, 1)
            if sift_total
            else 0,
            "baseline_input_tokens": baseline_input_tokens,
            "baseline_output_tokens": baseline_output_tokens,
            "sift_input_tokens": sift_input_tokens,
            "sift_output_tokens": sift_output_tokens,
            "token_reduction_pct": round(token_reduction_pct, 1),
        },
        "per_dataset": datasets,
        "per_question_type": qtypes,
        "results": results,
    }


def print_summary_table(report: dict[str, Any]) -> None:
    """Print a human-readable summary table to stdout."""
    summary = report["summary"]
    print("\n" + "=" * 70)
    print(f"  Tier 1 Benchmark Results — {report['model']}")
    print("=" * 70)
    print(
        f"\n  {'Condition':<12} {'Accuracy':>10} "
        f"{'Input Tok':>12} {'Output Tok':>12}"
    )
    print("  " + "-" * 50)
    print(
        f"  {'Baseline':<12} "
        f"{summary['baseline_accuracy']:>10} "
        f"{summary['baseline_input_tokens']:>12,} "
        f"{summary['baseline_output_tokens']:>12,}"
    )
    print(
        f"  {'Sift':<12} "
        f"{summary['sift_accuracy']:>10} "
        f"{summary['sift_input_tokens']:>12,} "
        f"{summary['sift_output_tokens']:>12,}"
    )
    print(f"\n  Token reduction: {summary['token_reduction_pct']}%")

    # Per-dataset
    print(f"\n  {'Dataset':<15} {'Baseline':>10} {'Sift':>10}")
    print("  " + "-" * 38)
    for ds_name, ds in sorted(report["per_dataset"].items()):
        b_acc = (
            f"{ds['baseline_correct']}/{ds['baseline_total']}"
            if ds["baseline_total"]
            else "—"
        )
        s_acc = (
            f"{ds['sift_correct']}/{ds['sift_total']}"
            if ds["sift_total"]
            else "—"
        )
        print(f"  {ds_name:<15} {b_acc:>10} {s_acc:>10}")

    # Per-question-type
    print(f"\n  {'Question Type':<15} {'Baseline':>10} {'Sift':>10}")
    print("  " + "-" * 38)
    for qt_name, qt in sorted(report["per_question_type"].items()):
        b_acc = (
            f"{qt['baseline_correct']}/{qt['baseline_total']}"
            if qt["baseline_total"]
            else "—"
        )
        s_acc = (
            f"{qt['sift_correct']}/{qt['sift_total']}"
            if qt["sift_total"]
            else "—"
        )
        print(f"  {qt_name:<15} {b_acc:>10} {s_acc:>10}")

    print("=" * 70 + "\n")
