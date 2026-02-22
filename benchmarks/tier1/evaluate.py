"""Answer evaluation and report generation for Tier 1 benchmark."""

from __future__ import annotations

import json
import re
import statistics
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
    tolerance: float = 0.0,
) -> bool:
    """Check if LLM answer matches gold numerically within tolerance.

    Tolerance is absolute.  For example, ``tolerance=0.01`` accepts
    answers within +-0.01 of gold.  The default ``0.0`` requires
    exact numeric match.
    """
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

    return abs(llm_val - gold_val) <= tolerance


def match_string(llm_answer: str, gold_answer: str) -> bool:
    """Check if LLM answer matches gold string.

    Accepts exact match or gold appearing as a whole-word sequence
    inside the LLM answer (the LLM elaborated).  Uses word-boundary
    anchors so short golds like ``"ak"`` do not false-match inside
    unrelated words like ``"make"``.
    """
    llm_clean = llm_answer.strip().lower()
    gold_clean = gold_answer.strip().lower()
    if not llm_clean or not gold_clean:
        return llm_clean == gold_clean
    if llm_clean == gold_clean:
        return True
    # Use \b when the gold edge is a word char; otherwise require
    # whitespace or string boundary so non-word chars like "+"
    # don't mis-anchor.
    start = (
        r"\b"
        if gold_clean[0].isalnum() or gold_clean[0] == "_"
        else r"(?:^|(?<=\s))"
    )
    end = (
        r"\b"
        if gold_clean[-1].isalnum() or gold_clean[-1] == "_"
        else r"(?:$|(?=\s))"
    )
    pattern = start + re.escape(gold_clean) + end
    return re.search(pattern, llm_clean) is not None


_TRUE_VARIANTS = frozenset({"yes", "true", "1"})
_FALSE_VARIANTS = frozenset({"no", "false", "0"})


def match_boolean(llm_answer: str, gold_answer: str) -> bool:
    """Check if LLM answer matches gold boolean value.

    Recognises ``yes/true/1`` as truthy and ``no/false/0`` as falsy.
    Both inputs are stripped and lowercased before comparison.
    Returns ``False`` when gold is not a recognised boolean variant.
    """
    llm_clean = llm_answer.strip().lower()
    gold_clean = gold_answer.strip().lower()
    if gold_clean in _TRUE_VARIANTS:
        return llm_clean in _TRUE_VARIANTS
    if gold_clean in _FALSE_VARIANTS:
        return llm_clean in _FALSE_VARIANTS
    return False


def match_list(llm_answer: str, gold_answer: str) -> bool:
    """Check if LLM answer matches gold list (order-insensitive).

    Uses sorted multiset comparison so duplicate elements are
    preserved (``["a", "a"]`` does NOT match ``["a"]``).
    """
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
    llm_norm = sorted(str(x).strip().lower() for x in llm_list)
    gold_norm = sorted(str(x).strip().lower() for x in gold_list)
    return llm_norm == gold_norm


def evaluate_answer(
    llm_answer: str,
    gold_answer: str,
    *,
    answer_type: str,
    tolerance: float = 0.0,
) -> bool:
    """Evaluate a single answer against gold."""
    if answer_type == "number":
        return match_number(llm_answer, gold_answer, tolerance=tolerance)
    if answer_type == "boolean":
        return match_boolean(llm_answer, gold_answer)
    if answer_type == "list":
        return match_list(llm_answer, gold_answer)
    return match_string(llm_answer, gold_answer)


def _latency_percentiles(
    latencies: list[float],
) -> dict[str, float | int]:
    """Compute p50, p90, and mean latency from a list of values.

    Returns an empty dict when *latencies* is empty.  p50 uses
    ``statistics.median`` (interpolated midpoint for even *n*);
    p90 uses nearest-rank indexing.
    """
    if not latencies:
        return {}
    s = sorted(latencies)
    n = len(s)
    p90_idx = min(n - 1, int(n * 0.9))
    return {
        "p50_ms": round(statistics.median(s), 1),
        "p90_ms": round(s[p90_idx], 1),
        "mean_ms": round(sum(s) / n, 1),
        "count": n,
    }


def build_report(
    results: list[dict[str, Any]],
    *,
    model: str,
    question_hash: str = "",
) -> dict[str, Any]:
    """Build a summary report from individual question results."""
    baseline_results = [r for r in results if r.get("condition") == "baseline"]
    sift_results = [r for r in results if r.get("condition") == "sift"]

    baseline_correct = sum(1 for r in baseline_results if r.get("correct"))
    sift_correct = sum(1 for r in sift_results if r.get("correct"))
    baseline_total = len(baseline_results)
    sift_total = len(sift_results)

    baseline_errors = sum(
        1 for r in baseline_results if not r.get("attempted", True)
    )
    sift_errors = sum(1 for r in sift_results if not r.get("attempted", True))

    baseline_input_tokens = sum(
        r.get("input_tokens", 0) for r in baseline_results
    )
    baseline_output_tokens = sum(
        r.get("output_tokens", 0) for r in baseline_results
    )
    sift_input_tokens = sum(r.get("input_tokens", 0) for r in sift_results)
    sift_output_tokens = sum(r.get("output_tokens", 0) for r in sift_results)

    token_reduction_pct = (
        max(0, (1 - sift_input_tokens / baseline_input_tokens) * 100)
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
                "baseline_errors": 0,
                "sift_correct": 0,
                "sift_total": 0,
                "sift_errors": 0,
                "baseline_input_tokens": 0,
                "sift_input_tokens": 0,
            }
        entry = datasets[ds]
        if cond == "baseline":
            entry["baseline_total"] += 1
            if r.get("correct"):
                entry["baseline_correct"] += 1
            if not r.get("attempted", True):
                entry["baseline_errors"] += 1
            entry["baseline_input_tokens"] += r.get("input_tokens", 0)
        elif cond == "sift":
            entry["sift_total"] += 1
            if r.get("correct"):
                entry["sift_correct"] += 1
            if not r.get("attempted", True):
                entry["sift_errors"] += 1
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

    # Latency percentiles
    baseline_latencies = [
        r["latency_ms"]
        for r in baseline_results
        if r.get("latency_ms") is not None
    ]
    sift_latencies = [
        r["latency_ms"] for r in sift_results if r.get("latency_ms") is not None
    ]

    # Per-difficulty breakdown
    diffs: dict[str, dict[str, Any]] = {}
    for r in results:
        d = str(r.get("difficulty", 1))
        cond = r.get("condition", "unknown")
        if d not in diffs:
            diffs[d] = {
                "baseline_correct": 0,
                "baseline_total": 0,
                "sift_correct": 0,
                "sift_total": 0,
                "sift_retries": 0,
                "baseline_latencies": [],
                "sift_latencies": [],
            }
        entry = diffs[d]
        if cond == "baseline":
            entry["baseline_total"] += 1
            if r.get("correct"):
                entry["baseline_correct"] += 1
            if r.get("latency_ms") is not None:
                entry["baseline_latencies"].append(r["latency_ms"])
        elif cond == "sift":
            entry["sift_total"] += 1
            if r.get("correct"):
                entry["sift_correct"] += 1
            entry["sift_retries"] += r.get("retries", 0)
            if r.get("latency_ms") is not None:
                entry["sift_latencies"].append(r["latency_ms"])

    per_difficulty: dict[str, dict[str, Any]] = {}
    for d, entry in diffs.items():
        per_difficulty[d] = {
            "baseline_correct": entry["baseline_correct"],
            "baseline_total": entry["baseline_total"],
            "sift_correct": entry["sift_correct"],
            "sift_total": entry["sift_total"],
            "sift_retries": entry["sift_retries"],
            "baseline_latency": _latency_percentiles(
                entry["baseline_latencies"],
            ),
            "sift_latency": _latency_percentiles(
                entry["sift_latencies"],
            ),
        }

    return {
        "model": model,
        "question_set_hash": question_hash,
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
            "baseline_errors": baseline_errors,
            "baseline_attempted": baseline_total - baseline_errors,
            "sift_errors": sift_errors,
            "sift_attempted": sift_total - sift_errors,
        },
        "latency": {
            "baseline": _latency_percentiles(baseline_latencies),
            "sift": _latency_percentiles(sift_latencies),
        },
        "per_dataset": datasets,
        "per_question_type": qtypes,
        "per_difficulty": per_difficulty,
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
        f"{'Errors':>8} "
        f"{'Input Tok':>12} {'Output Tok':>12}"
    )
    print("  " + "-" * 58)
    print(
        f"  {'Baseline':<12} "
        f"{summary['baseline_accuracy']:>10} "
        f"{summary.get('baseline_errors', 0):>8} "
        f"{summary['baseline_input_tokens']:>12,} "
        f"{summary['baseline_output_tokens']:>12,}"
    )
    print(
        f"  {'Sift':<12} "
        f"{summary['sift_accuracy']:>10} "
        f"{summary.get('sift_errors', 0):>8} "
        f"{summary['sift_input_tokens']:>12,} "
        f"{summary['sift_output_tokens']:>12,}"
    )
    print(f"\n  Token reduction: {summary['token_reduction_pct']}%")

    # Latency percentiles
    latency = report.get("latency", {})
    bl = latency.get("baseline", {})
    sl = latency.get("sift", {})
    if bl or sl:
        print(
            f"\n  {'Latency':<12} {'p50 (ms)':>10} "
            f"{'p90 (ms)':>10} {'mean (ms)':>10}"
        )
        print("  " + "-" * 44)
        if bl:
            print(
                f"  {'Baseline':<12} "
                f"{bl.get('p50_ms', 0):>10.1f} "
                f"{bl.get('p90_ms', 0):>10.1f} "
                f"{bl.get('mean_ms', 0):>10.1f}"
            )
        if sl:
            print(
                f"  {'Sift':<12} "
                f"{sl.get('p50_ms', 0):>10.1f} "
                f"{sl.get('p90_ms', 0):>10.1f} "
                f"{sl.get('mean_ms', 0):>10.1f}"
            )

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
        errors: list[str] = []
        if ds.get("baseline_errors"):
            errors.append(f"b_err={ds['baseline_errors']}")
        if ds.get("sift_errors"):
            errors.append(f"s_err={ds['sift_errors']}")
        suffix = f"  ({', '.join(errors)})" if errors else ""
        print(f"  {ds_name:<15} {b_acc:>10} {s_acc:>10}{suffix}")

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

    # Per-difficulty
    per_diff = report.get("per_difficulty", {})
    if per_diff:
        diff_labels = {"1": "easy", "2": "medium", "3": "hard"}
        print(
            f"\n  {'Difficulty':<12} {'Baseline':>10} "
            f"{'Sift':>10} {'Retries':>8}"
        )
        print("  " + "-" * 44)
        for d, dd in sorted(per_diff.items()):
            label = diff_labels.get(d, f"L{d}")
            b_acc = (
                f"{dd['baseline_correct']}/{dd['baseline_total']}"
                if dd["baseline_total"]
                else "—"
            )
            s_acc = (
                f"{dd['sift_correct']}/{dd['sift_total']}"
                if dd["sift_total"]
                else "—"
            )
            print(
                f"  {label:<12} {b_acc:>10} {s_acc:>10} {dd['sift_retries']:>8}"
            )

    print("=" * 70 + "\n")
