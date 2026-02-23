"""Answer evaluation and report generation for Tier 1 benchmark."""

from __future__ import annotations

import json
import math
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


def latency_percentiles(
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
    p90_idx = math.ceil(n * 0.9) - 1
    return {
        "p50_ms": round(statistics.median(s), 1),
        "p90_ms": round(s[p90_idx], 1),
        "mean_ms": round(sum(s) / n, 1),
        "count": n,
    }


def _condition_stats(
    cond_results: list[dict[str, Any]],
) -> dict[str, Any]:
    """Compute aggregate stats for a single condition."""
    total = len(cond_results)
    correct = sum(1 for r in cond_results if r.get("correct"))
    errors = sum(1 for r in cond_results if not r.get("attempted", True))
    input_tokens = sum(r.get("input_tokens", 0) for r in cond_results)
    output_tokens = sum(r.get("output_tokens", 0) for r in cond_results)
    return {
        "correct": correct,
        "total": total,
        "errors": errors,
        "attempted": total - errors,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "accuracy": f"{correct}/{total}",
        "accuracy_pct": round(correct / total * 100, 1) if total else 0,
    }


def build_report(
    results: list[dict[str, Any]],
    *,
    model: str,
    question_hash: str = "",
) -> dict[str, Any]:
    """Build a summary report from individual question results."""
    # Collect results per condition.
    by_condition: dict[str, list[dict[str, Any]]] = {}
    for r in results:
        cond = r.get("condition", "unknown")
        by_condition.setdefault(cond, []).append(r)

    baseline_results = by_condition.get("baseline", [])
    sift_results = by_condition.get("sift", [])
    multistep_results = by_condition.get("sift_multistep", [])

    bs = _condition_stats(baseline_results)
    ss = _condition_stats(sift_results)
    ms = _condition_stats(multistep_results)

    token_reduction_pct = (
        max(
            0,
            (1 - ss["input_tokens"] / bs["input_tokens"]) * 100,
        )
        if bs["input_tokens"] > 0
        else 0
    )

    # Per-dataset breakdown
    _cond_keys = ["baseline", "sift", "sift_multistep"]
    datasets: dict[str, dict[str, Any]] = {}
    for r in results:
        ds = r.get("dataset", "unknown")
        cond = r.get("condition", "unknown")
        if ds not in datasets:
            entry: dict[str, Any] = {}
            for ck in _cond_keys:
                entry[f"{ck}_correct"] = 0
                entry[f"{ck}_total"] = 0
                entry[f"{ck}_errors"] = 0
                entry[f"{ck}_input_tokens"] = 0
            datasets[ds] = entry
        entry = datasets[ds]
        if cond in _cond_keys:
            entry[f"{cond}_total"] += 1
            if r.get("correct"):
                entry[f"{cond}_correct"] += 1
            if not r.get("attempted", True):
                entry[f"{cond}_errors"] += 1
            entry[f"{cond}_input_tokens"] += r.get("input_tokens", 0)

    # Per-question-type breakdown
    qtypes: dict[str, dict[str, Any]] = {}
    for r in results:
        qt = r.get("question_type", "unknown")
        cond = r.get("condition", "unknown")
        if qt not in qtypes:
            qt_entry: dict[str, Any] = {}
            for ck in _cond_keys:
                qt_entry[f"{ck}_correct"] = 0
                qt_entry[f"{ck}_total"] = 0
            qtypes[qt] = qt_entry
        qt_entry = qtypes[qt]
        if cond in _cond_keys:
            qt_entry[f"{cond}_total"] += 1
            if r.get("correct"):
                qt_entry[f"{cond}_correct"] += 1

    # Latency percentiles
    latency_by_cond: dict[str, list[float]] = {}
    for r in results:
        cond = r.get("condition", "unknown")
        if r.get("latency_ms") is not None:
            latency_by_cond.setdefault(cond, []).append(r["latency_ms"])

    # Per-difficulty breakdown
    diffs: dict[str, dict[str, Any]] = {}
    for r in results:
        d = str(r.get("difficulty", 1))
        cond = r.get("condition", "unknown")
        if d not in diffs:
            diff_entry: dict[str, Any] = {}
            for ck in _cond_keys:
                diff_entry[f"{ck}_correct"] = 0
                diff_entry[f"{ck}_total"] = 0
                diff_entry[f"{ck}_retries"] = 0
                diff_entry[f"{ck}_latencies"] = []
            diffs[d] = diff_entry
        diff_entry = diffs[d]
        if cond in _cond_keys:
            diff_entry[f"{cond}_total"] += 1
            if r.get("correct"):
                diff_entry[f"{cond}_correct"] += 1
            diff_entry[f"{cond}_retries"] += r.get("retries", 0)
            if r.get("latency_ms") is not None:
                diff_entry[f"{cond}_latencies"].append(r["latency_ms"])

    per_difficulty: dict[str, dict[str, Any]] = {}
    for d, diff_entry in diffs.items():
        pd_entry: dict[str, Any] = {}
        for ck in _cond_keys:
            pd_entry[f"{ck}_correct"] = diff_entry[f"{ck}_correct"]
            pd_entry[f"{ck}_total"] = diff_entry[f"{ck}_total"]
            pd_entry[f"{ck}_retries"] = diff_entry[f"{ck}_retries"]
            pd_entry[f"{ck}_latency"] = latency_percentiles(
                diff_entry[f"{ck}_latencies"],
            )
        # Legacy keys for backward compat.
        pd_entry["sift_retries"] = pd_entry["sift_retries"]
        per_difficulty[d] = pd_entry

    latency_report: dict[str, Any] = {
        # Always include baseline and sift for backward compat.
        "baseline": latency_percentiles(
            latency_by_cond.get("baseline", []),
        ),
        "sift": latency_percentiles(
            latency_by_cond.get("sift", []),
        ),
    }
    ms_lat = latency_by_cond.get("sift_multistep", [])
    if ms_lat:
        latency_report["sift_multistep"] = latency_percentiles(
            ms_lat,
        )

    return {
        "model": model,
        "question_set_hash": question_hash,
        "summary": {
            # Legacy baseline/sift keys for backward compat.
            "baseline_accuracy": bs["accuracy"],
            "baseline_accuracy_pct": bs["accuracy_pct"],
            "sift_accuracy": ss["accuracy"],
            "sift_accuracy_pct": ss["accuracy_pct"],
            "baseline_input_tokens": bs["input_tokens"],
            "baseline_output_tokens": bs["output_tokens"],
            "sift_input_tokens": ss["input_tokens"],
            "sift_output_tokens": ss["output_tokens"],
            "token_reduction_pct": round(token_reduction_pct, 1),
            "baseline_errors": bs["errors"],
            "baseline_attempted": bs["attempted"],
            "sift_errors": ss["errors"],
            "sift_attempted": ss["attempted"],
            # Multistep additions.
            "sift_multistep_accuracy": ms["accuracy"],
            "sift_multistep_accuracy_pct": ms["accuracy_pct"],
            "sift_multistep_input_tokens": ms["input_tokens"],
            "sift_multistep_output_tokens": ms["output_tokens"],
            "sift_multistep_errors": ms["errors"],
            "sift_multistep_attempted": ms["attempted"],
        },
        "latency": latency_report,
        "per_dataset": datasets,
        "per_question_type": qtypes,
        "per_difficulty": per_difficulty,
        "results": results,
    }


def _fmt_acc(
    data: dict[str, Any],
    prefix: str,
) -> str:
    """Format accuracy as correct/total or dash when absent."""
    total = data.get(f"{prefix}_total", 0)
    if not total:
        return "\u2014"
    return f"{data.get(f'{prefix}_correct', 0)}/{total}"


def print_summary_table(report: dict[str, Any]) -> None:
    """Print a human-readable summary table to stdout."""
    summary = report["summary"]
    has_multistep = summary.get("sift_multistep_accuracy", "0/0") != "0/0"

    print("\n" + "=" * 70)
    print(f"  Tier 1 Benchmark Results \u2014 {report['model']}")
    print("=" * 70)
    print(
        f"\n  {'Condition':<16} {'Accuracy':>10} "
        f"{'Errors':>8} "
        f"{'Input Tok':>12} {'Output Tok':>12}"
    )
    print("  " + "-" * 62)

    _cond_rows: list[tuple[str, str, str, int, int]] = [
        (
            "Baseline",
            "baseline_accuracy",
            "baseline_errors",
            summary["baseline_input_tokens"],
            summary["baseline_output_tokens"],
        ),
        (
            "Sift",
            "sift_accuracy",
            "sift_errors",
            summary["sift_input_tokens"],
            summary["sift_output_tokens"],
        ),
    ]
    if has_multistep:
        _cond_rows.append(
            (
                "Sift Multistep",
                "sift_multistep_accuracy",
                "sift_multistep_errors",
                summary.get("sift_multistep_input_tokens", 0),
                summary.get("sift_multistep_output_tokens", 0),
            )
        )

    for label, acc_key, err_key, in_tok, out_tok in _cond_rows:
        print(
            f"  {label:<16} "
            f"{summary.get(acc_key, '0/0'):>10} "
            f"{summary.get(err_key, 0):>8} "
            f"{in_tok:>12,} "
            f"{out_tok:>12,}"
        )

    print(
        f"\n  Token reduction (sift vs baseline): "
        f"{summary['token_reduction_pct']}%"
    )

    # Latency percentiles
    latency = report.get("latency", {})
    lat_conds = [
        ("Baseline", "baseline"),
        ("Sift", "sift"),
    ]
    if has_multistep:
        lat_conds.append(("Sift Multistep", "sift_multistep"))
    any_lat = any(latency.get(k) for _, k in lat_conds)
    if any_lat:
        print(
            f"\n  {'Latency':<16} {'p50 (ms)':>10} "
            f"{'p90 (ms)':>10} {'mean (ms)':>10}"
        )
        print("  " + "-" * 50)
        for label, key in lat_conds:
            lp = latency.get(key, {})
            if lp:
                print(
                    f"  {label:<16} "
                    f"{lp.get('p50_ms', 0):>10.1f} "
                    f"{lp.get('p90_ms', 0):>10.1f} "
                    f"{lp.get('mean_ms', 0):>10.1f}"
                )

    # Per-dataset
    header = f"  {'Dataset':<15} {'Baseline':>10} {'Sift':>10}"
    if has_multistep:
        header += f" {'Multistep':>12}"
    print(f"\n{header}")
    sep_len = 38 + (14 if has_multistep else 0)
    print("  " + "-" * sep_len)
    for ds_name, ds in sorted(report["per_dataset"].items()):
        b_acc = _fmt_acc(ds, "baseline")
        s_acc = _fmt_acc(ds, "sift")
        line = f"  {ds_name:<15} {b_acc:>10} {s_acc:>10}"
        if has_multistep:
            m_acc = _fmt_acc(ds, "sift_multistep")
            line += f" {m_acc:>12}"
        errors: list[str] = []
        if ds.get("baseline_errors"):
            errors.append(f"b_err={ds['baseline_errors']}")
        if ds.get("sift_errors"):
            errors.append(f"s_err={ds['sift_errors']}")
        if ds.get("sift_multistep_errors"):
            errors.append(f"ms_err={ds['sift_multistep_errors']}")
        if errors:
            line += f"  ({', '.join(errors)})"
        print(line)

    # Per-question-type
    header = f"  {'Question Type':<15} {'Baseline':>10} {'Sift':>10}"
    if has_multistep:
        header += f" {'Multistep':>12}"
    print(f"\n{header}")
    print("  " + "-" * sep_len)
    for qt_name, qt in sorted(report["per_question_type"].items()):
        b_acc = _fmt_acc(qt, "baseline")
        s_acc = _fmt_acc(qt, "sift")
        line = f"  {qt_name:<15} {b_acc:>10} {s_acc:>10}"
        if has_multistep:
            m_acc = _fmt_acc(qt, "sift_multistep")
            line += f" {m_acc:>12}"
        print(line)

    # Per-difficulty
    per_diff = report.get("per_difficulty", {})
    if per_diff:
        diff_labels = {"1": "easy", "2": "medium", "3": "hard"}
        header = (
            f"  {'Difficulty':<12} {'Baseline':>10} {'Sift':>10} {'Retries':>8}"
        )
        if has_multistep:
            header += f" {'Multistep':>12} {'MS Retries':>10}"
        print(f"\n{header}")
        diff_sep = 44 + (24 if has_multistep else 0)
        print("  " + "-" * diff_sep)
        for d, dd in sorted(per_diff.items()):
            label = diff_labels.get(d, f"L{d}")
            b_acc = _fmt_acc(dd, "baseline")
            s_acc = _fmt_acc(dd, "sift")
            line = (
                f"  {label:<12} {b_acc:>10} "
                f"{s_acc:>10} "
                f"{dd.get('sift_retries', 0):>8}"
            )
            if has_multistep:
                m_acc = _fmt_acc(dd, "sift_multistep")
                line += (
                    f" {m_acc:>12} {dd.get('sift_multistep_retries', 0):>10}"
                )
            print(line)

    print("=" * 70 + "\n")
