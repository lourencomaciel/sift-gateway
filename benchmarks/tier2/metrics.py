"""Per-question and aggregate metric tracking for Tier 2 benchmark."""

from __future__ import annotations

import math
import statistics
from typing import Any

from benchmarks.tier2.agent_loop import AgentResult


def build_question_metrics(
    *,
    agent_result: AgentResult,
    question_id: str,
    dataset_name: str,
    question_type: str,
    difficulty: int,
    gold_answer: str,
    llm_answer: str,
    correct: bool,
) -> dict[str, Any]:
    """Build per-question metrics from an agent result.

    Args:
        agent_result: The ``AgentResult`` from the agent loop.
        question_id: Unique question identifier.
        dataset_name: Dataset the question targets.
        question_type: Question category (count, filter, etc.).
        difficulty: Difficulty level (1=easy, 2=medium, 3=hard).
        gold_answer: Expected answer.
        llm_answer: Agent's answer.
        correct: Whether the answer matched gold.

    Returns:
        Dict with all per-question metrics.
    """
    return {
        "question_id": question_id,
        "dataset": dataset_name,
        "question_type": question_type,
        "difficulty": difficulty,
        "gold_answer": gold_answer,
        "llm_answer": llm_answer,
        "correct": correct,
        "turns": agent_result.turns,
        "max_turns_reached": agent_result.max_turns_reached,
        "token_budget_reached": agent_result.token_budget_reached,
        "tool_calls": dict(agent_result.tool_call_counts),
        "total_tool_calls": sum(agent_result.tool_call_counts.values()),
        "code_query_attempts": agent_result.code_query_attempts,
        "code_query_errors": agent_result.code_query_errors,
        "pages_fetched": agent_result.pages_fetched,
        "input_tokens": agent_result.total_input_tokens,
        "output_tokens": agent_result.total_output_tokens,
        "latency_ms": agent_result.total_latency_ms,
        "per_turn": [
            {
                "input_tokens": tm.input_tokens,
                "output_tokens": tm.output_tokens,
                "latency_ms": tm.latency_ms,
                "tool_calls": tm.tool_calls,
            }
            for tm in agent_result.turn_metrics
        ],
    }


def _latency_percentiles(
    latencies: list[float],
) -> dict[str, float | int]:
    """Compute p50, p90, and mean from a list of latencies."""
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


def build_report(
    results: list[dict[str, Any]],
    *,
    model: str,
    question_hash: str = "",
) -> dict[str, Any]:
    """Build an aggregate report from per-question metrics.

    Args:
        results: List of per-question metric dicts.
        model: Model identifier used for the benchmark.
        question_hash: Hash of the question set for validation.

    Returns:
        Report dict with summary, per-dataset, per-question-type,
        per-difficulty breakdowns, and individual results.
    """
    total = len(results)
    correct = sum(1 for r in results if r.get("correct"))
    exhausted = sum(1 for r in results if r.get("max_turns_reached"))
    budget_reached = sum(1 for r in results if r.get("token_budget_reached"))

    total_input = sum(r.get("input_tokens", 0) for r in results)
    total_output = sum(r.get("output_tokens", 0) for r in results)
    total_tool_calls = sum(r.get("total_tool_calls", 0) for r in results)

    # Aggregate tool call distribution.
    tool_dist: dict[str, int] = {}
    for r in results:
        for cat, count in r.get("tool_calls", {}).items():
            tool_dist[cat] = tool_dist.get(cat, 0) + count

    total_code_attempts = sum(r.get("code_query_attempts", 0) for r in results)
    total_code_errors = sum(r.get("code_query_errors", 0) for r in results)
    code_retry_rate = (
        round(total_code_errors / total_code_attempts, 3)
        if total_code_attempts > 0
        else 0.0
    )
    pagination_count = sum(1 for r in results if r.get("pages_fetched", 0) > 0)

    turns_list = [r.get("turns", 0) for r in results]
    avg_turns = round(sum(turns_list) / total, 2) if total else 0
    tool_call_list = [r.get("total_tool_calls", 0) for r in results]
    avg_tool_calls = round(sum(tool_call_list) / total, 2) if total else 0

    latencies = [
        r["latency_ms"] for r in results if r.get("latency_ms") is not None
    ]

    # Per-dataset breakdown.
    per_dataset: dict[str, dict[str, Any]] = {}
    for r in results:
        ds = r.get("dataset", "unknown")
        if ds not in per_dataset:
            per_dataset[ds] = {
                "correct": 0,
                "total": 0,
                "input_tokens": 0,
                "output_tokens": 0,
                "exhausted": 0,
            }
        entry = per_dataset[ds]
        entry["total"] += 1
        if r.get("correct"):
            entry["correct"] += 1
        entry["input_tokens"] += r.get("input_tokens", 0)
        entry["output_tokens"] += r.get("output_tokens", 0)
        if r.get("max_turns_reached"):
            entry["exhausted"] += 1

    # Per-question-type breakdown.
    per_qtype: dict[str, dict[str, Any]] = {}
    for r in results:
        qt = r.get("question_type", "unknown")
        if qt not in per_qtype:
            per_qtype[qt] = {"correct": 0, "total": 0}
        per_qtype[qt]["total"] += 1
        if r.get("correct"):
            per_qtype[qt]["correct"] += 1

    # Per-difficulty breakdown.
    per_diff: dict[str, dict[str, Any]] = {}
    for r in results:
        d = str(r.get("difficulty", 1))
        if d not in per_diff:
            per_diff[d] = {
                "correct": 0,
                "total": 0,
                "code_errors": 0,
                "latencies": [],
            }
        entry = per_diff[d]
        entry["total"] += 1
        if r.get("correct"):
            entry["correct"] += 1
        entry["code_errors"] += r.get("code_query_errors", 0)
        if r.get("latency_ms") is not None:
            entry["latencies"].append(r["latency_ms"])

    per_difficulty: dict[str, dict[str, Any]] = {}
    for d, entry in per_diff.items():
        per_difficulty[d] = {
            "correct": entry["correct"],
            "total": entry["total"],
            "code_errors": entry["code_errors"],
            "latency": _latency_percentiles(entry["latencies"]),
        }

    return {
        "model": model,
        "question_set_hash": question_hash,
        "summary": {
            "accuracy": f"{correct}/{total}",
            "accuracy_pct": round(correct / total * 100, 1) if total else 0,
            "total_questions": total,
            "max_turns_exhausted": exhausted,
            "token_budget_reached": budget_reached,
            "input_tokens": total_input,
            "output_tokens": total_output,
            "avg_turns": avg_turns,
            "avg_tool_calls": avg_tool_calls,
            "total_tool_calls": total_tool_calls,
            "tool_call_distribution": tool_dist,
            "code_retry_rate": code_retry_rate,
            "pagination_questions": pagination_count,
        },
        "latency": _latency_percentiles(latencies),
        "per_dataset": per_dataset,
        "per_question_type": per_qtype,
        "per_difficulty": per_difficulty,
        "results": results,
    }


def print_summary_table(report: dict[str, Any]) -> None:
    """Print a human-readable summary table to stdout."""
    summary = report["summary"]
    print("\n" + "=" * 70)
    print(f"  Tier 2 Benchmark Results — {report['model']}")
    print("=" * 70)

    print(f"\n  Accuracy:  {summary['accuracy']} ({summary['accuracy_pct']}%)")
    print(f"  Questions: {summary['total_questions']}")
    print(
        f"  Exhausted: {summary['max_turns_exhausted']}  "
        f"Budget reached: {summary['token_budget_reached']}"
    )
    print(
        f"\n  Avg turns/question: {summary['avg_turns']}  "
        f"Avg tool calls/question: {summary['avg_tool_calls']}"
    )
    print(
        f"  Code retry rate: {summary['code_retry_rate']:.1%}  "
        f"Pagination questions: "
        f"{summary['pagination_questions']}"
    )
    print(
        f"  Input tokens: {summary['input_tokens']:,}  "
        f"Output tokens: {summary['output_tokens']:,}"
    )

    # Tool call distribution.
    dist = summary.get("tool_call_distribution", {})
    if dist:
        print("\n  Tool calls:")
        for cat in sorted(dist):
            print(f"    {cat:<15} {dist[cat]:>6}")

    # Latency.
    lat = report.get("latency", {})
    if lat:
        print(
            f"\n  Latency: p50={lat.get('p50_ms', 0):.0f}ms  "
            f"p90={lat.get('p90_ms', 0):.0f}ms  "
            f"mean={lat.get('mean_ms', 0):.0f}ms"
        )

    # Per-dataset.
    per_ds = report.get("per_dataset", {})
    if per_ds:
        print(f"\n  {'Dataset':<15} {'Accuracy':>10} {'Exhausted':>10}")
        print("  " + "-" * 38)
        for ds_name in sorted(per_ds):
            ds = per_ds[ds_name]
            acc = f"{ds['correct']}/{ds['total']}"
            exh = ds.get("exhausted", 0)
            print(f"  {ds_name:<15} {acc:>10} {exh:>10}")

    # Per-question-type.
    per_qt = report.get("per_question_type", {})
    if per_qt:
        print(f"\n  {'Question Type':<20} {'Accuracy':>10}")
        print("  " + "-" * 32)
        for qt_name in sorted(per_qt):
            qt = per_qt[qt_name]
            acc = f"{qt['correct']}/{qt['total']}"
            print(f"  {qt_name:<20} {acc:>10}")

    # Per-difficulty.
    per_diff = report.get("per_difficulty", {})
    if per_diff:
        diff_labels = {"1": "easy", "2": "medium", "3": "hard"}
        print(f"\n  {'Difficulty':<12} {'Accuracy':>10} {'Code Errors':>12}")
        print("  " + "-" * 38)
        for d in sorted(per_diff):
            dd = per_diff[d]
            label = diff_labels.get(d, f"L{d}")
            acc = f"{dd['correct']}/{dd['total']}"
            print(f"  {label:<12} {acc:>10} {dd['code_errors']:>12}")

    print("=" * 70 + "\n")
