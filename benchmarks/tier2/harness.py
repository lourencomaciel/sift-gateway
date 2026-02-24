#!/usr/bin/env python3
"""Tier 2 benchmark: LLM-driven autonomous agent loop.

The LLM autonomously decides which tools to call, when to paginate,
when to write code, and how to recover from errors.  This tests
whether the gateway's response format is genuinely useful to LLMs.
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
from pathlib import Path
import sys
import tempfile
from typing import Any

# Allow running as `python benchmarks/tier2/harness.py` without
# manually setting PYTHONPATH.
_REPO_ROOT = str(Path(__file__).resolve().parents[2])
_SRC_DIR = str(Path(__file__).resolve().parents[2] / "src")
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)
if _SRC_DIR not in sys.path:
    sys.path.insert(0, _SRC_DIR)

from benchmarks.tier1.datasets import ALL_DATASET_NAMES, DATASETS
from benchmarks.tier1.evaluate import evaluate_answer
from benchmarks.tier1.llm_client import LLMAPIError
from benchmarks.tier1.questions import (
    get_questions_for_dataset,
    question_set_hash,
)
from benchmarks.tier1.sift_runtime import create_runtime
from benchmarks.tier2.agent_loop import (
    _DEFAULT_MAX_INPUT_TOKENS,
    _DEFAULT_MAX_PAGES,
    _DEFAULT_MAX_TURNS,
    AgentResult,
    run_agent_loop,
)
from benchmarks.tier2.metrics import (
    build_question_metrics,
    build_report,
    print_summary_table,
)
from benchmarks.tier2.system_prompt import get_system_prompt
from benchmarks.tier2.tool_bridge import mcp_tools_to_definitions

_SESSION_ID = "benchmark_tier2"


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=("Tier 2 Benchmark: LLM-Driven Autonomous Agent Loop"),
    )
    parser.add_argument(
        "--model",
        default="claude-sonnet-4-6",
        help="LLM model to use",
    )
    parser.add_argument(
        "--api-key",
        default=None,
        help=("API key (or use ANTHROPIC_API_KEY env var)"),
    )
    parser.add_argument(
        "--datasets",
        nargs="*",
        default=None,
        help=(
            "Filter to specific datasets "
            f"(choices: {', '.join(ALL_DATASET_NAMES)})"
        ),
    )
    parser.add_argument(
        "--questions",
        nargs="*",
        default=None,
        help="Filter to specific question IDs",
    )
    parser.add_argument(
        "--data-dir",
        default=str(Path(__file__).resolve().parents[1] / "tier1" / "data"),
        help="Directory containing fetched datasets",
    )
    parser.add_argument(
        "--results-dir",
        default=str(Path(__file__).resolve().parent / "results"),
        help="Directory for output reports",
    )
    parser.add_argument(
        "--sift-data-dir",
        default=None,
        help="Sift data directory (default: temp dir per run)",
    )
    parser.add_argument(
        "--max-turns",
        type=int,
        default=_DEFAULT_MAX_TURNS,
        help="Max agent turns per question",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=_DEFAULT_MAX_PAGES,
        help="Max pagination calls per question",
    )
    parser.add_argument(
        "--max-input-tokens",
        type=int,
        default=_DEFAULT_MAX_INPUT_TOKENS,
        help="Token budget safety valve per question",
    )
    parser.add_argument(
        "--temperature",
        type=float,
        default=0.0,
        help="LLM sampling temperature",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON report to stdout",
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Record errors as failed results instead of aborting",
    )
    parser.add_argument(
        "--save-conversations",
        action="store_true",
        help="Include full conversations in JSON results",
    )
    return parser


def _load_dataset(data_dir: Path, dataset_name: str) -> Any:
    """Load a benchmark dataset from disk."""
    ds = DATASETS[dataset_name]
    path = data_dir / ds.local_filename
    if not path.exists():
        msg = (
            f"Dataset file not found: {path}. "
            f"Run: uv run python benchmarks/tier1/fetch_data.py"
        )
        raise FileNotFoundError(msg)
    return json.loads(path.read_text(encoding="utf-8"))


def _run_benchmark(args: argparse.Namespace) -> dict[str, Any]:
    """Execute the full Tier 2 benchmark run."""
    data_dir = Path(args.data_dir)
    dataset_names = args.datasets or list(ALL_DATASET_NAMES)

    for name in dataset_names:
        if name not in DATASETS:
            print(
                f"Unknown dataset: {name}. "
                f"Valid: {', '.join(ALL_DATASET_NAMES)}",
                file=sys.stderr,
            )
            raise SystemExit(1)

    question_filter: set[str] | None = (
        set(args.questions) if args.questions else None
    )

    # Preload datasets for gold-answer computation.
    loaded: dict[str, Any] = {}
    for name in dataset_names:
        print(f"Loading dataset: {name}")
        loaded[name] = _load_dataset(data_dir, name)

    results: list[dict[str, Any]] = []
    system_prompt = get_system_prompt()

    sift_data_dir = args.sift_data_dir
    if sift_data_dir is not None:
        _run_agent_across_datasets(
            dataset_names=dataset_names,
            loaded=loaded,
            results=results,
            sift_data_dir=sift_data_dir,
            question_filter=question_filter,
            system_prompt=system_prompt,
            args=args,
        )
    else:
        with tempfile.TemporaryDirectory(prefix="sift-bench-tier2-") as tmp:
            _run_agent_across_datasets(
                dataset_names=dataset_names,
                loaded=loaded,
                results=results,
                sift_data_dir=tmp,
                question_filter=question_filter,
                system_prompt=system_prompt,
                args=args,
            )

    return build_report(
        results,
        model=args.model,
        question_hash=question_set_hash(),
    )


def _run_agent_across_datasets(
    *,
    dataset_names: list[str],
    loaded: dict[str, Any],
    results: list[dict[str, Any]],
    sift_data_dir: str,
    question_filter: set[str] | None,
    system_prompt: str,
    args: argparse.Namespace,
) -> None:
    """Run the autonomous agent condition across datasets."""
    bench_data_dir = args.data_dir

    with create_runtime(
        data_dir=sift_data_dir,
        bench_data_dir=bench_data_dir,
    ) as runtime:
        # Discover available tools.
        mcp_tools = runtime.list_tools()
        tools = mcp_tools_to_definitions(mcp_tools)
        print(
            f"\nDiscovered {len(tools)} tools: "
            f"{', '.join(t.name for t in tools)}\n"
        )

        print("--- Agent Loop ---\n")

        for name in dataset_names:
            data = loaded[name]
            questions = get_questions_for_dataset(name)

            if question_filter and not any(
                q.question_id in question_filter for q in questions
            ):
                continue

            for q in questions:
                if question_filter and q.question_id not in question_filter:
                    continue

                gold = q.gold_answer_fn(data)
                print(f"  [{name}] {q.question_id}: {q.question_text[:50]}...")

                try:
                    agent_result = run_agent_loop(
                        question=q.question_text,
                        runtime=runtime,
                        tools=tools,
                        model=args.model,
                        system_prompt=system_prompt,
                        session_id=_SESSION_ID,
                        api_key=args.api_key,
                        temperature=args.temperature,
                        max_turns=args.max_turns,
                        max_pages=args.max_pages,
                        max_input_tokens=args.max_input_tokens,
                    )
                except LLMAPIError as exc:
                    if not args.continue_on_error:
                        raise
                    print(
                        f"    -> ERROR: {exc}",
                        file=sys.stderr,
                    )
                    empty_result = AgentResult(
                        answer="",
                        turns=0,
                        max_turns_reached=False,
                        token_budget_reached=False,
                    )
                    error_metrics = build_question_metrics(
                        agent_result=empty_result,
                        question_id=q.question_id,
                        dataset_name=name,
                        question_text=q.question_text,
                        question_type=q.question_type,
                        difficulty=q.difficulty,
                        gold_answer=gold,
                        llm_answer="",
                        correct=False,
                    )
                    error_metrics["error"] = str(exc)
                    results.append(error_metrics)
                    continue

                llm_answer = agent_result.answer
                correct = evaluate_answer(
                    llm_answer,
                    gold,
                    answer_type=q.answer_type,
                    tolerance=q.tolerance,
                )

                metrics = build_question_metrics(
                    agent_result=agent_result,
                    question_id=q.question_id,
                    dataset_name=name,
                    question_text=q.question_text,
                    question_type=q.question_type,
                    difficulty=q.difficulty,
                    gold_answer=gold,
                    llm_answer=llm_answer,
                    correct=correct,
                )

                if args.save_conversations:
                    metrics["conversation"] = agent_result.conversation

                status = "CORRECT" if correct else "WRONG"
                extra = ""
                if agent_result.max_turns_reached:
                    extra = " [MAX_TURNS]"
                if agent_result.token_budget_reached:
                    extra = " [BUDGET]"
                print(
                    f"    -> {status} "
                    f"(gold={gold}, "
                    f"llm={llm_answer[:40]}) "
                    f"turns={agent_result.turns} "
                    f"tools={sum(agent_result.tool_call_counts.values())}"
                    f"{extra}"
                )
                results.append(metrics)


def main() -> int:
    """CLI entrypoint."""
    args = _build_parser().parse_args()
    report = _run_benchmark(args)

    # Save JSON report.
    results_dir = Path(args.results_dir)
    results_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    model_slug = args.model.replace("/", "_").replace(":", "_")
    report_path = results_dir / f"tier2_{model_slug}_{ts}.json"
    report_path.write_text(
        json.dumps(
            report,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
            default=str,
        ),
        encoding="utf-8",
    )
    print(f"\nReport saved: {report_path}")

    if args.json:
        print(
            json.dumps(
                report,
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
                default=str,
            )
        )
    else:
        print_summary_table(report)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
