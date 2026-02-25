#!/usr/bin/env python3
"""Main benchmark harness comparing baseline (stuffed) vs Sift (queried)."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import UTC, datetime
import json
from pathlib import Path
import sys
import tempfile
import time
from typing import Any

# Allow running as `python benchmarks/tier1/harness.py` without
# manually setting PYTHONPATH.  The repo root is needed for
# `from benchmarks.tier1...` imports, and `src/` is needed for
# `from sift_gateway...` imports.
# Not needed when using `uv run` (recommended) which sets up
# the virtualenv and sys.path automatically.
_REPO_ROOT = str(Path(__file__).resolve().parents[2])
_SRC_DIR = str(Path(__file__).resolve().parents[2] / "src")
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)
if _SRC_DIR not in sys.path:
    sys.path.insert(0, _SRC_DIR)

from benchmarks.common.baseline import (
    BASELINE_SYSTEM,
    MAX_BASELINE_BYTES_DEFAULT,
    MAX_BASELINE_TOKENS_DEFAULT,
    truncate_for_baseline,
)
from benchmarks.common.datasets import ALL_DATASET_NAMES, DATASETS, load_dataset
from benchmarks.common.evaluate import evaluate_answer
from benchmarks.common.llm_client import LLMAPIError, call_llm
from benchmarks.common.questions import (
    Question,
    get_questions_for_dataset,
    question_set_hash,
)
from benchmarks.common.sift_runtime import (
    CodeExecutionError,
    call_mirrored_tool,
    create_runtime,
    execute_code,
    extract_root_paths,
    mcp_response_to_describe_format,
)
from benchmarks.tier1.code_extract import (
    extract_code,
    extract_root_path_comment,
)
from benchmarks.tier1.code_result import unwrap_code_result
from benchmarks.tier1.evaluate import build_report, print_summary_table
from benchmarks.tier1.schema_prompt import format_schema_for_prompt

_SIFT_CODEGEN_SYSTEM = (
    "You are a data analyst. Given the schema of a dataset, write a "
    "Python function `def run(data, schema, params):` that answers "
    "the question. `data` is the extracted value at the chosen "
    "root_path — it may be a list of dicts, a list of scalars, or a "
    "dict (e.g. columnar data). `schema` describes the fields and "
    "`params` is an empty dict. Return ONLY the Python function — "
    "no explanation."
)

_SIFT_ANSWER_SYSTEM = (
    "You are a data analyst. Given the result of a code query on a "
    "dataset, answer the original question. Give ONLY the final "
    "answer value — no explanation, no units, no surrounding text."
)


def _make_result(
    question: Question,
    *,
    condition: str,
    gold: str,
    llm_answer: str = "",
    correct: bool = False,
    input_tokens: int = 0,
    output_tokens: int = 0,
    latency_ms: float = 0.0,
    attempted: bool = True,
    **extra: Any,
) -> dict[str, Any]:
    """Build a result dict with shared question metadata."""
    result: dict[str, Any] = {
        "condition": condition,
        "dataset": question.dataset_name,
        "question_id": question.question_id,
        "question_type": question.question_type,
        "difficulty": question.difficulty,
        "question_text": question.question_text,
        "gold_answer": gold,
        "llm_answer": llm_answer,
        "correct": correct,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "latency_ms": latency_ms,
        "attempted": attempted,
    }
    result.update(extra)
    return result


def _run_baseline(
    question: Question,
    data: Any,
    *,
    model: str,
    api_key: str | None,
    temperature: float,
    max_baseline_bytes: int,
    max_baseline_tokens: int,
) -> dict[str, Any]:
    """Run a single baseline (context-stuffed) question."""
    gold = question.gold_answer_fn(data)
    data_json, truncated = truncate_for_baseline(
        data,
        max_bytes=max_baseline_bytes,
        max_tokens=max_baseline_tokens,
    )

    user_msg = (
        f"Here is the JSON data:\n\n{data_json}\n\n"
        f"Question: {question.question_text}"
    )

    start = time.monotonic()
    try:
        resp = call_llm(
            model=model,
            system_prompt=BASELINE_SYSTEM,
            user_message=user_msg,
            api_key=api_key,
            temperature=temperature,
        )
    except LLMAPIError as exc:
        elapsed = (time.monotonic() - start) * 1000.0
        return _make_result(
            question,
            condition="baseline",
            gold=gold,
            error=str(exc),
            latency_ms=elapsed,
            truncated=truncated,
            attempted=False,
        )

    correct = evaluate_answer(
        resp.text,
        gold,
        answer_type=question.answer_type,
        tolerance=question.tolerance,
    )
    return _make_result(
        question,
        condition="baseline",
        gold=gold,
        llm_answer=resp.text,
        correct=correct,
        input_tokens=resp.input_tokens,
        output_tokens=resp.output_tokens,
        latency_ms=resp.latency_ms,
        truncated=truncated,
    )


@dataclass(frozen=True)
class _CodegenResult:
    """Outcome of the code-generation + execution retry loop."""

    code_result: dict[str, Any] | None
    attempts: int
    last_error: str
    input_tokens: int
    output_tokens: int
    latency_ms: float


@dataclass(frozen=True)
class _AnswerResult:
    """Outcome of the LLM answer-extraction call."""

    text: str
    input_tokens: int
    output_tokens: int
    latency_ms: float
    error: str


def _codegen_loop(
    *,
    codegen_msg: str,
    root_paths: list[str],
    runtime: Any,
    artifact_id: str,
    model: str,
    api_key: str | None,
    temperature: float,
    max_retries: int,
) -> _CodegenResult:
    """Run the code-generation + execution retry loop.

    ``LLMAPIError`` propagates immediately.  Only
    ``CodeExecutionError`` triggers a retry.
    """
    multi_root = len(root_paths) > 1
    attempts = 0
    input_tokens = 0
    output_tokens = 0
    latency_ms = 0.0
    last_error = ""
    last_code = ""

    while attempts <= max_retries:
        # Build the effective message (append retry context when
        # retrying so the LLM sees the previous failure).
        if attempts > 0:
            effective_msg = (
                f"{codegen_msg}\n\n"
                f"Previous code:\n```python\n{last_code}\n```\n\n"
                f"Error:\n{last_error}\n"
                f"Please fix the code."
            )
        else:
            effective_msg = codegen_msg

        codegen_resp = call_llm(
            model=model,
            system_prompt=_SIFT_CODEGEN_SYSTEM,
            user_message=effective_msg,
            api_key=api_key,
            temperature=temperature,
        )

        input_tokens += codegen_resp.input_tokens
        output_tokens += codegen_resp.output_tokens
        latency_ms += codegen_resp.latency_ms

        code = extract_code(codegen_resp.text).code

        # Resolve which root_path to execute against.
        if multi_root:
            selected = extract_root_path_comment(codegen_resp.text, root_paths)
            root_path = selected if selected else root_paths[0]
        else:
            root_path = root_paths[0]

        # Execute the generated code.
        try:
            exec_start = time.monotonic()
            code_result = execute_code(
                runtime,
                artifact_id=artifact_id,
                root_path=root_path,
                code=code,
            )
            latency_ms += (time.monotonic() - exec_start) * 1000.0
            return _CodegenResult(
                code_result=code_result,
                attempts=attempts,
                last_error="",
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                latency_ms=latency_ms,
            )
        except CodeExecutionError as exc:
            latency_ms += (time.monotonic() - exec_start) * 1000.0
            last_code = code
            last_error = str(exc)
            attempts += 1

    # All retries exhausted.
    return _CodegenResult(
        code_result=None,
        attempts=attempts - 1,
        last_error=last_error,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        latency_ms=latency_ms,
    )


def _extract_answer(
    *,
    question_text: str,
    code_output: str,
    model: str,
    api_key: str | None,
    temperature: float,
) -> _AnswerResult:
    """Call the LLM to extract a final answer from code output.

    Catches ``LLMAPIError`` and returns an ``_AnswerResult`` with
    ``error`` populated instead of propagating.
    """
    answer_msg = (
        f"Question: {question_text}\n\n"
        f"Code query result:\n{code_output}\n\n"
        f"Give ONLY the final answer value."
    )

    try:
        resp = call_llm(
            model=model,
            system_prompt=_SIFT_ANSWER_SYSTEM,
            user_message=answer_msg,
            api_key=api_key,
            temperature=temperature,
        )
    except LLMAPIError as exc:
        return _AnswerResult(
            text="",
            input_tokens=0,
            output_tokens=0,
            latency_ms=0.0,
            error=f"answer extraction failed: {exc}",
        )

    return _AnswerResult(
        text=resp.text,
        input_tokens=resp.input_tokens,
        output_tokens=resp.output_tokens,
        latency_ms=resp.latency_ms,
        error="",
    )


def _run_sift(
    question: Question,
    data: Any,
    *,
    runtime: Any,
    artifact_id: str,
    root_paths: list[str],
    schema_text: str,
    model: str,
    api_key: str | None,
    temperature: float,
    max_retries: int,
) -> dict[str, Any]:
    """Run a single Sift (schema_ref + codegen) question."""
    gold = question.gold_answer_fn(data)

    # Build the codegen prompt.
    root_selection_block = ""
    if len(root_paths) > 1:
        roots_list = "\n".join(f"  - {rp}" for rp in root_paths)
        root_selection_block = (
            f"\nAvailable root_paths (data at the chosen root "
            f"will be extracted and passed as `data`):\n"
            f"{roots_list}\n\n"
            f"On the FIRST line of your response, specify which "
            f"root_path to use as a Python comment:\n"
            f"# root_path: <chosen_path>\n"
        )
    codegen_msg = (
        f"Dataset schema:\n{schema_text}\n\n"
        f"Question: {question.question_text}\n\n"
        f"{root_selection_block}"
        f"Write ONLY the Python function `def run(data, schema, params):` "
        f"that computes the answer. Return the answer value directly "
        f"(not a string description)."
    )

    # Step 1: Code generation + execution (with retries).
    cg = _codegen_loop(
        codegen_msg=codegen_msg,
        root_paths=root_paths,
        runtime=runtime,
        artifact_id=artifact_id,
        model=model,
        api_key=api_key,
        temperature=temperature,
        max_retries=max_retries,
    )

    if cg.code_result is None:
        return _make_result(
            question,
            condition="sift",
            gold=gold,
            error=f"code execution failed: {cg.last_error}",
            input_tokens=cg.input_tokens,
            output_tokens=cg.output_tokens,
            latency_ms=cg.latency_ms,
            retries=cg.attempts,
            attempted=False,
        )

    # Step 2: Serialize code result.
    code_output = json.dumps(
        unwrap_code_result(cg.code_result), ensure_ascii=False
    )

    # Step 3: Extract final answer.
    ans = _extract_answer(
        question_text=question.question_text,
        code_output=code_output,
        model=model,
        api_key=api_key,
        temperature=temperature,
    )

    if ans.error:
        return _make_result(
            question,
            condition="sift",
            gold=gold,
            error=ans.error,
            input_tokens=cg.input_tokens,
            output_tokens=cg.output_tokens,
            latency_ms=cg.latency_ms,
            retries=cg.attempts,
            attempted=False,
        )

    correct = evaluate_answer(
        ans.text,
        gold,
        answer_type=question.answer_type,
        tolerance=question.tolerance,
    )

    return _make_result(
        question,
        condition="sift",
        gold=gold,
        llm_answer=ans.text,
        correct=correct,
        input_tokens=cg.input_tokens + ans.input_tokens,
        output_tokens=cg.output_tokens + ans.output_tokens,
        latency_ms=cg.latency_ms + ans.latency_ms,
        retries=cg.attempts,
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Tier 1 Benchmark: Sift vs Context-Stuffing for Factual QA"
        ),
    )
    parser.add_argument(
        "--model",
        default="claude-sonnet-4-6",
        help="LLM model to use",
    )
    parser.add_argument(
        "--api-key",
        default=None,
        help="API key (or use ANTHROPIC_API_KEY / OPENAI_API_KEY env)",
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
        "--data-dir",
        default=str(Path(__file__).resolve().parent / "data"),
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
        "--questions",
        nargs="*",
        default=None,
        help="Filter to specific question IDs (e.g. eq_mag_gte4)",
    )
    parser.add_argument(
        "--max-baseline-payload-bytes",
        type=int,
        default=MAX_BASELINE_BYTES_DEFAULT,
        help="Max baseline payload size in bytes",
    )
    parser.add_argument(
        "--max-baseline-tokens",
        type=int,
        default=MAX_BASELINE_TOKENS_DEFAULT,
        help="Max estimated baseline tokens (conservative)",
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
        "--skip-baseline",
        action="store_true",
        help="Skip baseline condition",
    )
    parser.add_argument(
        "--skip-sift",
        action="store_true",
        help="Skip Sift condition",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=2,
        help="Max code execution retries for Sift condition",
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help=(
            "Record LLM API errors as failed results instead of "
            "aborting the benchmark run"
        ),
    )
    return parser


def _run_benchmark(args: argparse.Namespace) -> dict[str, Any]:
    """Execute the full benchmark run."""
    data_dir = Path(args.data_dir)
    dataset_names = args.datasets or ALL_DATASET_NAMES

    for name in dataset_names:
        if name not in DATASETS:
            print(
                f"Unknown dataset: {name}. "
                f"Valid: {', '.join(ALL_DATASET_NAMES)}",
                file=sys.stderr,
            )
            raise SystemExit(1)

    results: list[dict[str, Any]] = []
    question_filter: set[str] | None = (
        set(args.questions) if args.questions else None
    )

    # Preload all datasets
    loaded: dict[str, Any] = {}
    for name in dataset_names:
        print(f"Loading dataset: {name}")
        loaded[name] = load_dataset(data_dir, name)

    # Run baseline condition
    if not args.skip_baseline:
        print("\n--- Baseline (context-stuffed) ---\n")
        for name in dataset_names:
            data = loaded[name]
            questions = get_questions_for_dataset(name)
            for q in questions:
                if question_filter and q.question_id not in question_filter:
                    continue
                print(f"  [{name}] {q.question_id}: {q.question_text[:50]}...")
                result = _run_baseline(
                    q,
                    data,
                    model=args.model,
                    api_key=args.api_key,
                    temperature=args.temperature,
                    max_baseline_bytes=(args.max_baseline_payload_bytes),
                    max_baseline_tokens=args.max_baseline_tokens,
                )
                status = "CORRECT" if result["correct"] else "WRONG"
                print(
                    f"    -> {status} "
                    f"(gold={result['gold_answer']}, "
                    f"llm={result['llm_answer'][:40]})"
                )
                results.append(result)

    # Run Sift condition
    if not args.skip_sift:
        print("\n--- Sift (schema_ref + codegen) ---\n")

        sift_data_dir = args.sift_data_dir
        if sift_data_dir is not None:
            _run_sift_condition(
                dataset_names=dataset_names,
                loaded=loaded,
                results=results,
                sift_data_dir=sift_data_dir,
                question_filter=question_filter,
                args=args,
            )
        else:
            with tempfile.TemporaryDirectory(prefix="sift-bench-tier1-") as tmp:
                _run_sift_condition(
                    dataset_names=dataset_names,
                    loaded=loaded,
                    results=results,
                    sift_data_dir=tmp,
                    question_filter=question_filter,
                    args=args,
                )

    return build_report(
        results,
        model=args.model,
        question_hash=question_set_hash(),
    )


def _run_sift_condition(
    *,
    dataset_names: list[str],
    loaded: dict[str, Any],
    results: list[dict[str, Any]],
    sift_data_dir: str,
    question_filter: set[str] | None,
    args: argparse.Namespace,
) -> None:
    """Execute Sift condition across datasets."""
    continue_on_error = args.continue_on_error
    with create_runtime(
        data_dir=sift_data_dir,
        bench_data_dir=args.data_dir,
    ) as runtime:
        for name in dataset_names:
            data = loaded[name]
            questions = get_questions_for_dataset(name)

            # Skip dataset entirely if no questions match filter.
            if question_filter and not any(
                q.question_id in question_filter for q in questions
            ):
                continue

            print(f"  Calling mirrored tool for {name} ...")
            mcp_result = call_mirrored_tool(
                runtime,
                dataset_name=name,
            )
            artifact_id = mcp_result["artifact_id"]
            print(f"    artifact_id: {artifact_id}")

            describe_compat = mcp_response_to_describe_format(
                mcp_result,
                runtime,
            )
            root_paths = extract_root_paths(describe_compat)
            schema_text = format_schema_for_prompt(describe_compat)
            print(f"    root_paths: {root_paths}")

            for q in questions:
                if question_filter and q.question_id not in question_filter:
                    continue
                print(f"  [{name}] {q.question_id}: {q.question_text[:50]}...")
                try:
                    result = _run_sift(
                        q,
                        data,
                        runtime=runtime,
                        artifact_id=artifact_id,
                        root_paths=root_paths,
                        schema_text=schema_text,
                        model=args.model,
                        api_key=args.api_key,
                        temperature=args.temperature,
                        max_retries=args.max_retries,
                    )
                except LLMAPIError as exc:
                    if not continue_on_error:
                        raise
                    gold = q.gold_answer_fn(data)
                    result = _make_result(
                        q,
                        condition="sift",
                        gold=gold,
                        error=f"LLM API error: {exc}",
                        attempted=False,
                    )
                status = "CORRECT" if result["correct"] else "WRONG"
                error_suffix = ""
                if result.get("error"):
                    error_suffix = f" [ERROR: {result['error'][:50]}]"
                print(
                    f"    -> {status} "
                    f"(gold={result['gold_answer']}, "
                    f"llm={result['llm_answer'][:40]})"
                    f"{error_suffix}"
                )
                results.append(result)


def main() -> int:
    """CLI entrypoint."""
    args = _build_parser().parse_args()
    report = _run_benchmark(args)

    # Save JSON report
    results_dir = Path(args.results_dir)
    results_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    model_slug = args.model.replace("/", "_").replace(":", "_")
    report_path = results_dir / f"tier1_{model_slug}_{ts}.json"
    report_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True)
    )
    print(f"\nReport saved: {report_path}")

    if args.json:
        print(
            json.dumps(
                report,
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
        )
    else:
        print_summary_table(report)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
