#!/usr/bin/env python3
"""Main benchmark harness comparing baseline (stuffed) vs Sift (queried)."""

from __future__ import annotations

import argparse
import copy
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

from benchmarks.tier1.datasets import ALL_DATASET_NAMES, DATASETS
from benchmarks.tier1.evaluate import (
    build_report,
    evaluate_answer,
    print_summary_table,
)
from benchmarks.tier1.llm_client import LLMAPIError, call_llm
from benchmarks.tier1.questions import (
    Question,
    get_questions_for_dataset,
    question_set_hash,
)
from benchmarks.tier1.sift_runtime import (
    CodeExecutionError,
    capture_payload,
    create_runtime,
    describe_artifact,
    execute_code,
    extract_root_paths,
)

_MAX_BASELINE_BYTES_DEFAULT = 400_000

# Conservative estimate: structured JSON tokenizes at roughly
# 3 bytes per token (short keys, numbers, punctuation each become
# separate tokens).  The limit leaves ~20K tokens headroom for
# system prompt + question text.
_MAX_BASELINE_TOKENS_DEFAULT = 180_000
_BYTES_PER_TOKEN_JSON = 3

_BASELINE_SYSTEM = (
    "You are a data analyst. Answer the question about the JSON data "
    "provided. Give ONLY the final answer value — no explanation, "
    "no units, no surrounding text. For numbers, give the numeric "
    "value. For strings, give the exact value."
)

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


def _load_dataset(data_dir: Path, dataset_name: str) -> Any:
    """Load a dataset from disk."""
    ds = DATASETS[dataset_name]
    path = data_dir / ds.local_filename
    if not path.exists():
        msg = (
            f"Dataset file not found: {path}\n"
            f"Run: python benchmarks/tier1/fetch_data.py"
        )
        raise FileNotFoundError(msg)
    return json.loads(path.read_text(encoding="utf-8"))


def _effective_max_bytes(
    max_bytes: int,
    max_tokens: int,
) -> int:
    """Return the smaller of the byte cap and the token-derived cap.

    JSON tokenizes poorly (~2 bytes/token for structured data),
    so a byte cap alone often exceeds the model's context window.
    """
    token_derived = max_tokens * _BYTES_PER_TOKEN_JSON
    return min(max_bytes, token_derived)


def _fits(candidate: str, limit: int) -> bool:
    """Check if a JSON string fits within the byte limit."""
    # String length is a lower bound on UTF-8 byte length,
    # so skip the encode when the string alone exceeds cap.
    if len(candidate) > limit:
        return False
    return len(candidate.encode("utf-8")) <= limit


def _truncate_list(data: list[Any], limit: int) -> str:
    """Binary search for the largest array prefix that fits."""
    best_json = json.dumps(data[:1], ensure_ascii=False)
    if not _fits(best_json, limit):
        # Even a single item exceeds the limit — return it anyway
        # so callers always get valid JSON (best-effort).
        return best_json
    low, high = 1, len(data)
    while low <= high:
        mid = (low + high) // 2
        candidate = json.dumps(data[:mid], ensure_ascii=False)
        if not _fits(candidate, limit):
            high = mid - 1
        else:
            best_json = candidate
            low = mid + 1
    return best_json


def _truncate_dict(data: dict[str, Any], limit: int) -> str | None:
    """Shrink array values inside a dict to fit within *limit* bytes.

    Finds lists at the top level and one level deep, then binary-
    searches on a keep-fraction applied uniformly to all of them.
    Returns ``None`` if the dict cannot be shrunk to fit (e.g. no
    array values to trim).
    """
    # Collect (key_path, original_length) for all arrays.
    # key_path is a tuple: ("key",) for top-level, ("parent", "key")
    # for one-level-deep.  This avoids identity-based container
    # lookups that break across deepcopy boundaries.
    arrays: list[tuple[tuple[str, ...], int]] = []
    for key, val in data.items():
        if isinstance(val, list) and len(val) > 1:
            arrays.append(((key,), len(val)))
        elif isinstance(val, dict):
            for subkey, subval in val.items():
                if isinstance(subval, list) and len(subval) > 1:
                    arrays.append(((key, subkey), len(subval)))

    if not arrays:
        return None

    # Binary search on the fraction of array elements to keep.
    low_f, high_f = 0.0, 1.0
    best_json: str | None = None
    for _ in range(30):
        mid_f = (low_f + high_f) / 2
        trial = copy.deepcopy(data)
        for key_path, orig_len in arrays:
            keep = max(1, int(orig_len * mid_f))
            if len(key_path) == 1:
                trial[key_path[0]] = trial[key_path[0]][:keep]
            else:
                trial[key_path[0]][key_path[1]] = trial[key_path[0]][
                    key_path[1]
                ][:keep]

        candidate = json.dumps(trial, ensure_ascii=False)
        if _fits(candidate, limit):
            best_json = candidate
            low_f = mid_f
        else:
            high_f = mid_f

    return best_json


def _truncate_for_baseline(
    data: Any,
    *,
    max_bytes: int,
    max_tokens: int,
) -> tuple[str, bool]:
    """Serialize data for baseline, truncating if too large.

    Uses both a byte cap and a token-estimate cap to avoid
    exceeding the model's context window.
    """
    limit = _effective_max_bytes(max_bytes, max_tokens)
    full_json = json.dumps(data, ensure_ascii=False)
    if _fits(full_json, limit):
        return full_json, False

    if isinstance(data, list):
        return _truncate_list(data, limit), True

    if isinstance(data, dict):
        shrunk = _truncate_dict(data, limit)
        if shrunk is not None:
            return shrunk, True

    note = {
        "_truncated": True,
        "_note": "payload too large for baseline",
    }
    return json.dumps(note, ensure_ascii=False), True


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
    data_json, truncated = _truncate_for_baseline(
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
            system_prompt=_BASELINE_SYSTEM,
            user_message=user_msg,
            api_key=api_key,
            temperature=temperature,
        )
    except Exception as exc:
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


def _extract_code_from_response(text: str) -> str:
    """Extract Python code from LLM response.

    Tries markdown fences first, then falls back to raw text.
    Validates that the result contains ``def run`` to avoid
    passing explanation prose as code.
    """
    candidates: list[str] = []

    if "```python" in text:
        parts = text.split("```python", 1)
        if len(parts) > 1:
            candidates.append(parts[1].split("```", 1)[0].strip())
    if "```" in text and not candidates:
        parts = text.split("```", 1)
        if len(parts) > 1:
            candidates.append(parts[1].split("```", 1)[0].strip())

    candidates.append(text.strip())

    for candidate in candidates:
        if "def run" in candidate:
            return candidate

    # No candidate contains def run — will likely fail at execution
    # and be retried with the error context appended.
    print(
        "  [warn] LLM response has no 'def run'; using raw text as code",
        file=sys.stderr,
    )
    return candidates[0] if candidates else text.strip()


def _format_schema_for_prompt(describe_result: dict[str, Any]) -> str:
    """Format schema info from describe result into a prompt string."""
    schemas = describe_result.get("schemas", [])
    roots = describe_result.get("roots", [])

    parts: list[str] = []

    if roots:
        parts.append("Dataset roots:")
        for root in roots:
            rp = root.get("root_path", "$")
            count = root.get("count_estimate", "?")
            shape = root.get("root_shape", "?")
            parts.append(f"  - root_path: {rp}, count: {count}, shape: {shape}")

    for schema in schemas:
        rp = schema.get("root_path", "$")
        parts.append(f"\nSchema for root '{rp}':")
        fields = schema.get("fields", [])
        for field in fields:
            fp = field.get("field_path", "?")
            types = field.get("types", "?")
            example = field.get("example_value")
            nullable = field.get("nullable", False)
            line = f"  - {fp}: {types}"
            if nullable:
                line += " (nullable)"
            if example is not None:
                example_str = json.dumps(example)
                if len(example_str) > 80:
                    example_str = example_str[:77] + "..."
                line += f" — e.g. {example_str}"
            parts.append(line)

        # Detect columnar layout: object root where most fields
        # are arrays (e.g. weather data stored as parallel arrays).
        matching_root = next(
            (r for r in roots if r.get("root_path") == rp),
            None,
        )
        if (
            matching_root is not None
            and matching_root.get("root_shape") == "object"
            and fields
        ):
            array_count = sum(
                1 for f in fields if str(f.get("types", "")).startswith("array")
            )
            if array_count >= len(fields) / 2:
                # Pick the first array field name for the example.
                example_field = next(
                    (
                        f.get("field_path", "").rsplit(".", 1)[-1]
                        for f in fields
                        if str(f.get("types", "")).startswith("array")
                    ),
                    "field",
                )
                parts.append(
                    "\nIMPORTANT — This root is columnar"
                    " (dict of parallel arrays)."
                    "\n`data` is a dict where each key maps"
                    " to a list of values."
                    "\nAll lists have the same length; index"
                    " i corresponds to the same record."
                    f'\nAccess: data["{example_field}"][i],'
                    ' NOT data[i]["field"].'
                    "\nExample pattern:"
                    f'\n  total = sum(data["{example_field}"])'
                    f'\n  n = len(data["{example_field}"])'
                )

    return "\n".join(parts)


def _extract_root_path_from_response(
    text: str,
    available: list[str],
) -> str | None:
    """Parse a ``# root_path: …`` comment from LLM output.

    Returns ``None`` when no valid selection is found so the caller
    can fall back to the first available root.

    Note: this scans the raw LLM response (before
    ``_extract_code_from_response`` strips fences).  The two
    functions are coupled — both consume ``codegen_resp.text``.
    """
    for line in text.strip().splitlines():
        stripped = line.strip()
        if stripped.startswith("# root_path:"):
            candidate = stripped.split(":", 1)[1].strip()
            if candidate in available:
                return candidate
    return None


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
    total_input_tokens = 0
    total_output_tokens = 0
    total_latency = 0.0

    multi_root = len(root_paths) > 1

    # Step 1: Ask LLM to generate code
    root_selection_block = ""
    if multi_root:
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

    attempts = 0
    code_result: dict[str, Any] | None = None
    last_error = ""
    last_code = ""

    while attempts <= max_retries:
        # Step 1a: Generate code via LLM.  LLMAPIError propagates
        # immediately — only CodeExecutionError triggers a retry.
        if attempts > 0:
            codegen_msg_retry = (
                f"{codegen_msg}\n\n"
                f"Previous code:\n```python\n{last_code}\n```\n\n"
                f"Error:\n{last_error}\n"
                f"Please fix the code."
            )
            codegen_resp = call_llm(
                model=model,
                system_prompt=_SIFT_CODEGEN_SYSTEM,
                user_message=codegen_msg_retry,
                api_key=api_key,
                temperature=temperature,
            )
        else:
            codegen_resp = call_llm(
                model=model,
                system_prompt=_SIFT_CODEGEN_SYSTEM,
                user_message=codegen_msg,
                api_key=api_key,
                temperature=temperature,
            )

        total_input_tokens += codegen_resp.input_tokens
        total_output_tokens += codegen_resp.output_tokens
        total_latency += codegen_resp.latency_ms

        code = _extract_code_from_response(codegen_resp.text)

        # Resolve which root_path to execute against.
        if multi_root:
            selected = _extract_root_path_from_response(
                codegen_resp.text, root_paths
            )
            root_path = selected if selected else root_paths[0]
        else:
            root_path = root_paths[0]

        # Step 1b: Execute the generated code.
        try:
            exec_start = time.monotonic()
            code_result = execute_code(
                runtime,
                artifact_id=artifact_id,
                root_path=root_path,
                code=code,
            )
            total_latency += (time.monotonic() - exec_start) * 1000.0
            break
        except CodeExecutionError as exc:
            total_latency += (time.monotonic() - exec_start) * 1000.0
            last_code = code
            last_error = str(exc)
            attempts += 1
            if attempts > max_retries:
                return _make_result(
                    question,
                    condition="sift",
                    gold=gold,
                    error=f"code execution failed: {last_error}",
                    input_tokens=total_input_tokens,
                    output_tokens=total_output_tokens,
                    latency_ms=total_latency,
                    retries=attempts - 1,
                    attempted=False,
                )

    # Step 2: Extract result from code execution
    code_output = ""
    if code_result is not None:
        items = code_result.get("items")
        payload = code_result.get("payload")
        if isinstance(items, list):
            if len(items) == 1:
                code_output = json.dumps(items[0], ensure_ascii=False)
            else:
                code_output = json.dumps(items, ensure_ascii=False)
        elif payload is not None:
            code_output = json.dumps(payload, ensure_ascii=False)
        else:
            code_output = json.dumps(code_result, ensure_ascii=False)

    # Step 3: Ask LLM to extract final answer from code result
    answer_msg = (
        f"Question: {question.question_text}\n\n"
        f"Code query result:\n{code_output}\n\n"
        f"Give ONLY the final answer value."
    )

    try:
        answer_resp = call_llm(
            model=model,
            system_prompt=_SIFT_ANSWER_SYSTEM,
            user_message=answer_msg,
            api_key=api_key,
            temperature=temperature,
        )
    except Exception as exc:
        return _make_result(
            question,
            condition="sift",
            gold=gold,
            error=f"answer extraction failed: {exc}",
            input_tokens=total_input_tokens,
            output_tokens=total_output_tokens,
            latency_ms=total_latency,
            retries=attempts,
            attempted=False,
        )

    total_input_tokens += answer_resp.input_tokens
    total_output_tokens += answer_resp.output_tokens
    total_latency += answer_resp.latency_ms

    correct = evaluate_answer(
        answer_resp.text,
        gold,
        answer_type=question.answer_type,
        tolerance=question.tolerance,
    )

    return _make_result(
        question,
        condition="sift",
        gold=gold,
        llm_answer=answer_resp.text,
        correct=correct,
        input_tokens=total_input_tokens,
        output_tokens=total_output_tokens,
        latency_ms=total_latency,
        retries=attempts,
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
        default=_MAX_BASELINE_BYTES_DEFAULT,
        help="Max baseline payload size in bytes",
    )
    parser.add_argument(
        "--max-baseline-tokens",
        type=int,
        default=_MAX_BASELINE_TOKENS_DEFAULT,
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
        loaded[name] = _load_dataset(data_dir, name)

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
    continue_on_error = getattr(args, "continue_on_error", False)
    with create_runtime(data_dir=sift_data_dir) as runtime:
        for name in dataset_names:
            data = loaded[name]
            questions = get_questions_for_dataset(name)

            # Skip dataset entirely if no questions match filter.
            if question_filter and not any(
                q.question_id in question_filter for q in questions
            ):
                continue

            print(f"  Capturing {name} ...")
            capture_result = capture_payload(
                runtime,
                payload=data,
                dataset_name=name,
                question_id="all",
            )
            artifact_id = capture_result["artifact_id"]
            print(f"    artifact_id: {artifact_id}")

            print(f"  Describing {name} ...")
            describe_result = describe_artifact(
                runtime,
                artifact_id=artifact_id,
            )
            root_paths = extract_root_paths(describe_result)
            schema_text = _format_schema_for_prompt(describe_result)
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
