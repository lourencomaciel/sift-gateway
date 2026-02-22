#!/usr/bin/env python3
"""Automate OpenClaw prompt tests and validate transcript artifacts."""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
import json
from pathlib import Path
import re
import shlex
import subprocess
import sys
import time
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_NO_SIFT_PROMPT = ROOT / "docs" / "openclaw" / "prompts" / "prompt_no_sift.md"
DEFAULT_WITH_SIFT_PROMPT = (
    ROOT / "docs" / "openclaw" / "prompts" / "prompt_with_sift.md"
)
DEFAULT_NO_SIFT_LOG = ROOT / "tmp" / "openclaw_logs" / "run_log_no_sift.txt"
DEFAULT_WITH_SIFT_LOG = ROOT / "tmp" / "openclaw_logs" / "run_log_with_sift.txt"
DEFAULT_REPORT = ROOT / "tmp" / "openclaw_logs" / "automation_report.json"
FINAL_BLOCK_PATTERN = re.compile(
    r"FINAL_ANSWER=(?P<answer>[^\n]+)\n"
    r"TOTAL_CLI_OUTPUT_KB=(?P<kb>[0-9]+(?:\.[0-9]+)?)\n"
    r"TOTAL_RUNTIME_SECONDS=(?P<seconds>[0-9]+(?:\.[0-9]+)?)"
)


@dataclass(frozen=True)
class CaseConfig:
    """One prompt execution case."""

    name: str
    prompt_path: Path
    log_path: Path
    must_contain: tuple[str, ...]
    must_not_contain: tuple[str, ...]


@dataclass(frozen=True)
class CaseResult:
    """Result details for one prompt execution."""

    name: str
    ok: bool
    error: str | None
    runner_returncode: int | None
    runner_duration_seconds: float | None
    final_answer: str | None
    total_cli_output_kb: float | None
    total_runtime_seconds: float | None
    runner_stdout_bytes: int
    runner_stderr_bytes: int
    log_path: str


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run and validate OpenClaw prompt-based transcript tests."
    )
    parser.add_argument(
        "--runner",
        default="openclaw",
        help="Command used to run OpenClaw, e.g. 'openclaw' or 'npx openclaw'.",
    )
    parser.add_argument(
        "--prompt-mode",
        choices=["stdin", "arg", "file-arg"],
        default="stdin",
        help=(
            "How prompt text is passed to the runner: "
            "stdin, argument value, or argument file path."
        ),
    )
    parser.add_argument(
        "--prompt-flag",
        default="--prompt",
        help="Flag used with --prompt-mode=arg or file-arg.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=1800.0,
        help="Per-case runner timeout in seconds.",
    )
    parser.add_argument(
        "--no-sift-prompt",
        type=Path,
        default=DEFAULT_NO_SIFT_PROMPT,
        help="Prompt file for no-sift case.",
    )
    parser.add_argument(
        "--with-sift-prompt",
        type=Path,
        default=DEFAULT_WITH_SIFT_PROMPT,
        help="Prompt file for with-sift case.",
    )
    parser.add_argument(
        "--no-sift-log",
        type=Path,
        default=DEFAULT_NO_SIFT_LOG,
        help="Transcript file that no-sift prompt asks OpenClaw to write.",
    )
    parser.add_argument(
        "--with-sift-log",
        type=Path,
        default=DEFAULT_WITH_SIFT_LOG,
        help="Transcript file that with-sift prompt asks OpenClaw to write.",
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=DEFAULT_REPORT,
        help="JSON output report path.",
    )
    parser.add_argument(
        "--keep-existing-logs",
        action="store_true",
        help="Do not delete existing transcript files before each run.",
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Skip runner invocation and only validate existing logs.",
    )
    return parser.parse_args()


def _load_text(path: Path) -> str:
    if not path.exists():
        raise FileNotFoundError(f"missing file: {path}")
    return path.read_text(encoding="utf-8")


def _build_cases(args: argparse.Namespace) -> list[CaseConfig]:
    return [
        CaseConfig(
            name="no_sift",
            prompt_path=args.no_sift_prompt,
            log_path=args.no_sift_log,
            must_contain=(),
            must_not_contain=("sift-gateway",),
        ),
        CaseConfig(
            name="with_sift",
            prompt_path=args.with_sift_prompt,
            log_path=args.with_sift_log,
            must_contain=("sift-gateway run --json", "sift-gateway code --json"),
            must_not_contain=(),
        ),
    ]


def _format_cmd(cmd: list[str]) -> str:
    return shlex.join(cmd)


def _runner_command(
    *,
    runner_tokens: list[str],
    prompt_mode: str,
    prompt_flag: str,
    prompt_text: str,
    prompt_path: Path,
) -> tuple[list[str], str | None]:
    if prompt_mode == "stdin":
        return runner_tokens, prompt_text
    if prompt_mode == "arg":
        return [*runner_tokens, prompt_flag, prompt_text], None
    if prompt_mode == "file-arg":
        return [*runner_tokens, prompt_flag, str(prompt_path)], None
    raise ValueError(f"unsupported prompt mode: {prompt_mode}")


def _invoke_runner(
    *,
    runner_tokens: list[str],
    prompt_mode: str,
    prompt_flag: str,
    prompt_text: str,
    prompt_path: Path,
    timeout_seconds: float,
) -> tuple[int, float, str, str]:
    command, stdin_text = _runner_command(
        runner_tokens=runner_tokens,
        prompt_mode=prompt_mode,
        prompt_flag=prompt_flag,
        prompt_text=prompt_text,
        prompt_path=prompt_path,
    )
    started = time.perf_counter()
    completed = subprocess.run(
        command,
        input=stdin_text,
        text=True,
        capture_output=True,
        check=False,
        timeout=timeout_seconds,
    )
    duration = time.perf_counter() - started
    return completed.returncode, duration, completed.stdout, completed.stderr


def _parse_final_block(text: str) -> tuple[str, float, float]:
    matches = list(FINAL_BLOCK_PATTERN.finditer(text))
    if not matches:
        raise ValueError("log is missing required FINAL_ANSWER/TOTAL_* block")
    match = matches[-1]
    answer = match.group("answer").strip()
    if answer == "":
        raise ValueError("FINAL_ANSWER is empty")
    kb = float(match.group("kb"))
    seconds = float(match.group("seconds"))
    if kb < 0:
        raise ValueError("TOTAL_CLI_OUTPUT_KB must be non-negative")
    if seconds < 0:
        raise ValueError("TOTAL_RUNTIME_SECONDS must be non-negative")
    return answer, kb, seconds


def _validate_log(case: CaseConfig) -> tuple[str, float, float]:
    text = _load_text(case.log_path)
    for token in case.must_contain:
        if token not in text:
            raise ValueError(
                f"{case.name}: required token missing from log: {token!r}"
            )
    for token in case.must_not_contain:
        if token in text:
            raise ValueError(f"{case.name}: forbidden token found in log: {token!r}")
    return _parse_final_block(text)


def _run_case(
    case: CaseConfig,
    *,
    runner_tokens: list[str],
    prompt_mode: str,
    prompt_flag: str,
    timeout_seconds: float,
    keep_existing_logs: bool,
    validate_only: bool,
) -> CaseResult:
    try:
        prompt_text = _load_text(case.prompt_path)
        case.log_path.parent.mkdir(parents=True, exist_ok=True)
        if case.log_path.exists() and not keep_existing_logs:
            case.log_path.unlink()

        returncode: int | None = None
        runner_duration: float | None = None
        runner_stdout = ""
        runner_stderr = ""

        if not validate_only:
            returncode, runner_duration, runner_stdout, runner_stderr = _invoke_runner(
                runner_tokens=runner_tokens,
                prompt_mode=prompt_mode,
                prompt_flag=prompt_flag,
                prompt_text=prompt_text,
                prompt_path=case.prompt_path,
                timeout_seconds=timeout_seconds,
            )
            if returncode != 0:
                return CaseResult(
                    name=case.name,
                    ok=False,
                    error=f"runner exited with code {returncode}",
                    runner_returncode=returncode,
                    runner_duration_seconds=runner_duration,
                    final_answer=None,
                    total_cli_output_kb=None,
                    total_runtime_seconds=None,
                    runner_stdout_bytes=len(runner_stdout.encode("utf-8")),
                    runner_stderr_bytes=len(runner_stderr.encode("utf-8")),
                    log_path=str(case.log_path),
                )

        answer, kb, elapsed = _validate_log(case)
        return CaseResult(
            name=case.name,
            ok=True,
            error=None,
            runner_returncode=returncode,
            runner_duration_seconds=runner_duration,
            final_answer=answer,
            total_cli_output_kb=kb,
            total_runtime_seconds=elapsed,
            runner_stdout_bytes=len(runner_stdout.encode("utf-8")),
            runner_stderr_bytes=len(runner_stderr.encode("utf-8")),
            log_path=str(case.log_path),
        )
    except subprocess.TimeoutExpired:
        return CaseResult(
            name=case.name,
            ok=False,
            error=f"runner timed out after {timeout_seconds:.1f}s",
            runner_returncode=None,
            runner_duration_seconds=timeout_seconds,
            final_answer=None,
            total_cli_output_kb=None,
            total_runtime_seconds=None,
            runner_stdout_bytes=0,
            runner_stderr_bytes=0,
            log_path=str(case.log_path),
        )
    except Exception as exc:
        return CaseResult(
            name=case.name,
            ok=False,
            error=str(exc),
            runner_returncode=None,
            runner_duration_seconds=None,
            final_answer=None,
            total_cli_output_kb=None,
            total_runtime_seconds=None,
            runner_stdout_bytes=0,
            runner_stderr_bytes=0,
            log_path=str(case.log_path),
        )


def _write_report(
    *,
    report_path: Path,
    runner: str,
    prompt_mode: str,
    prompt_flag: str,
    timeout_seconds: float,
    validate_only: bool,
    results: list[CaseResult],
) -> None:
    report_path.parent.mkdir(parents=True, exist_ok=True)
    payload: dict[str, Any] = {
        "runner": runner,
        "prompt_mode": prompt_mode,
        "prompt_flag": prompt_flag,
        "timeout_seconds": timeout_seconds,
        "validate_only": validate_only,
        "all_ok": all(result.ok for result in results),
        "results": [asdict(result) for result in results],
    }
    report_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def main() -> int:
    """Run all cases, write the report, and return an exit status."""
    args = _parse_args()
    runner_tokens = shlex.split(args.runner)
    if not runner_tokens:
        sys.stderr.write("error: --runner resolved to empty command\n")
        return 2

    cases = _build_cases(args)
    results = [
        _run_case(
            case,
            runner_tokens=runner_tokens,
            prompt_mode=args.prompt_mode,
            prompt_flag=args.prompt_flag,
            timeout_seconds=args.timeout_seconds,
            keep_existing_logs=args.keep_existing_logs,
            validate_only=args.validate_only,
        )
        for case in cases
    ]
    _write_report(
        report_path=args.report,
        runner=args.runner,
        prompt_mode=args.prompt_mode,
        prompt_flag=args.prompt_flag,
        timeout_seconds=args.timeout_seconds,
        validate_only=args.validate_only,
        results=results,
    )

    all_ok = all(result.ok for result in results)
    for result in results:
        status = "PASS" if result.ok else "FAIL"
        summary = f"{status} {result.name}"
        if result.ok:
            summary += (
                f" answer={result.final_answer!r}"
                f" kb={result.total_cli_output_kb:.3f}"
                f" runtime_s={result.total_runtime_seconds:.3f}"
            )
        else:
            summary += f" error={result.error}"
        sys.stdout.write(summary + "\n")

    sys.stdout.write(f"report={args.report}\n")
    return 0 if all_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
