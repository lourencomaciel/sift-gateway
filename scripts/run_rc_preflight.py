#!/usr/bin/env python3
"""Run release-candidate preflight checks in one command."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
import os
import shlex
import subprocess
import sys
import time
from typing import Any


@dataclass(frozen=True)
class Step:
    """One preflight check command."""

    name: str
    command: list[str]


@dataclass(frozen=True)
class StepResult:
    """Execution result for one preflight step."""

    name: str
    command: list[str]
    returncode: int
    duration_ms: float
    stdout: str
    stderr: str

    @property
    def ok(self) -> bool:
        """Return whether the step exited successfully."""
        return self.returncode == 0


def _write_line(text: str, *, stream: Any | None = None) -> None:
    target = stream if stream is not None else sys.stdout
    target.write(text)
    target.write("\n")


def _format_command(command: list[str]) -> str:
    return shlex.join(command)


def _run_step(step: Step, *, env: dict[str, str]) -> StepResult:
    start = time.perf_counter()
    completed = subprocess.run(
        step.command,
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )
    duration_ms = (time.perf_counter() - start) * 1000.0
    return StepResult(
        name=step.name,
        command=step.command,
        returncode=completed.returncode,
        duration_ms=duration_ms,
        stdout=completed.stdout,
        stderr=completed.stderr,
    )


def _build_steps(args: argparse.Namespace) -> list[Step]:
    steps: list[Step] = []
    if not args.skip_lint:
        steps.append(
            Step(
                name="lint",
                command=["uv", "run", "python", "-m", "ruff", "check", "src", "tests"],
            )
        )
    if not args.skip_types:
        steps.append(
            Step(
                name="typecheck",
                command=["uv", "run", "python", "-m", "mypy", "src"],
            )
        )
    if not args.skip_tests:
        steps.append(
            Step(
                name="tests",
                command=["uv", "run", "python", "-m", "pytest", "tests/unit", "-q"],
            )
        )
    if not args.skip_docs:
        steps.append(
            Step(
                name="docs_contract",
                command=["uv", "run", "python", "scripts/check_docs_consistency.py"],
            )
        )
    if not args.skip_build:
        steps.append(Step(name="build", command=["uv", "build"]))
    if not args.skip_smoke:
        steps.extend(
            [
                Step(
                    name="smoke_cli_version",
                    command=["uv", "run", "sift-gateway", "--version"],
                ),
                Step(
                    name="smoke_cli_list",
                    command=[
                        "uv",
                        "run",
                        "sift-gateway",
                        "list",
                        "--limit",
                        "1",
                        "--json",
                    ],
                ),
                Step(
                    name="smoke_mcp_check",
                    command=["uv", "run", "sift-gateway", "--check"],
                ),
            ]
        )
    return steps


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run local release-candidate preflight checks.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable JSON summary.",
    )
    parser.add_argument(
        "--uv-cache-dir",
        default=os.environ.get("UV_CACHE_DIR", "/tmp/uv-cache"),
        help="UV cache dir to use for all checks.",
    )
    parser.add_argument("--skip-lint", action="store_true")
    parser.add_argument("--skip-types", action="store_true")
    parser.add_argument("--skip-tests", action="store_true")
    parser.add_argument("--skip-docs", action="store_true")
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--skip-smoke", action="store_true")
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Run all steps even when one fails.",
    )
    return parser


def _emit_human(results: list[StepResult]) -> None:
    for result in results:
        status = "ok" if result.ok else "fail"
        _write_line(
            f"[{status}] {result.name} "
            f"({result.duration_ms:.1f} ms): "
            f"{_format_command(result.command)}"
        )
        if result.stdout.strip():
            _write_line(result.stdout.rstrip())
        if result.stderr.strip():
            _write_line(result.stderr.rstrip(), stream=sys.stderr)


def _emit_json(results: list[StepResult]) -> None:
    payload = {
        "ok": all(result.ok for result in results),
        "steps": [
            {
                "name": result.name,
                "command": result.command,
                "returncode": result.returncode,
                "duration_ms": result.duration_ms,
                "stdout": result.stdout,
                "stderr": result.stderr,
            }
            for result in results
        ],
    }
    _write_line(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))


def main() -> int:
    """Execute selected preflight checks and return process exit code."""
    args = _build_parser().parse_args()
    steps = _build_steps(args)
    if not steps:
        _write_line("no checks selected")
        return 0

    env = dict(os.environ)
    env["UV_CACHE_DIR"] = args.uv_cache_dir

    results: list[StepResult] = []
    for step in steps:
        result = _run_step(step, env=env)
        results.append(result)
        if not result.ok and not args.continue_on_error:
            break

    if args.json:
        _emit_json(results)
    else:
        _emit_human(results)

    return 0 if all(result.ok for result in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
