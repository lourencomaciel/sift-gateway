#!/usr/bin/env python3
"""Run extensive end-to-end smoke checks for the sift-gateway CLI."""

from __future__ import annotations

import argparse
from collections.abc import Callable
from dataclasses import dataclass
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
import traceback
from typing import Any


class SmokeAssertionError(RuntimeError):
    """Raised when one smoke trial assertion fails."""


@dataclass(frozen=True)
class CommandResult:
    """One CLI invocation result."""

    command: list[str]
    returncode: int
    duration_ms: float
    stdout: str
    stderr: str


@dataclass(frozen=True)
class TrialResult:
    """One smoke trial result."""

    name: str
    ok: bool
    duration_ms: float
    details: dict[str, Any]
    error: str | None
    traceback: str | None
    commands: list[CommandResult]


def _assert(condition: bool, message: str) -> None:
    if not condition:
        raise SmokeAssertionError(message)


def _require_dict(value: Any, *, label: str) -> dict[str, Any]:
    _assert(isinstance(value, dict), f"{label} must be a JSON object")
    return value


def _require_str(value: Any, *, label: str) -> str:
    _assert(
        isinstance(value, str) and bool(value.strip()),
        f"{label} must be a non-empty string",
    )
    return value


def _write_executable(path: Path, content: str) -> Path:
    path.write_text(content, encoding="utf-8")
    path.chmod(0o755)
    return path


def _write_line(text: str, *, stream: Any | None = None) -> None:
    target = stream if stream is not None else sys.stdout
    target.write(text)
    target.write("\n")


class SmokeContext:
    """Execution context shared across all smoke trials."""

    def __init__(
        self,
        *,
        gateway_bin: Path,
        data_dir: Path,
        verbose: bool,
    ) -> None:
        """Initialize execution context for the smoke run."""
        self.gateway_bin = gateway_bin
        self.data_dir = data_dir
        self.verbose = verbose
        self.state: dict[str, Any] = {}
        self._trial_commands: list[CommandResult] = []

    def begin_trial(self) -> None:
        """Start recording commands for one trial."""
        self._trial_commands = []

    def consume_trial_commands(self) -> list[CommandResult]:
        """Return and reset recorded commands for one trial."""
        commands = self._trial_commands
        self._trial_commands = []
        return commands

    def _format_command(self, command: list[str]) -> str:
        return shlex.join(command)

    def cli(
        self,
        args: list[str],
        *,
        input_text: str | None = None,
        env_overrides: dict[str, str] | None = None,
    ) -> CommandResult:
        """Run one sift-gateway command and capture result details."""
        command = [
            str(self.gateway_bin),
            "--data-dir",
            str(self.data_dir),
            *args,
        ]
        if self.verbose:
            _write_line(f"$ {self._format_command(command)}")
        env = dict(os.environ)
        if env_overrides:
            env.update(env_overrides)
        started = time.perf_counter()
        completed = subprocess.run(
            command,
            input=input_text,
            text=True,
            capture_output=True,
            check=False,
            env=env,
        )
        duration_ms = (time.perf_counter() - started) * 1000.0
        result = CommandResult(
            command=command,
            returncode=completed.returncode,
            duration_ms=duration_ms,
            stdout=completed.stdout,
            stderr=completed.stderr,
        )
        self._trial_commands.append(result)
        return result

    def cli_json(
        self,
        args: list[str],
        *,
        input_text: str | None = None,
        env_overrides: dict[str, str] | None = None,
    ) -> tuple[CommandResult, dict[str, Any]]:
        """Run one command expected to return a JSON object on stdout."""
        result = self.cli(
            args,
            input_text=input_text,
            env_overrides=env_overrides,
        )
        raw = result.stdout.strip()
        _assert(raw != "", "expected JSON on stdout but got empty output")
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise SmokeAssertionError(
                f"invalid JSON output: {exc}"
            ) from exc
        return result, _require_dict(payload, label="command output")

    def capture_json_artifact(self, payload_json: str) -> dict[str, Any]:
        """Capture one JSON payload via `run --json` and return response."""
        result, payload = self.cli_json(
            [
                "run",
                "--json",
                "--",
                "echo",
                payload_json,
            ]
        )
        _assert(
            result.returncode == 0,
            f"capture command failed with exit {result.returncode}",
        )
        _require_str(payload.get("artifact_id"), label="artifact_id")
        return payload


def _prepare_fixtures(fixtures_dir: Path) -> dict[str, Path]:
    fixtures_dir.mkdir(parents=True, exist_ok=True)
    page1 = _write_executable(
        fixtures_dir / "emit_page1.sh",
        (
            "#!/bin/sh\n"
            "printf '{\"next\":\"?after=CURSOR_2&limit=100\","
            "\"items\":[{\"id\":1,\"name\":\"alpha\"},"
            "{\"id\":2,\"name\":\"beta\"}]}'\n"
        ),
    )
    page2 = _write_executable(
        fixtures_dir / "emit_page2.sh",
        (
            "#!/bin/sh\n"
            "printf '{\"items\":[{\"id\":3,\"name\":\"gamma\"},"
            "{\"id\":4,\"name\":\"delta\"}]}'\n"
        ),
    )
    incompat_page1 = _write_executable(
        fixtures_dir / "emit_incompat_page1.sh",
        (
            "#!/bin/sh\n"
            "printf '{\"next\":\"?after=CURSOR_2&limit=100\","
            "\"items\":[{\"id\":1,\"name\":\"alpha\"}]}'\n"
        ),
    )
    incompat_page2 = _write_executable(
        fixtures_dir / "emit_incompat_page2.sh",
        (
            "#!/bin/sh\n"
            "printf '{\"items\":[{\"id\":2,\"value\":42}]}'\n"
        ),
    )
    code_file = fixtures_dir / "code_file.py"
    code_file.write_text(
        (
            "def run(data, schema, params):\n"
            "    return {\n"
            "        'rows': len(data),\n"
            "        'tag': params.get('tag'),\n"
            "    }\n"
        ),
        encoding="utf-8",
    )
    return {
        "page1_script": page1,
        "page2_script": page2,
        "incompat_page1_script": incompat_page1,
        "incompat_page2_script": incompat_page2,
        "code_file": code_file,
        "missing_code_file": fixtures_dir / "missing_code.py",
    }


def _expect_cli_error(
    ctx: SmokeContext,
    *,
    args: list[str],
    expected_message: str,
    expected_exit: int = 1,
) -> None:
    """Run one command and assert its stderr contains the expected message."""
    result = ctx.cli(args)
    _assert(
        result.returncode == expected_exit,
        f"expected exit={expected_exit}, got {result.returncode}",
    )
    _assert(expected_message in result.stderr, "missing validation message")


def _trial_version(ctx: SmokeContext) -> dict[str, Any]:
    result = ctx.cli(["--version"])
    _assert(result.returncode == 0, "expected --version to succeed")
    version = result.stdout.strip()
    _assert(version.startswith("sift-gateway "), "unexpected --version output")
    return {"version": version}


def _trial_help(ctx: SmokeContext) -> dict[str, Any]:
    result = ctx.cli(["--help"])
    _assert(result.returncode == 0, "expected --help to succeed")
    text = result.stdout
    _assert("usage:" in text.lower(), "--help missing usage text")
    _assert("init" in text, "--help missing init command")
    _assert("upstream" in text, "--help missing upstream command")
    return {}


def _trial_check(ctx: SmokeContext) -> dict[str, Any]:
    result = ctx.cli(["--check"])
    _assert(result.returncode == 0, "expected --check to succeed")
    return {}


def _trial_run_json_basic(ctx: SmokeContext) -> dict[str, Any]:
    result, payload = ctx.cli_json(
        [
            "run",
            "--json",
            "--",
            "echo",
            '{"items":[{"id":10,"name":"x"},{"id":11,"name":"y"}]}',
        ]
    )
    _assert(result.returncode == 0, "basic run should exit 0")
    artifact_id = _require_str(payload.get("artifact_id"), label="artifact_id")
    _assert(payload.get("records") == 2, "expected records=2")
    _assert(payload.get("command_exit_code") == 0, "expected exit code 0")
    _assert(payload.get("response_mode") in {"full", "schema_ref"}, "bad mode")
    ctx.state["art_basic"] = artifact_id
    return {
        "artifact_id": artifact_id,
        "response_mode": payload.get("response_mode"),
    }


def _trial_run_json_stdin(ctx: SmokeContext) -> dict[str, Any]:
    result, payload = ctx.cli_json(
        ["run", "--stdin", "--json"],
        input_text='{"items":[{"id":20},{"id":21},{"id":22}]}',
    )
    _assert(result.returncode == 0, "stdin run should exit 0")
    _assert(payload.get("records") == 3, "expected stdin records=3")
    _assert(payload.get("command_exit_code") == 0, "expected stdin exit=0")
    return {"artifact_id": payload.get("artifact_id")}


def _trial_run_tags_ttl(ctx: SmokeContext) -> dict[str, Any]:
    result, payload = ctx.cli_json(
        [
            "run",
            "--ttl",
            "30m",
            "--tag",
            "ci,smoke",
            "--tag",
            "nightly",
            "--json",
            "--",
            "echo",
            '{"items":[{"id":1}]}',
        ]
    )
    _assert(result.returncode == 0, "tag/ttl run should exit 0")
    tags = payload.get("tags")
    _assert(tags == ["ci", "smoke", "nightly"], "unexpected normalized tags")
    expires_at = payload.get("expires_at")
    _assert(
        isinstance(expires_at, str) and bool(expires_at),
        "expected expires_at in response",
    )
    return {"artifact_id": payload.get("artifact_id"), "tags": tags}


def _trial_run_nonzero_exit(ctx: SmokeContext) -> dict[str, Any]:
    result, payload = ctx.cli_json(
        [
            "run",
            "--json",
            "--",
            "/bin/sh",
            "-lc",
            'printf "{\\"items\\":[1]}"; exit 7',
        ]
    )
    _assert(result.returncode == 7, "process exit must mirror wrapped command")
    _assert(payload.get("command_exit_code") == 7, "payload exit code mismatch")
    _assert(payload.get("status") == "error", "expected status=error")
    return {"artifact_id": payload.get("artifact_id")}


def _trial_run_invalid_stdin_with_command(ctx: SmokeContext) -> dict[str, Any]:
    _expect_cli_error(
        ctx,
        args=["run", "--stdin", "--json", "--", "echo", "hello"],
        expected_message="--stdin cannot be combined with a command",
    )
    return {}


def _trial_run_invalid_stdin_with_continue(ctx: SmokeContext) -> dict[str, Any]:
    artifact_id = _require_str(ctx.state.get("art_basic"), label="art_basic")
    _expect_cli_error(
        ctx,
        args=[
            "run",
            "--stdin",
            "--continue-from",
            artifact_id,
            "--json",
        ],
        expected_message="--stdin cannot be combined with --continue-from",
    )
    return {}


def _trial_run_invalid_missing_command(ctx: SmokeContext) -> dict[str, Any]:
    _expect_cli_error(
        ctx,
        args=["run"],
        expected_message="run requires a command or --stdin",
    )
    return {}


def _trial_run_pagination(ctx: SmokeContext) -> dict[str, Any]:
    page1_script = _require_str(
        str(ctx.state["fixtures"]["page1_script"]),
        label="page1 script",
    )
    result, payload = ctx.cli_json(
        [
            "run",
            "--json",
            "--",
            page1_script,
            "--after",
            "CURSOR_1",
            "--limit",
            "100",
        ]
    )
    _assert(result.returncode == 0, "pagination page1 should exit 0")
    artifact_id = _require_str(payload.get("artifact_id"), label="artifact_id")
    pagination = _require_dict(payload.get("pagination"), label="pagination")
    _assert(pagination.get("has_next_page") is True, "expected has_next_page")
    _assert(
        pagination.get("next_params") == {"after": "CURSOR_2", "limit": 100},
        "unexpected next_params",
    )
    next_action = _require_dict(
        pagination.get("next_action"),
        label="next_action",
    )
    _assert(next_action.get("command") == "run", "bad next_action.command")
    _assert(
        next_action.get("continue_from_artifact_id") == artifact_id,
        "next_action continue_from_artifact_id mismatch",
    )
    ctx.state["art_page1"] = artifact_id
    return {"artifact_id": artifact_id}


def _trial_run_continue_from(ctx: SmokeContext) -> dict[str, Any]:
    page2_script = _require_str(
        str(ctx.state["fixtures"]["page2_script"]),
        label="page2 script",
    )
    parent_artifact_id = _require_str(
        ctx.state.get("art_page1"),
        label="art_page1",
    )
    result, payload = ctx.cli_json(
        [
            "run",
            "--continue-from",
            parent_artifact_id,
            "--json",
            "--",
            page2_script,
            "--after",
            "CURSOR_2",
            "--limit",
            "100",
        ]
    )
    _assert(result.returncode == 0, "continuation page2 should exit 0")
    artifact_id = _require_str(payload.get("artifact_id"), label="artifact_id")
    _assert(
        payload.get("source_artifact_id") == parent_artifact_id,
        "source_artifact_id mismatch",
    )
    lineage = _require_dict(payload.get("lineage"), label="lineage")
    _assert(lineage.get("chain_seq") == 1, "expected chain_seq=1")
    _assert(
        lineage.get("parent_artifact_id") == parent_artifact_id,
        "parent lineage mismatch",
    )
    pagination = _require_dict(payload.get("pagination"), label="pagination")
    _assert(
        pagination.get("has_next_page") is False,
        "expected continuation has_next_page=false",
    )
    ctx.state["art_page2"] = artifact_id
    return {"artifact_id": artifact_id}


def _trial_code_single_expr(ctx: SmokeContext) -> dict[str, Any]:
    artifact_id = _require_str(ctx.state.get("art_basic"), label="art_basic")
    result, payload = ctx.cli_json(
        [
            "code",
            artifact_id,
            "$.items",
            "--expr",
            "len(df)",
            "--json",
        ]
    )
    _assert(result.returncode == 0, "single expr query should exit 0")
    _assert(payload.get("payload") == 2, "single expr payload mismatch")
    _assert(payload.get("total_matched") == 1, "single expr total_mismatch")
    return {"artifact_id": payload.get("artifact_id")}


def _trial_code_single_inline_with_params(ctx: SmokeContext) -> dict[str, Any]:
    artifact_id = _require_str(ctx.state.get("art_basic"), label="art_basic")
    result, payload = ctx.cli_json(
        [
            "code",
            artifact_id,
            "$.items",
            "--code",
            (
                "def run(data, schema, params): "
                "return {'rows': len(data), 'param': params.get('x')}"
            ),
            "--params",
            '{"x": 7}',
            "--json",
        ]
    )
    _assert(result.returncode == 0, "inline code query should exit 0")
    result_payload = _require_dict(payload.get("payload"), label="payload")
    _assert(result_payload.get("rows") == 2, "inline rows mismatch")
    _assert(result_payload.get("param") == 7, "inline param mismatch")
    return {"artifact_id": payload.get("artifact_id")}


def _trial_code_single_file(ctx: SmokeContext) -> dict[str, Any]:
    artifact_id = _require_str(ctx.state.get("art_basic"), label="art_basic")
    code_file = _require_str(
        str(ctx.state["fixtures"]["code_file"]),
        label="code file",
    )
    result, payload = ctx.cli_json(
        [
            "code",
            artifact_id,
            "$.items",
            "--file",
            code_file,
            "--params",
            '{"tag":"file"}',
            "--json",
        ]
    )
    _assert(result.returncode == 0, "file code query should exit 0")
    result_payload = _require_dict(payload.get("payload"), label="payload")
    _assert(result_payload.get("rows") == 2, "file rows mismatch")
    _assert(result_payload.get("tag") == "file", "file param mismatch")
    return {"artifact_id": payload.get("artifact_id")}


def _trial_code_multi_expr_shared_root(ctx: SmokeContext) -> dict[str, Any]:
    left = ctx.capture_json_artifact('{"items":[{"id":21},{"id":22}]}')
    right = ctx.capture_json_artifact('{"items":[{"id":31},{"id":32}]}')
    art_left = _require_str(left.get("artifact_id"), label="left artifact")
    art_right = _require_str(right.get("artifact_id"), label="right artifact")
    ctx.state["art_multi_left"] = art_left
    ctx.state["art_multi_right"] = art_right
    result, payload = ctx.cli_json(
        [
            "code",
            "--scope",
            "single",
            "--artifact-id",
            art_left,
            "--artifact-id",
            art_right,
            "--root-path",
            "$.items",
            "--expr",
            "len(df)",
            "--json",
        ]
    )
    _assert(result.returncode == 0, "multi shared-root expr should exit 0")
    _assert(payload.get("payload") == 4, "multi shared-root payload mismatch")
    return {"artifact_id": payload.get("artifact_id")}


def _trial_code_multi_expr_per_artifact_roots(ctx: SmokeContext) -> dict[str, Any]:
    users = ctx.capture_json_artifact('{"users":[{"id":"u1"},{"id":"u2"}]}')
    orders = ctx.capture_json_artifact('{"orders":[{"id":"o1"},{"id":"o2"}]}')
    users_artifact = _require_str(
        users.get("artifact_id"),
        label="users artifact",
    )
    orders_artifact = _require_str(
        orders.get("artifact_id"),
        label="orders artifact",
    )
    result, payload = ctx.cli_json(
        [
            "code",
            "--scope",
            "single",
            "--artifact-id",
            users_artifact,
            "--artifact-id",
            orders_artifact,
            "--root-path",
            "$.users",
            "--root-path",
            "$.orders",
            "--expr",
            "sum(frame.shape[0] for frame in artifact_frames.values())",
            "--json",
        ]
    )
    _assert(result.returncode == 0, "multi per-root expr should exit 0")
    _assert(payload.get("payload") == 4, "multi per-root payload mismatch")
    return {"artifact_id": payload.get("artifact_id")}


def _trial_code_multi_legacy_signature_rejected(
    ctx: SmokeContext,
) -> dict[str, Any]:
    art_left = _require_str(
        ctx.state.get("art_multi_left"),
        label="art_multi_left",
    )
    art_right = _require_str(
        ctx.state.get("art_multi_right"),
        label="art_multi_right",
    )
    result, payload = ctx.cli_json(
        [
            "code",
            "--scope",
            "single",
            "--artifact-id",
            art_left,
            "--artifact-id",
            art_right,
            "--root-path",
            "$.items",
            "--code",
            "def run(data, schema, params): return len(data)",
            "--json",
        ]
    )
    _assert(result.returncode == 1, "legacy multi signature should exit 1")
    _assert(payload.get("code") == "INVALID_ARGUMENT", "unexpected error code")
    details = _require_dict(payload.get("details"), label="details")
    _assert(
        details.get("code") == "CODE_ENTRYPOINT_MISSING",
        "unexpected details.code",
    )
    return {"message": payload.get("message")}


def _trial_code_multi_incompatible_lineage_hint(
    ctx: SmokeContext,
) -> dict[str, Any]:
    incompat_page1_script = _require_str(
        str(ctx.state["fixtures"]["incompat_page1_script"]),
        label="incompat page1 script",
    )
    incompat_page2_script = _require_str(
        str(ctx.state["fixtures"]["incompat_page2_script"]),
        label="incompat page2 script",
    )
    _, page1_payload = ctx.cli_json(
        [
            "run",
            "--json",
            "--",
            incompat_page1_script,
            "--after",
            "CURSOR_1",
            "--limit",
            "100",
        ]
    )
    page1_artifact = _require_str(
        page1_payload.get("artifact_id"),
        label="incompat page1 artifact",
    )
    _, page2_payload = ctx.cli_json(
        [
            "run",
            "--continue-from",
            page1_artifact,
            "--json",
            "--",
            incompat_page2_script,
            "--after",
            "CURSOR_2",
            "--limit",
            "100",
        ]
    )
    page2_artifact = _require_str(
        page2_payload.get("artifact_id"),
        label="incompat page2 artifact",
    )
    result, payload = ctx.cli_json(
        [
            "code",
            "--artifact-id",
            page1_artifact,
            "--artifact-id",
            page2_artifact,
            "--root-path",
            "$.items",
            "--expr",
            "len(df)",
            "--json",
        ]
    )
    _assert(
        result.returncode == 1,
        "incompatible lineage query should fail with exit 1",
    )
    _assert(payload.get("code") == "INVALID_ARGUMENT", "unexpected error code")
    details = _require_dict(payload.get("details"), label="details")
    _assert(
        details.get("code") == "INCOMPATIBLE_LINEAGE_SCHEMA",
        "unexpected details.code",
    )
    hint = _require_str(details.get("hint"), label="details.hint")
    _assert("scope=single" in hint, "hint should mention scope=single")
    return {"hint": hint}


def _trial_code_invalid_params_json(ctx: SmokeContext) -> dict[str, Any]:
    artifact_id = _require_str(ctx.state.get("art_basic"), label="art_basic")
    _expect_cli_error(
        ctx,
        args=[
            "code",
            artifact_id,
            "$.items",
            "--code",
            "def run(data, schema, params): return data",
            "--params",
            "{bad",
        ],
        expected_message="invalid --params JSON",
    )
    return {}


def _trial_code_invalid_mixed_positional_and_flags(
    ctx: SmokeContext,
) -> dict[str, Any]:
    _expect_cli_error(
        ctx,
        args=[
            "code",
            "art_1",
            "$.items",
            "--artifact-id",
            "art_2",
            "--root-path",
            "$.items",
            "--expr",
            "len(df)",
        ],
        expected_message="cannot mix positional artifact_id/root_path",
    )
    return {}


def _trial_code_invalid_mismatched_root_paths(ctx: SmokeContext) -> dict[str, Any]:
    _expect_cli_error(
        ctx,
        args=[
            "code",
            "--artifact-id",
            "art_1",
            "--artifact-id",
            "art_2",
            "--root-path",
            "$.one",
            "--root-path",
            "$.two",
            "--root-path",
            "$.three",
            "--expr",
            "len(df)",
        ],
        expected_message="provide one --root-path or repeat --root-path",
    )
    return {}


def _trial_code_invalid_partial_positional(ctx: SmokeContext) -> dict[str, Any]:
    _expect_cli_error(
        ctx,
        args=[
            "code",
            "art_1",
            "--expr",
            "len(df)",
        ],
        expected_message="requires both artifact_id and root_path",
    )
    return {}


def _trial_code_invalid_duplicate_artifact_ids(ctx: SmokeContext) -> dict[str, Any]:
    _expect_cli_error(
        ctx,
        args=[
            "code",
            "--artifact-id",
            "art_dup",
            "--artifact-id",
            "art_dup",
            "--root-path",
            "$.items",
            "--expr",
            "len(df)",
        ],
        expected_message="duplicate --artifact-id values are not supported",
    )
    return {}


def _trial_code_invalid_missing_file(ctx: SmokeContext) -> dict[str, Any]:
    artifact_id = _require_str(ctx.state.get("art_basic"), label="art_basic")
    missing_file = _require_str(
        str(ctx.state["fixtures"]["missing_code_file"]),
        label="missing code file path",
    )
    _expect_cli_error(
        ctx,
        args=[
            "code",
            artifact_id,
            "$.items",
            "--file",
            missing_file,
        ],
        expected_message="code file not found",
    )
    return {}


TRIALS: list[tuple[str, Callable[[SmokeContext], dict[str, Any]]]] = [
    ("version", _trial_version),
    ("help", _trial_help),
    ("check", _trial_check),
    ("run_json_basic", _trial_run_json_basic),
    ("run_json_stdin", _trial_run_json_stdin),
    ("run_tags_ttl", _trial_run_tags_ttl),
    ("run_nonzero_exit", _trial_run_nonzero_exit),
    ("run_invalid_stdin_with_command", _trial_run_invalid_stdin_with_command),
    ("run_invalid_stdin_with_continue", _trial_run_invalid_stdin_with_continue),
    ("run_invalid_missing_command", _trial_run_invalid_missing_command),
    ("run_pagination", _trial_run_pagination),
    ("run_continue_from", _trial_run_continue_from),
    ("code_single_expr", _trial_code_single_expr),
    ("code_single_inline_with_params", _trial_code_single_inline_with_params),
    ("code_single_file", _trial_code_single_file),
    ("code_multi_expr_shared_root", _trial_code_multi_expr_shared_root),
    (
        "code_multi_expr_per_artifact_roots",
        _trial_code_multi_expr_per_artifact_roots,
    ),
    (
        "code_multi_legacy_signature_rejected",
        _trial_code_multi_legacy_signature_rejected,
    ),
    (
        "code_multi_incompatible_lineage_hint",
        _trial_code_multi_incompatible_lineage_hint,
    ),
    ("code_invalid_params_json", _trial_code_invalid_params_json),
    (
        "code_invalid_mixed_positional_and_flags",
        _trial_code_invalid_mixed_positional_and_flags,
    ),
    ("code_invalid_mismatched_root_paths", _trial_code_invalid_mismatched_root_paths),
    ("code_invalid_partial_positional", _trial_code_invalid_partial_positional),
    ("code_invalid_duplicate_artifact_ids", _trial_code_invalid_duplicate_artifact_ids),
    ("code_invalid_missing_file", _trial_code_invalid_missing_file),
]


def _format_command(command: list[str]) -> str:
    return shlex.join(command)


def _short_output(text: str, *, max_chars: int = 300) -> str:
    normalized = text.strip()
    if len(normalized) <= max_chars:
        return normalized
    return normalized[: max_chars - 3] + "..."


def _emit_human(results: list[TrialResult], *, work_dir: Path, data_dir: Path) -> None:
    for result in results:
        status = "ok" if result.ok else "fail"
        _write_line(f"[{status}] {result.name} ({result.duration_ms:.1f} ms)")
        if result.error:
            _write_line(f"  error: {result.error}")
        if result.details:
            _write_line(
                "  details: "
                + json.dumps(result.details, ensure_ascii=False, sort_keys=True)
            )
        for command in result.commands:
            _write_line(
                "  cmd: "
                + _format_command(command.command)
                + f" (exit {command.returncode}, {command.duration_ms:.1f} ms)"
            )
            if command.stdout.strip():
                _write_line(f"  stdout: {_short_output(command.stdout)}")
            if command.stderr.strip():
                _write_line(f"  stderr: {_short_output(command.stderr)}")
    _write_line(f"work_dir={work_dir}")
    _write_line(f"data_dir={data_dir}")


def _emit_json(results: list[TrialResult], *, work_dir: Path, data_dir: Path) -> None:
    payload = {
        "ok": all(result.ok for result in results),
        "work_dir": str(work_dir),
        "data_dir": str(data_dir),
        "trials": [
            {
                "name": result.name,
                "ok": result.ok,
                "duration_ms": result.duration_ms,
                "details": result.details,
                "error": result.error,
                "traceback": result.traceback,
                "commands": [
                    {
                        "command": command.command,
                        "returncode": command.returncode,
                        "duration_ms": command.duration_ms,
                        "stdout": command.stdout,
                        "stderr": command.stderr,
                    }
                    for command in result.commands
                ],
            }
            for result in results
        ],
    }
    _write_line(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run extensive sift-gateway CLI smoke checks.",
    )
    parser.add_argument(
        "--gateway-bin",
        default=(
            "./.venv/bin/sift-gateway"
            if Path("./.venv/bin/sift-gateway").exists()
            else "sift-gateway"
        ),
        help="Path to sift-gateway executable.",
    )
    parser.add_argument(
        "--work-dir",
        default=None,
        help="Directory for temporary fixture files.",
    )
    parser.add_argument(
        "--data-dir",
        default=None,
        help="Gateway data dir to use during smoke tests.",
    )
    parser.add_argument(
        "--keep-work-dir",
        action="store_true",
        help="Do not delete auto-created work dir on exit.",
    )
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="Stop after first failed trial.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable JSON report.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print each command before executing it.",
    )
    return parser


def _resolve_gateway_bin(raw_gateway_bin: str) -> Path | None:
    """Resolve a gateway binary path from explicit path or PATH lookup."""
    candidate = Path(raw_gateway_bin)
    if candidate.exists():
        return candidate
    resolved = shutil.which(raw_gateway_bin)
    if resolved is None:
        return None
    return Path(resolved)


def _prepare_runtime_paths(
    *,
    work_dir_arg: str | None,
    data_dir_arg: str | None,
) -> tuple[Path, Path, bool]:
    """Create/resolve work and data directories for one smoke run."""
    owned_work_dir = work_dir_arg is None
    if owned_work_dir:
        work_dir = Path(tempfile.mkdtemp(prefix="sift-smoke-cli-"))
    else:
        work_dir = Path(work_dir_arg).expanduser().resolve()
        work_dir.mkdir(parents=True, exist_ok=True)

    if data_dir_arg is None:
        data_dir = work_dir / "data"
    else:
        data_dir = Path(data_dir_arg).expanduser().resolve()
    data_dir.mkdir(parents=True, exist_ok=True)
    return work_dir, data_dir, owned_work_dir


def _run_trials(
    context: SmokeContext,
    *,
    fail_fast: bool,
) -> list[TrialResult]:
    """Execute all configured smoke trials and collect results."""
    results: list[TrialResult] = []
    for name, trial_fn in TRIALS:
        context.begin_trial()
        started = time.perf_counter()
        details: dict[str, Any] = {}
        error: str | None = None
        tb: str | None = None
        ok = True
        try:
            details = trial_fn(context)
        except Exception as exc:
            ok = False
            error = str(exc)
            tb = traceback.format_exc()
        duration_ms = (time.perf_counter() - started) * 1000.0
        results.append(
            TrialResult(
                name=name,
                ok=ok,
                duration_ms=duration_ms,
                details=details,
                error=error,
                traceback=tb,
                commands=context.consume_trial_commands(),
            )
        )
        if fail_fast and not ok:
            break
    return results


def _cleanup_work_dir(
    *,
    work_dir: Path,
    owned_work_dir: bool,
    keep_work_dir: bool,
    results: list[TrialResult],
) -> None:
    """Delete auto-created work directory when configured and successful."""
    should_delete = (
        owned_work_dir
        and not keep_work_dir
        and not any(not result.ok for result in results)
    )
    if should_delete:
        shutil.rmtree(work_dir, ignore_errors=True)


def main() -> int:
    """Run smoke trials and return a process exit code."""
    args = _build_parser().parse_args()

    gateway_bin = _resolve_gateway_bin(args.gateway_bin)
    if gateway_bin is None:
        _write_line(
            f"gateway binary not found: {args.gateway_bin}",
            stream=sys.stderr,
        )
        return 1

    if not os.access(gateway_bin, os.X_OK):
        _write_line(
            f"gateway binary is not executable: {gateway_bin}",
            stream=sys.stderr,
        )
        return 1

    work_dir, data_dir, owned_work_dir = _prepare_runtime_paths(
        work_dir_arg=args.work_dir,
        data_dir_arg=args.data_dir,
    )

    fixtures = _prepare_fixtures(work_dir / "fixtures")
    context = SmokeContext(
        gateway_bin=gateway_bin.resolve(),
        data_dir=data_dir,
        verbose=args.verbose,
    )
    context.state["fixtures"] = fixtures

    results: list[TrialResult] = []
    try:
        results = _run_trials(context, fail_fast=args.fail_fast)
    finally:
        _cleanup_work_dir(
            work_dir=work_dir,
            owned_work_dir=owned_work_dir,
            keep_work_dir=args.keep_work_dir,
            results=results,
        )

    if args.json:
        _emit_json(results, work_dir=work_dir, data_dir=data_dir)
    else:
        _emit_human(results, work_dir=work_dir, data_dir=data_dir)

    return 0 if all(result.ok for result in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
