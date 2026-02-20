from __future__ import annotations

from contextlib import contextmanager
import io
import json
from pathlib import Path
import subprocess
from typing import Any

import pytest

from sift_gateway import cli_main


class _FakeRuntime:
    pass


@contextmanager
def _fake_runtime_context(*, data_dir_override: str | None):
    del data_dir_override
    yield _FakeRuntime()


@contextmanager
def _fake_runtime_context_with_run_redaction_failure(
    *, data_dir_override: str | None
):
    del data_dir_override

    class _Gateway:
        @staticmethod
        def _sanitize_tool_result(payload: dict[str, Any]) -> dict[str, Any]:
            if "payload" in payload:
                return {
                    "type": "gateway_error",
                    "code": "INTERNAL",
                    "message": "response redaction failed",
                    "details": {},
                }
            return payload

    class _RuntimeWithGateway:
        gateway = _Gateway()

    yield _RuntimeWithGateway()


def test_serve_code_passes_inline_code_and_params(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "sift_gateway.cli_main._runtime_context",
        _fake_runtime_context,
    )
    captured_args: dict[str, Any] = {}

    def _fake_code(runtime: Any, arguments: dict[str, Any]) -> dict[str, Any]:
        del runtime
        captured_args.update(arguments)
        return {"items": [{"x": 1}], "truncated": False}

    monkeypatch.setattr(
        "sift_gateway.cli_main.execute_artifact_code", _fake_code
    )

    exit_code = cli_main.serve(
        [
            "code",
            "art_1",
            "$.items",
            "--code",
            "def run(data, schema, params):\n    return data",
            "--params",
            '{"limit": 5}',
            "--json",
        ]
    )

    assert exit_code == 0
    assert captured_args["_gateway_context"]["session_id"] == "cli"
    assert captured_args["artifact_id"] == "art_1"
    assert captured_args["root_path"] == "$.items"
    assert captured_args["params"] == {"limit": 5}
    assert "def run" in captured_args["code"]


def test_serve_code_loads_file(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(
        "sift_gateway.cli_main._runtime_context",
        _fake_runtime_context,
    )
    code_file = tmp_path / "analysis.py"
    code_file.write_text(
        "def run(data, schema, params):\n    return {'rows': len(data)}\n",
        encoding="utf-8",
    )
    captured_args: dict[str, Any] = {}

    def _fake_code(runtime: Any, arguments: dict[str, Any]) -> dict[str, Any]:
        del runtime
        captured_args.update(arguments)
        return {"items": [], "truncated": False}

    monkeypatch.setattr(
        "sift_gateway.cli_main.execute_artifact_code", _fake_code
    )

    exit_code = cli_main.serve(
        [
            "code",
            "art_1",
            "$.items",
            "--file",
            str(code_file),
            "--json",
        ]
    )

    assert exit_code == 0
    assert "rows" in captured_args["code"]


def test_serve_code_supports_multi_artifact_shared_root_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "sift_gateway.cli_main._runtime_context",
        _fake_runtime_context,
    )
    captured_args: dict[str, Any] = {}

    def _fake_code(runtime: Any, arguments: dict[str, Any]) -> dict[str, Any]:
        del runtime
        captured_args.update(arguments)
        return {"items": [], "truncated": False}

    monkeypatch.setattr(
        "sift_gateway.cli_main.execute_artifact_code", _fake_code
    )

    exit_code = cli_main.serve(
        [
            "code",
            "--artifact-id",
            "art_users",
            "--artifact-id",
            "art_orders",
            "--root-path",
            "$.items",
            "--expr",
            "len(df)",
            "--json",
        ]
    )

    assert exit_code == 0
    assert captured_args["artifact_ids"] == ["art_users", "art_orders"]
    assert captured_args["root_paths"] == {
        "art_users": "$.items",
        "art_orders": "$.items",
    }
    assert "artifact_id" not in captured_args
    assert "def run(artifacts, schemas, params):" in captured_args["code"]


def test_serve_code_supports_multi_artifact_per_artifact_root_paths(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "sift_gateway.cli_main._runtime_context",
        _fake_runtime_context,
    )
    captured_args: dict[str, Any] = {}

    def _fake_code(runtime: Any, arguments: dict[str, Any]) -> dict[str, Any]:
        del runtime
        captured_args.update(arguments)
        return {"items": [], "truncated": False}

    monkeypatch.setattr(
        "sift_gateway.cli_main.execute_artifact_code", _fake_code
    )

    exit_code = cli_main.serve(
        [
            "code",
            "--artifact-id",
            "art_users",
            "--artifact-id",
            "art_orders",
            "--root-path",
            "$.users",
            "--root-path",
            "$.orders",
            "--expr",
            "len(df)",
            "--json",
        ]
    )

    assert exit_code == 0
    assert captured_args["artifact_ids"] == ["art_users", "art_orders"]
    assert captured_args["root_paths"] == {
        "art_users": "$.users",
        "art_orders": "$.orders",
    }


def test_serve_code_expr_builds_dataframe_wrapper(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "sift_gateway.cli_main._runtime_context",
        _fake_runtime_context,
    )
    captured_args: dict[str, Any] = {}

    def _fake_code(runtime: Any, arguments: dict[str, Any]) -> dict[str, Any]:
        del runtime
        captured_args.update(arguments)
        return {"items": [1], "truncated": False}

    monkeypatch.setattr(
        "sift_gateway.cli_main.execute_artifact_code", _fake_code
    )

    exit_code = cli_main.serve(
        [
            "code",
            "art_1",
            "$.items",
            "--expr",
            "df['value'].sum()",
            "--json",
        ]
    )

    assert exit_code == 0
    assert "import pandas as pd" in captured_args["code"]
    assert "def run(artifacts, schemas, params):" in captured_args["code"]
    assert "artifact_frames" in captured_args["code"]
    assert "return df['value'].sum()" in captured_args["code"]


def test_serve_code_rejects_invalid_params_json(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        "sift_gateway.cli_main._runtime_context",
        _fake_runtime_context,
    )

    exit_code = cli_main.serve(
        [
            "code",
            "art_1",
            "$.items",
            "--code",
            "def run(data, schema, params):\n    return data",
            "--params",
            "{bad",
        ]
    )
    err = capsys.readouterr().err

    assert exit_code == 1
    assert "invalid --params JSON" in err


def test_serve_code_scope_single_sets_scope_single(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "sift_gateway.cli_main._runtime_context",
        _fake_runtime_context,
    )
    captured_args: dict[str, Any] = {}

    def _fake_code(runtime: Any, arguments: dict[str, Any]) -> dict[str, Any]:
        del runtime
        captured_args.update(arguments)
        return {"items": [], "truncated": False}

    monkeypatch.setattr(
        "sift_gateway.cli_main.execute_artifact_code", _fake_code
    )

    exit_code = cli_main.serve(
        [
            "code",
            "art_1",
            "$.items",
            "--scope",
            "single",
            "--expr",
            "len(df)",
            "--json",
        ]
    )

    assert exit_code == 0
    assert captured_args["scope"] == "single"


def test_serve_code_rejects_mixed_positional_and_multi_flags(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        "sift_gateway.cli_main._runtime_context",
        _fake_runtime_context,
    )

    exit_code = cli_main.serve(
        [
            "code",
            "art_1",
            "$.items",
            "--artifact-id",
            "art_2",
            "--root-path",
            "$.items",
            "--expr",
            "len(df)",
        ]
    )
    err = capsys.readouterr().err

    assert exit_code == 1
    assert "cannot mix positional artifact_id/root_path" in err


def test_serve_code_rejects_mismatched_multi_root_path_count(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        "sift_gateway.cli_main._runtime_context",
        _fake_runtime_context,
    )

    exit_code = cli_main.serve(
        [
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
        ]
    )
    err = capsys.readouterr().err

    assert exit_code == 1
    assert "provide one --root-path or repeat --root-path" in err


def test_serve_code_rejects_partial_positional_mode(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        "sift_gateway.cli_main._runtime_context",
        _fake_runtime_context,
    )

    exit_code = cli_main.serve(
        [
            "code",
            "art_1",
            "--expr",
            "len(df)",
        ]
    )
    err = capsys.readouterr().err

    assert exit_code == 1
    assert "requires both artifact_id and root_path" in err


def test_serve_run_human_output_snapshot(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        "sift_gateway.cli_main._runtime_context",
        _fake_runtime_context,
    )
    monkeypatch.setattr(
        "sift_gateway.cli_main.subprocess.run",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            args=["echo", "hello"],
            returncode=0,
            stdout=b"hello\n",
            stderr=b"",
        ),
    )
    monkeypatch.setattr(
        "sift_gateway.cli_main.execute_artifact_capture",
        lambda runtime, *, arguments: {
            "artifact_id": "art_new",
            "created_seq": 4,
            "status": "ok",
            "kind": "data",
            "capture_kind": "cli_command",
            "capture_key": str(arguments["capture_key"]),
            "payload_json_bytes": 12,
            "payload_binary_bytes_total": 0,
            "payload_total_bytes": 12,
            "expires_at": None,
            "reused": False,
        },
    )

    exit_code = cli_main.serve(["run", "--", "echo", "hello"])
    out = capsys.readouterr().out

    assert exit_code == 0
    assert (
        out == "artifact: art_new\n"
        "mode:     full\n"
        "records:  1\n"
        "bytes:    12\n"
        "capture:  cli_command\n"
        "exit:     0\n"
        "hint:     use `sift-gateway code art_new '$' --expr \"len(df)\"`\n"
    )


def test_extract_cli_flag_args_parses_common_patterns() -> None:
    parsed = cli_main._extract_cli_flag_args(
        [
            "gh",
            "api",
            "--limit=100",
            "--after",
            "CUR_1",
            "--verbose",
            "--no-cache",
        ]
    )

    assert parsed["limit"] == 100
    assert parsed["after"] == "CUR_1"
    assert parsed["verbose"] is True
    assert parsed["cache"] is False


def test_serve_run_human_output_omits_expanded_schema_details(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        "sift_gateway.cli_main._runtime_context",
        _fake_runtime_context,
    )
    monkeypatch.setattr(
        "sift_gateway.cli_main.subprocess.run",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            args=["echo", "hello"],
            returncode=0,
            stdout=b"hello\n",
            stderr=b"",
        ),
    )
    monkeypatch.setattr(
        "sift_gateway.cli_main.execute_artifact_capture",
        lambda runtime, *, arguments: {
            "artifact_id": "art_new",
            "created_seq": 4,
            "status": "ok",
            "kind": "data",
            "capture_kind": "cli_command",
            "capture_key": str(arguments["capture_key"]),
            "payload_json_bytes": 12,
            "payload_binary_bytes_total": 0,
            "payload_total_bytes": 12,
            "expires_at": None,
            "reused": False,
        },
    )
    monkeypatch.setattr(
        "sift_gateway.cli_main.execute_artifact_describe",
        lambda runtime, *, arguments: {
            "artifact_id": arguments["artifact_id"],
            "scope": "single",
            "schemas": [
                {
                    "v": "schema_v1",
                    "h": "sha256:test",
                    "rp": "$",
                    "m": "exact",
                    "cv": {"c": "complete", "or": 1},
                    "fd": {"oc": 1},
                    "f": [
                        {
                            "p": "$.email",
                            "t": ["string"],
                            "n": False,
                            "r": True,
                            "e": "joana@example.com",
                            "dv": ["joana@example.com", "other@example.com"],
                        }
                    ],
                    "d": {
                        "dh": "sha256:data",
                        "tv": "traversal_v1",
                        "bf": None,
                    },
                }
            ],
        },
    )

    exit_code = cli_main.serve(["run", "--", "echo", "hello"])
    out = capsys.readouterr().out

    assert exit_code == 0
    assert "schema_scope: single\n" not in out
    assert "schema_roots: 1\n" not in out
    assert "schema_fields: 1\n" not in out


def test_extract_cli_flag_args_preserves_leading_zero_tokens() -> None:
    parsed = cli_main._extract_cli_flag_args(
        [
            "fake-api",
            "--cursor",
            "000123",
            "--page",
            "1",
            "--limit",
            "100",
        ]
    )

    assert parsed["cursor"] == "000123"
    assert parsed["page"] == 1
    assert parsed["limit"] == 100


def test_serve_run_injects_pagination_state_into_capture_meta(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        "sift_gateway.cli_main._runtime_context",
        _fake_runtime_context,
    )
    monkeypatch.setattr(
        "sift_gateway.cli_main.subprocess.run",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            args=["fake"],
            returncode=0,
            stdout=(
                b'{"next":"?after=CURSOR_2&limit=100",'
                b'"items":[{"id":"1"}]}'
            ),
            stderr=b"",
        ),
    )
    captured: dict[str, Any] = {}

    def _fake_capture(
        runtime: Any, *, arguments: dict[str, Any]
    ) -> dict[str, Any]:
        del runtime
        captured.update(arguments)
        return {
            "artifact_id": "art_page_1",
            "created_seq": 10,
            "status": "ok",
            "kind": "data",
            "capture_kind": "cli_command",
            "capture_key": "rk_3",
            "payload_json_bytes": 64,
            "payload_binary_bytes_total": 0,
            "payload_total_bytes": 64,
            "expires_at": None,
            "reused": False,
        }

    monkeypatch.setattr(
        "sift_gateway.cli_main.execute_artifact_capture",
        _fake_capture,
    )

    exit_code = cli_main.serve(
        [
            "run",
            "--json",
            "--",
            "fake-api",
            "--after",
            "CURSOR_1",
            "--limit",
            "100",
        ]
    )
    out = capsys.readouterr().out.strip()
    payload = json.loads(out)

    assert exit_code == 0
    assert payload["pagination"]["has_next_page"] is True
    assert payload["pagination"]["next_action"] == {
        "command": "run",
        "continue_from_artifact_id": "art_page_1",
        "command_line": "sift-gateway run --continue-from art_page_1 -- <next-command>",
    }
    meta = captured["meta"]
    assert meta["capture_mode"] == "command"
    pagination = meta["_gateway_pagination"]
    assert pagination["upstream_prefix"] == "cli"
    assert pagination["tool_name"] == "run"
    assert pagination["page_number"] == 0
    assert pagination["next_params"] == {"after": "CURSOR_2", "limit": 100}


def test_serve_run_derives_numeric_progression_from_integer_flags(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        "sift_gateway.cli_main._runtime_context",
        _fake_runtime_context,
    )
    monkeypatch.setattr(
        "sift_gateway.cli_main.subprocess.run",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            args=["fake"],
            returncode=0,
            stdout=b'{"has_more": true, "items": [{"id":"1"}]}',
            stderr=b"",
        ),
    )
    captured: dict[str, Any] = {}

    def _fake_capture(
        runtime: Any, *, arguments: dict[str, Any]
    ) -> dict[str, Any]:
        del runtime
        captured.update(arguments)
        return {
            "artifact_id": "art_page_1",
            "created_seq": 10,
            "status": "ok",
            "kind": "data",
            "capture_kind": "cli_command",
            "capture_key": "rk_3",
            "payload_json_bytes": 64,
            "payload_binary_bytes_total": 0,
            "payload_total_bytes": 64,
            "expires_at": None,
            "reused": False,
        }

    monkeypatch.setattr(
        "sift_gateway.cli_main.execute_artifact_capture",
        _fake_capture,
    )

    exit_code = cli_main.serve(
        [
            "run",
            "--json",
            "--",
            "fake-api",
            "--page",
            "1",
            "--limit",
            "100",
        ]
    )
    out = capsys.readouterr().out.strip()
    payload = json.loads(out)

    assert exit_code == 0
    assert payload["pagination"]["has_next_page"] is True
    assert payload["pagination"]["next_params"] == {"page": 2}
    pagination = captured["meta"]["_gateway_pagination"]
    assert pagination["next_params"] == {"page": 2}


def test_serve_run_persists_and_returns_command_exit_code(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        "sift_gateway.cli_main._runtime_context",
        _fake_runtime_context,
    )
    monkeypatch.setattr(
        "sift_gateway.cli_main.subprocess.run",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            args=["fake"],
            returncode=3,
            stdout=b'{"items":[1,2]}',
            stderr=b"",
        ),
    )
    captured: dict[str, Any] = {}

    def _fake_capture(
        runtime: Any, *, arguments: dict[str, Any]
    ) -> dict[str, Any]:
        del runtime
        captured.update(arguments)
        return {
            "artifact_id": "art_new",
            "created_seq": 8,
            "status": "error",
            "kind": "data",
            "capture_kind": "cli_command",
            "capture_key": "rk_1",
            "payload_json_bytes": 25,
            "payload_binary_bytes_total": 0,
            "payload_total_bytes": 25,
            "expires_at": "2026-02-20T00:00:00Z",
            "reused": False,
        }

    monkeypatch.setattr(
        "sift_gateway.cli_main.execute_artifact_capture", _fake_capture
    )

    exit_code = cli_main.serve(["run", "--json", "--", "fake-command"])
    out = capsys.readouterr().out.strip()
    payload = json.loads(out)

    assert exit_code == 3
    assert payload["artifact_id"] == "art_new"
    assert payload["command_exit_code"] == 3
    assert captured["capture_kind"] == "cli_command"
    assert captured["tool_name"] == "run"


def test_serve_run_legacy_capture_error_returns_nonzero(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        "sift_gateway.cli_main._runtime_context",
        _fake_runtime_context,
    )
    monkeypatch.setattr(
        "sift_gateway.cli_main.subprocess.run",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            args=["fake"],
            returncode=0,
            stdout=b'{"items":[1]}',
            stderr=b"",
        ),
    )
    monkeypatch.setattr(
        "sift_gateway.cli_main.execute_artifact_capture",
        lambda runtime, *, arguments: {
            "code": "CAPTURE_PERSISTENCE_FAILED",
            "message": "artifact persistence failed",
            "details": {"stage": "persist_artifact"},
        },
    )

    exit_code = cli_main.serve(["run", "--json", "--", "fake-command"])
    out = capsys.readouterr().out.strip()
    payload = json.loads(out)

    assert exit_code == 1
    assert payload["code"] == "CAPTURE_PERSISTENCE_FAILED"
    assert payload["message"] == "artifact persistence failed"


def test_serve_run_fails_closed_on_redaction_error_before_capture(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        "sift_gateway.cli_main._runtime_context",
        _fake_runtime_context_with_run_redaction_failure,
    )
    monkeypatch.setattr(
        "sift_gateway.cli_main.subprocess.run",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            args=["fake"],
            returncode=0,
            stdout=b'{"items":[1]}',
            stderr=b"",
        ),
    )
    capture_called = {"value": False}

    def _capture_marker(runtime: Any, *, arguments: dict[str, Any]) -> dict[str, Any]:
        del runtime, arguments
        capture_called["value"] = True
        return {"artifact_id": "art_should_not_exist"}

    monkeypatch.setattr(
        "sift_gateway.cli_main.execute_artifact_capture",
        _capture_marker,
    )

    exit_code = cli_main.serve(["run", "--json", "--", "fake-command"])
    out = capsys.readouterr().out.strip()
    payload = json.loads(out)

    assert exit_code == 1
    assert payload["type"] == "gateway_error"
    assert payload["code"] == "INTERNAL"
    assert payload["message"] == "response redaction failed"
    assert capture_called["value"] is False


def test_serve_run_stdin_uses_stdin_capture_kind(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "sift_gateway.cli_main._runtime_context",
        _fake_runtime_context,
    )
    captured: dict[str, Any] = {}

    def _fake_capture(
        runtime: Any, *, arguments: dict[str, Any]
    ) -> dict[str, Any]:
        del runtime
        captured.update(arguments)
        return {
            "artifact_id": "art_stdin",
            "created_seq": 2,
            "status": "ok",
            "kind": "data",
            "capture_kind": "stdin_pipe",
            "capture_key": "rk_2",
            "payload_json_bytes": 10,
            "payload_binary_bytes_total": 0,
            "payload_total_bytes": 10,
            "expires_at": None,
            "reused": False,
        }

    class _FakeStdin:
        def __init__(self, raw: bytes) -> None:
            self.buffer = io.BytesIO(raw)

    monkeypatch.setattr(
        "sift_gateway.cli_main.execute_artifact_capture", _fake_capture
    )
    monkeypatch.setattr("sys.stdin", _FakeStdin(b'{"k": "v"}'))

    exit_code = cli_main.serve(["run", "--stdin", "--json"])

    assert exit_code == 0
    assert captured["capture_kind"] == "stdin_pipe"
    assert captured["tool_name"] == "stdin"
    assert captured["payload"] == {"k": "v"}


def test_serve_run_rejects_stdin_with_continue_from(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        "sift_gateway.cli_main._runtime_context",
        _fake_runtime_context,
    )

    exit_code = cli_main.serve(
        ["run", "--stdin", "--continue-from", "art_1", "--json"]
    )
    err = capsys.readouterr().err

    assert exit_code == 1
    assert "--stdin cannot be combined with --continue-from" in err


def test_serve_run_continue_from_links_lineage_and_page_number(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        "sift_gateway.cli_main._runtime_context",
        _fake_runtime_context,
    )
    monkeypatch.setattr(
        "sift_gateway.cli_main._load_cli_continue_chain_seq",
        lambda runtime, artifact_id: 1,
    )
    monkeypatch.setattr(
        "sift_gateway.cli_main.subprocess.run",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            args=["fake-api"],
            returncode=0,
            stdout=(
                b'{"next":"?after=CUR_3&limit=100",'
                b'"items":[{"id":"2"}]}'
            ),
            stderr=b"",
        ),
    )
    captured: dict[str, Any] = {}

    def _fake_capture(
        runtime: Any, *, arguments: dict[str, Any]
    ) -> dict[str, Any]:
        del runtime
        captured.update(arguments)
        return {
            "artifact_id": "art_page_2",
            "created_seq": 11,
            "status": "ok",
            "kind": "data",
            "capture_kind": "cli_command",
            "capture_key": "rk_next",
            "payload_json_bytes": 64,
            "payload_binary_bytes_total": 0,
            "payload_total_bytes": 64,
            "expires_at": None,
            "reused": False,
        }

    monkeypatch.setattr(
        "sift_gateway.cli_main.execute_artifact_capture",
        _fake_capture,
    )

    exit_code = cli_main.serve(
        [
            "run",
            "--continue-from",
            "art_page_1",
            "--json",
            "--tag",
            "page2",
            "--",
            "fake-api",
            "--after",
            "CUR_2",
            "--limit",
            "100",
        ]
    )
    out = capsys.readouterr().out.strip()
    payload = json.loads(out)

    assert exit_code == 0
    assert payload["artifact_id"] == "art_page_2"
    assert payload["source_artifact_id"] == "art_page_1"
    assert payload["pagination"]["has_next_page"] is True
    assert payload["pagination"]["next_action"] == {
        "command": "run",
        "continue_from_artifact_id": "art_page_2",
        "command_line": "sift-gateway run --continue-from art_page_2 -- <next-command>",
    }
    assert payload["pagination"]["page_number"] == 1
    assert captured["parent_artifact_id"] == "art_page_1"
    assert captured["chain_seq"] == 1
    assert captured["capture_origin"]["continue_from_artifact_id"] == "art_page_1"
    assert captured["capture_origin"]["command_argv"] == [
        "fake-api",
        "--after",
        "CUR_2",
        "--limit",
        "100",
    ]
    assert captured["meta"]["continue_from_artifact_id"] == "art_page_1"
    assert captured["meta"]["_gateway_pagination"]["page_number"] == 1
    assert captured["meta"]["_gateway_pagination"]["next_params"] == {
        "after": "CUR_3",
        "limit": 100,
    }


def test_serve_run_continue_from_returns_command_exit_code_in_json_mode(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        "sift_gateway.cli_main._runtime_context",
        _fake_runtime_context,
    )
    monkeypatch.setattr(
        "sift_gateway.cli_main._load_cli_continue_chain_seq",
        lambda runtime, artifact_id: 3,
    )
    monkeypatch.setattr(
        "sift_gateway.cli_main.subprocess.run",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            args=["fake-api"],
            returncode=5,
            stdout=b'{"items":[1]}',
            stderr=b"",
        ),
    )
    monkeypatch.setattr(
        "sift_gateway.cli_main.execute_artifact_capture",
        lambda runtime, *, arguments: {
            "artifact_id": "art_page_3",
            "created_seq": 12,
            "status": "error",
            "kind": "data",
            "capture_kind": "cli_command",
            "capture_key": "rk_next",
            "payload_json_bytes": 11,
            "payload_binary_bytes_total": 0,
            "payload_total_bytes": 11,
            "expires_at": None,
            "reused": False,
        },
    )

    exit_code = cli_main.serve(
        [
            "run",
            "--continue-from",
            "art_page_2",
            "--json",
            "--",
            "fake-api",
            "--after",
            "CUR_3",
        ]
    )
    out = capsys.readouterr().out.strip()
    payload = json.loads(out)

    assert payload["command_exit_code"] == 5
    assert payload["source_artifact_id"] == "art_page_2"
    assert exit_code == 5
