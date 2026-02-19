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


def test_serve_list_json_uses_search_and_prints_json(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        "sift_gateway.cli_main._runtime_context",
        _fake_runtime_context,
    )

    captured_args: dict[str, Any] = {}

    def _fake_search(
        runtime: Any, *, arguments: dict[str, Any]
    ) -> dict[str, Any]:
        del runtime
        captured_args.update(arguments)
        return {
            "items": [
                {
                    "artifact_id": "art_1",
                    "created_seq": 1,
                    "source_tool": "demo.echo",
                    "capture_kind": "mcp_tool",
                    "kind": "data",
                    "status": "ok",
                    "payload_total_bytes": 10,
                }
            ],
            "truncated": False,
            "cursor": None,
        }

    monkeypatch.setattr(
        "sift_gateway.cli_main.execute_artifact_search", _fake_search
    )

    exit_code = cli_main.serve(["list", "--json", "--limit", "12"])
    out = capsys.readouterr().out.strip()
    payload = json.loads(out)

    assert exit_code == 0
    assert payload["items"][0]["artifact_id"] == "art_1"
    assert captured_args["limit"] == 12
    assert captured_args["_gateway_context"]["session_id"] == "cli"


def test_serve_query_passes_where_object_to_select(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "sift_gateway.cli_main._runtime_context",
        _fake_runtime_context,
    )

    captured_args: dict[str, Any] = {}

    def _fake_select(runtime: Any, arguments: dict[str, Any]) -> dict[str, Any]:
        del runtime
        captured_args.update(arguments)
        return {"items": [], "truncated": False, "cursor": None}

    monkeypatch.setattr(
        "sift_gateway.cli_main.execute_artifact_select", _fake_select
    )

    exit_code = cli_main.serve(
        [
            "query",
            "art_1",
            "$.items",
            "--select",
            "id,title",
            "--where",
            '{"path":"$.state","op":"eq","value":"open"}',
            "--json",
        ]
    )

    assert exit_code == 0
    assert captured_args["artifact_id"] == "art_1"
    assert captured_args["root_path"] == "$.items"
    assert captured_args["select_paths"] == ["id", "title"]
    assert captured_args["where"] == {
        "path": "$.state",
        "op": "eq",
        "value": "open",
    }


def test_serve_query_rejects_invalid_where_json(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        "sift_gateway.cli_main._runtime_context",
        _fake_runtime_context,
    )

    exit_code = cli_main.serve(
        ["query", "art_1", "$.items", "--where", "{not-json"]
    )
    err = capsys.readouterr().err

    assert exit_code == 1
    assert "invalid --where JSON" in err


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


def test_serve_non_json_list_emits_compact_lines(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        "sift_gateway.cli_main._runtime_context",
        _fake_runtime_context,
    )

    def _fake_search(
        runtime: Any, *, arguments: dict[str, Any]
    ) -> dict[str, Any]:
        del runtime, arguments
        return {
            "items": [
                {
                    "artifact_id": "art_1",
                    "created_seq": 1,
                    "source_tool": "demo.echo",
                    "capture_kind": "mcp_tool",
                    "kind": "data",
                    "status": "ok",
                    "payload_total_bytes": 10,
                }
            ],
            "truncated": True,
            "cursor": "cur_next",
        }

    monkeypatch.setattr(
        "sift_gateway.cli_main.execute_artifact_search", _fake_search
    )

    exit_code = cli_main.serve(["list"])
    out = capsys.readouterr().out

    assert exit_code == 0
    assert "art_1" in out
    assert "next_cursor: cur_next" in out


def test_serve_non_json_list_output_snapshot(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        "sift_gateway.cli_main._runtime_context",
        _fake_runtime_context,
    )

    def _fake_search(
        runtime: Any, *, arguments: dict[str, Any]
    ) -> dict[str, Any]:
        del runtime, arguments
        return {
            "items": [
                {
                    "artifact_id": "art_1",
                    "created_seq": 1,
                    "source_tool": "demo.echo",
                    "capture_kind": "mcp_tool",
                    "kind": "data",
                    "status": "ok",
                    "payload_total_bytes": 10,
                }
            ],
            "truncated": True,
            "cursor": "cur_next",
        }

    monkeypatch.setattr(
        "sift_gateway.cli_main.execute_artifact_search", _fake_search
    )

    exit_code = cli_main.serve(["list"])
    out = capsys.readouterr().out

    assert exit_code == 0
    assert (
        out
        == "art_1 seq=1 kind=data status=ok source=demo.echo capture=mcp_tool bytes=10\n"
        "next_cursor: cur_next\n"
    )


def test_serve_schema_human_output_snapshot(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        "sift_gateway.cli_main._runtime_context",
        _fake_runtime_context,
    )

    def _fake_schema(runtime: Any, arguments: dict[str, Any]) -> dict[str, Any]:
        del runtime, arguments
        return {
            "artifact_id": "art_1",
            "scope": "single",
            "artifacts": [{"artifact_id": "art_1"}],
            "roots": [{"root_path": "$.items", "count_estimate": 3}],
        }

    monkeypatch.setattr(
        "sift_gateway.cli_main.execute_artifact_describe", _fake_schema
    )

    exit_code = cli_main.serve(["schema", "art_1", "--scope", "single"])
    out = capsys.readouterr().out

    assert exit_code == 0
    assert (
        out == "artifact: art_1\n"
        "scope: single\n"
        "artifacts: 1\n"
        "roots: 1\n"
        "- $.items count=3\n"
    )


def test_serve_returns_error_exit_for_gateway_error_payload(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        "sift_gateway.cli_main._runtime_context",
        _fake_runtime_context,
    )

    def _fake_get(runtime: Any, *, arguments: dict[str, Any]) -> dict[str, Any]:
        del runtime, arguments
        return {"code": "NOT_FOUND", "message": "artifact not found"}

    monkeypatch.setattr("sift_gateway.cli_main.execute_artifact_get", _fake_get)

    exit_code = cli_main.serve(["get", "art_missing"])
    err = capsys.readouterr().err

    assert exit_code == 1
    assert "NOT_FOUND: artifact not found" in err


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
        "records:  1\n"
        "bytes:    12\n"
        "capture:  cli_command\n"
        "exit:     0\n"
        "hint:     use `sift-gateway query art_new '$'` to explore\n"
    )


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


def test_serve_diff_json_reports_hash_equality(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        "sift_gateway.cli_main._runtime_context",
        _fake_runtime_context,
    )
    monkeypatch.setattr(
        "sift_gateway.cli_main._fetch_artifact_for_diff",
        lambda runtime, artifact_id: {
            "artifact_id": artifact_id,
            "payload_hash_full": "same",
            "payload_total_bytes": 12,
            "envelope": {"x": 1},
        },
    )

    exit_code = cli_main.serve(["diff", "art_a", "art_b", "--json"])
    out = capsys.readouterr().out.strip()
    payload = json.loads(out)

    assert exit_code == 0
    assert payload["equal"] is True
    assert payload["left_artifact_id"] == "art_a"
    assert payload["right_artifact_id"] == "art_b"


def test_serve_diff_human_output_snapshot(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        "sift_gateway.cli_main._runtime_context",
        _fake_runtime_context,
    )
    monkeypatch.setattr(
        "sift_gateway.cli_main._fetch_artifact_for_diff",
        lambda runtime, artifact_id: {
            "artifact_id": artifact_id,
            "payload_hash_full": "same",
            "payload_total_bytes": 12,
            "envelope": {"x": 1},
        },
    )

    exit_code = cli_main.serve(["diff", "art_a", "art_b"])
    out = capsys.readouterr().out

    assert exit_code == 0
    assert (
        out == "left:    art_a\n"
        "right:   art_b\n"
        "equal:   True\n"
        "hashes:  same / same\n"
        "bytes:   12 / 12\n"
    )
