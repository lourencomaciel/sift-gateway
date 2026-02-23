from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

from sift_gateway.core.artifact_code import execute_artifact_code


class _FakeCursor:
    def __init__(
        self,
        *,
        one: tuple[Any, ...] | None = None,
        all_rows: list[tuple[Any, ...]] | None = None,
    ) -> None:
        self._one = one
        self._all_rows = all_rows or []

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._one

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self._all_rows


class _SeqConnection:
    def __init__(self, cursors: list[_FakeCursor]) -> None:
        self._cursors = cursors
        self.committed = False

    def execute(
        self,
        _sql: str,
        _params: tuple[Any, ...] | list[Any],
    ) -> _FakeCursor:
        if not self._cursors:
            return _FakeCursor()
        return self._cursors.pop(0)

    def commit(self) -> None:
        self.committed = True


class _ConnectionContext:
    def __init__(self, connection: _SeqConnection) -> None:
        self._connection = connection

    def __enter__(self) -> _SeqConnection:
        return self._connection

    def __exit__(self, *_args: object) -> bool:
        return False


class _SeqPool:
    def __init__(self, connection: _SeqConnection) -> None:
        self._connection = connection

    def connection(self) -> _ConnectionContext:
        return _ConnectionContext(self._connection)


_DERIVED_COUNTER = 0


@dataclass
class _Runtime:
    db_pool: _SeqPool | None
    code_query_max_input_records: int = 100_000
    code_query_max_input_bytes: int = 50_000_000
    code_query_max_steps: int = 5
    persisted_calls: list[dict[str, Any]] = field(
        default_factory=list,
    )
    metric_calls: list[tuple[str, int]] = field(
        default_factory=list,
    )

    @property
    def code_query_timeout_seconds(self) -> float:
        return 10.0

    @property
    def code_query_max_memory_mb(self) -> int:
        return 256

    @property
    def code_query_allowed_import_roots(self) -> list[str] | None:
        return ["json", "math"]

    @property
    def max_jsonpath_length(self) -> int:
        return 1024

    @property
    def max_path_segments(self) -> int:
        return 64

    @property
    def max_wildcard_expansion_total(self) -> int:
        return 1000

    @property
    def related_query_max_artifacts(self) -> int:
        return 10

    @property
    def max_bytes_out(self) -> int:
        return 1_000_000

    @property
    def blobs_payload_dir(self) -> Path:
        return Path(".")

    def bounded_limit(self, limit_value: Any) -> int:
        del limit_value
        return 50

    def verify_cursor_payload(
        self,
        *,
        token: str,
        tool: str,
        artifact_id: str,
    ) -> dict[str, Any]:
        del token, tool, artifact_id
        raise ValueError("invalid cursor")

    def cursor_position(self, payload: dict[str, Any]) -> dict[str, Any]:
        del payload
        raise NotImplementedError

    def issue_cursor(
        self,
        *,
        tool: str,
        artifact_id: str,
        position_state: dict[str, Any],
        extra: dict[str, Any] | None = None,
    ) -> str:
        del tool, artifact_id, position_state, extra
        return "cur_next"

    def cursor_error(self, token_error: Exception) -> dict[str, Any]:
        return {
            "code": "CURSOR_STALE",
            "message": str(token_error),
        }

    def assert_cursor_field(
        self,
        payload: dict[str, Any],
        *,
        field: str,
        expected: object,
    ) -> None:
        actual = payload.get(field)
        if actual != expected:
            raise ValueError(f"{field} mismatch")

    def artifact_visible(
        self,
        connection: Any,
        *,
        session_id: str,
        artifact_id: str,
    ) -> bool:
        del connection, session_id, artifact_id
        return True

    def safe_touch_for_retrieval(
        self,
        connection: Any,
        *,
        session_id: str,
        artifact_id: str,
    ) -> bool:
        del connection, session_id, artifact_id
        return True

    def safe_touch_for_retrieval_many(
        self,
        connection: Any,
        *,
        session_id: str,
        artifact_ids: list[str],
    ) -> bool:
        del connection, session_id, artifact_ids
        return True

    def safe_touch_for_search(
        self,
        connection: Any,
        *,
        session_id: str,
        artifact_ids: list[str],
    ) -> bool:
        del connection, session_id, artifact_ids
        return True

    def resolve_related_artifacts(
        self,
        connection: Any,
        *,
        session_id: str,
        anchor_artifact_id: str,
    ) -> list[dict[str, Any]]:
        del connection, session_id
        return [{"artifact_id": anchor_artifact_id, "generation": 1}]

    def compute_related_set_hash(
        self,
        artifacts: list[dict[str, Any]],
    ) -> str:
        del artifacts
        return "related_hash_1"

    def build_lineage_root_catalog(
        self,
        entries: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        del entries
        return []

    def check_sample_corruption(
        self,
        root_row: dict[str, Any],
        sample_rows: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        del root_row, sample_rows
        return None

    def increment_metric(self, attr: str, amount: int = 1) -> None:
        self.metric_calls.append((attr, amount))

    def observe_metric(self, attr: str, value: float) -> None:
        del attr, value

    def persist_code_derived(
        self,
        *,
        parent_artifact_ids: list[str],
        requested_root_paths: dict[str, str],
        root_path: str,
        code_hash: str,
        params_hash: str,
        result_items: list[Any],
    ) -> tuple[str | None, dict[str, Any] | None]:
        global _DERIVED_COUNTER
        _DERIVED_COUNTER += 1
        derived_id = f"art_derived_{_DERIVED_COUNTER}"
        self.persisted_calls.append(
            {
                "parent_artifact_ids": parent_artifact_ids,
                "requested_root_paths": requested_root_paths,
                "root_path": root_path,
                "code_hash": code_hash,
                "params_hash": params_hash,
                "result_items": result_items,
                "derived_artifact_id": derived_id,
            }
        )
        return derived_id, None

    def not_implemented(self, tool_name: str) -> dict[str, Any]:
        return {
            "code": "NOT_IMPLEMENTED",
            "message": f"{tool_name} unavailable",
        }


def _meta_row(
    artifact_id: str = "art_1",
) -> tuple[Any, ...]:
    return (artifact_id, "full", "ready", "off", None, 1, "mbf")


def _root_row(
    root_path: str = "$.items",
) -> tuple[Any, ...]:
    return (
        "rk_1",
        root_path,
        1,
        "array",
        {"id": {"number": 1}},
        None,
        {},
    )


def _schema_root_row() -> tuple[Any, ...]:
    return (
        "rk_1",
        "$.items",
        "schema_v1",
        "sha256:schema_items",
        "exact",
        "complete",
        1,
        "sha256:dataset_items",
        "traversal_v1",
        "mbf",
    )


def _schema_field_row() -> tuple[Any, ...]:
    return (
        "$.id",
        ["number"],
        False,
        True,
        1,
        "1",
        None,
        None,
    )


def _artifact_row(
    items: list[dict[str, Any]],
    artifact_id: str = "art_1",
) -> tuple[Any, ...]:
    return (
        artifact_id,
        f"hash_{artifact_id}",
        None,
        "full",
        "ready",
        1,
        0,
        "mbf",
        {"content": [{"type": "json", "value": {"items": items}}]},
        "none",
        None,
        False,
    )


def _step0_cursors() -> list[_FakeCursor]:
    """DB cursors for step 0 against art_1 root_path=$.items."""
    return [
        _FakeCursor(one=_meta_row()),
        _FakeCursor(one=_root_row()),
        _FakeCursor(one=_schema_root_row()),
        _FakeCursor(all_rows=[_schema_field_row()]),
        _FakeCursor(one=_artifact_row([{"id": 1}, {"id": 2}])),
    ]


def _step_n_cursors(
    derived_id: str,
    items: list[dict[str, Any]],
) -> list[_FakeCursor]:
    """DB cursors for step 1+ against a derived artifact at root $."""
    return [
        _FakeCursor(one=_meta_row(derived_id)),
        _FakeCursor(
            one=(
                "rk_d",
                "$",
                1,
                "array",
                {"id": {"number": 1}},
                None,
                {},
            )
        ),
        _FakeCursor(
            one=(
                "rk_d",
                "$",
                "schema_v1",
                "sha256:schema_d",
                "exact",
                "complete",
                1,
                "sha256:dataset_d",
                "traversal_v1",
                "mbf",
            )
        ),
        _FakeCursor(all_rows=[_schema_field_row()]),
        _FakeCursor(one=_artifact_row(items, artifact_id=derived_id)),
    ]


def test_pipeline_two_steps_chains_artifacts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    global _DERIVED_COUNTER
    _DERIVED_COUNTER = 0

    # Step 0: original data → [{"id":1},{"id":2}]
    # Step 1: derived data → aggregated result
    step0_cursors = _step0_cursors()
    step1_cursors = _step_n_cursors("art_derived_1", [{"id": 1}, {"id": 2}])
    conn = _SeqConnection(step0_cursors + step1_cursors)
    runtime = _Runtime(db_pool=_SeqPool(conn))

    exec_calls: list[dict[str, Any]] = []

    def _fake_exec(**kwargs: Any) -> Any:
        exec_calls.append(kwargs)
        return kwargs["data"]

    monkeypatch.setattr(
        "sift_gateway.core.artifact_code.execute_code_in_subprocess",
        _fake_exec,
    )

    result = execute_artifact_code(
        runtime,
        arguments={
            "_gateway_context": {"session_id": "sess_1"},
            "artifact_id": "art_1",
            "root_path": "$.items",
            "steps": [
                {"code": "def run(data, schema, params): return data"},
                {
                    "code": ("def run(data, schema, params): return len(data)"),
                },
            ],
        },
    )

    assert len(runtime.persisted_calls) == 2
    # Step 1 should target step 0's derived artifact.
    assert runtime.persisted_calls[1]["parent_artifact_ids"] == [
        "art_derived_1"
    ]
    assert "pipeline" in result.get("metadata", {})
    pipeline = result["metadata"]["pipeline"]
    assert pipeline["version"] == "pipeline_v1"
    assert pipeline["step_count"] == 2
    assert len(pipeline["step_hashes"]) == 2
    assert pipeline["intermediate_artifact_ids"] == ["art_derived_1"]


def test_pipeline_single_step_equivalent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    global _DERIVED_COUNTER
    _DERIVED_COUNTER = 100

    conn = _SeqConnection(_step0_cursors())
    runtime = _Runtime(db_pool=_SeqPool(conn))

    monkeypatch.setattr(
        "sift_gateway.core.artifact_code.execute_code_in_subprocess",
        lambda **kwargs: kwargs["data"],
    )

    result = execute_artifact_code(
        runtime,
        arguments={
            "_gateway_context": {"session_id": "sess_1"},
            "artifact_id": "art_1",
            "root_path": "$.items",
            "steps": [
                {"code": ("def run(data, schema, params): return data")},
            ],
        },
    )

    assert result["artifact_id"] == "art_derived_101"
    assert result["total_matched"] == 2
    pipeline = result["metadata"]["pipeline"]
    assert pipeline["step_count"] == 1
    assert pipeline["intermediate_artifact_ids"] == []


def test_pipeline_step_failure_includes_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    global _DERIVED_COUNTER
    _DERIVED_COUNTER = 200

    # Step 0 succeeds; step 1 will fail due to runtime error.
    from sift_gateway.codegen.runtime import CodeRuntimeError

    call_count = 0

    def _fail_on_second(**kwargs: Any) -> Any:
        nonlocal call_count
        call_count += 1
        if call_count == 2:
            raise CodeRuntimeError(
                code="CODE_RUNTIME_EXCEPTION",
                message="bad code",
            )
        return kwargs["data"]

    step0_cursors = _step0_cursors()
    step1_cursors = _step_n_cursors("art_derived_201", [{"id": 1}, {"id": 2}])
    conn = _SeqConnection(step0_cursors + step1_cursors)
    runtime = _Runtime(db_pool=_SeqPool(conn))

    monkeypatch.setattr(
        "sift_gateway.core.artifact_code.execute_code_in_subprocess",
        _fail_on_second,
    )

    result = execute_artifact_code(
        runtime,
        arguments={
            "_gateway_context": {"session_id": "sess_1"},
            "artifact_id": "art_1",
            "root_path": "$.items",
            "steps": [
                {"code": ("def run(data, schema, params): return data")},
                {"code": ("def run(data, schema, params): return bad")},
            ],
        },
    )

    assert result["code"] == "INVALID_ARGUMENT"
    details = result.get("details", {})
    assert details.get("step_index") == 1
    assert details.get("total_steps") == 2
    assert details.get("last_successful_artifact_id") == ("art_derived_201")


def test_pipeline_exceeds_max_steps_rejected() -> None:
    conn = _SeqConnection([])
    runtime = _Runtime(db_pool=_SeqPool(conn), code_query_max_steps=2)

    result = execute_artifact_code(
        runtime,
        arguments={
            "_gateway_context": {"session_id": "sess_1"},
            "artifact_id": "art_1",
            "root_path": "$.items",
            "steps": [
                {"code": "def run(data, schema, params): pass"},
                {"code": "def run(data, schema, params): pass"},
                {"code": "def run(data, schema, params): pass"},
            ],
        },
    )

    assert result["code"] == "INVALID_ARGUMENT"
    assert "3 steps" in result["message"]
    assert result["details"]["code"] == "PIPELINE_TOO_MANY_STEPS"


def test_pipeline_intermediate_artifacts_persisted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    global _DERIVED_COUNTER
    _DERIVED_COUNTER = 300

    step0_cursors = _step0_cursors()
    step1_cursors = _step_n_cursors("art_derived_301", [{"id": 1}, {"id": 2}])
    conn = _SeqConnection(step0_cursors + step1_cursors)
    runtime = _Runtime(db_pool=_SeqPool(conn))

    monkeypatch.setattr(
        "sift_gateway.core.artifact_code.execute_code_in_subprocess",
        lambda **kwargs: kwargs["data"],
    )

    execute_artifact_code(
        runtime,
        arguments={
            "_gateway_context": {"session_id": "sess_1"},
            "artifact_id": "art_1",
            "root_path": "$.items",
            "steps": [
                {"code": ("def run(data, schema, params): return data")},
                {"code": ("def run(data, schema, params): return data")},
            ],
        },
    )

    assert len(runtime.persisted_calls) == 2


def test_pipeline_metadata_has_step_hashes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    global _DERIVED_COUNTER
    _DERIVED_COUNTER = 400

    step0_cursors = _step0_cursors()
    step1_cursors = _step_n_cursors("art_derived_401", [{"id": 1}])
    conn = _SeqConnection(step0_cursors + step1_cursors)
    runtime = _Runtime(db_pool=_SeqPool(conn))

    monkeypatch.setattr(
        "sift_gateway.core.artifact_code.execute_code_in_subprocess",
        lambda **kwargs: kwargs["data"],
    )

    result = execute_artifact_code(
        runtime,
        arguments={
            "_gateway_context": {"session_id": "sess_1"},
            "artifact_id": "art_1",
            "root_path": "$.items",
            "steps": [
                {"code": ("def run(data, schema, params): return data")},
                {
                    "code": ("def run(data, schema, params): return data[0:1]"),
                    "params": {"n": 1},
                },
            ],
        },
    )

    pipeline = result["metadata"]["pipeline"]
    assert len(pipeline["step_hashes"]) == 2
    for sh in pipeline["step_hashes"]:
        assert "code_hash" in sh
        assert "params_hash" in sh
    # Different code → different hashes.
    assert (
        pipeline["step_hashes"][0]["code_hash"]
        != pipeline["step_hashes"][1]["code_hash"]
    )


def test_pipeline_backward_compat_no_steps(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = _SeqConnection(_step0_cursors())
    runtime = _Runtime(db_pool=_SeqPool(conn))

    monkeypatch.setattr(
        "sift_gateway.core.artifact_code.execute_code_in_subprocess",
        lambda **kwargs: kwargs["data"],
    )

    result = execute_artifact_code(
        runtime,
        arguments={
            "_gateway_context": {"session_id": "sess_1"},
            "artifact_id": "art_1",
            "root_path": "$.items",
            "code": ("def run(data, schema, params): return data"),
        },
    )

    # No pipeline metadata when steps not used.
    assert "pipeline" not in result.get("metadata", {})
    assert result["total_matched"] == 2


def test_pipeline_steps_overrides_code(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    global _DERIVED_COUNTER
    _DERIVED_COUNTER = 500

    conn = _SeqConnection(_step0_cursors())
    runtime = _Runtime(db_pool=_SeqPool(conn))

    exec_codes: list[str] = []

    def _capture_exec(**kwargs: Any) -> Any:
        exec_codes.append(kwargs["code"])
        return kwargs["data"]

    monkeypatch.setattr(
        "sift_gateway.core.artifact_code.execute_code_in_subprocess",
        _capture_exec,
    )

    result = execute_artifact_code(
        runtime,
        arguments={
            "_gateway_context": {"session_id": "sess_1"},
            "artifact_id": "art_1",
            "root_path": "$.items",
            "code": "def run(data, schema, params): return 'ignored'",
            "steps": [
                {"code": ("def run(data, schema, params): return data")},
            ],
        },
    )

    # Should have used the step code, not top-level code.
    assert len(exec_codes) == 1
    assert "return data" in exec_codes[0]
    assert "ignored" not in exec_codes[0]
    assert "pipeline" in result.get("metadata", {})
