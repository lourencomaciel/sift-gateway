"""Unit tests for subprocess code-query runtime."""

from __future__ import annotations

import pytest

from sift_gateway.codegen.runtime import (
    CodeRuntimeConfig,
    CodeRuntimeError,
    CodeRuntimeTimeoutError,
    _build_env,
    execute_code_in_subprocess,
)


def test_execute_code_in_subprocess_returns_result() -> None:
    result = execute_code_in_subprocess(
        code="""
def run(data, schema, params):
    return [{"count": len(data), "root": schema.get("root_path")}]
""",
        data=[{"id": 1}, {"id": 2}],
        schema={"root_path": "$.items"},
        params={},
        runtime=CodeRuntimeConfig(timeout_seconds=2.0, max_memory_mb=256),
    )
    assert result == [{"count": 2, "root": "$.items"}]


def test_execute_code_in_subprocess_raises_validation_error() -> None:
    with pytest.raises(CodeRuntimeError) as exc:
        execute_code_in_subprocess(
            code="""
import os

def run(data, schema, params):
    return []
""",
            data=[],
            schema={"root_path": "$.items"},
            params={},
            runtime=CodeRuntimeConfig(
                timeout_seconds=2.0,
                max_memory_mb=256,
            ),
        )
    assert exc.value.code == "CODE_IMPORT_NOT_ALLOWED"
    assert exc.value.traceback is None


def test_execute_code_in_subprocess_timeout() -> None:
    with pytest.raises(CodeRuntimeTimeoutError):
        execute_code_in_subprocess(
            code="""
def run(data, schema, params):
    while True:
        pass
""",
            data=[],
            schema={"root_path": "$.items"},
            params={},
            runtime=CodeRuntimeConfig(
                timeout_seconds=0.1,
                max_memory_mb=256,
            ),
        )


def test_execute_code_in_subprocess_allows_analytics_imports() -> None:
    result = execute_code_in_subprocess(
        code="""
import numpy as np
import pandas as pd

def run(data, schema, params):
    arr = np.array([1, 2, 3])
    frame = pd.DataFrame({"v": [10, 20]})
    return {"sum": int(arr.sum()), "rows": int(frame.shape[0])}
""",
        data=[],
        schema={"root_path": "$.items"},
        params={},
        runtime=CodeRuntimeConfig(timeout_seconds=10.0, max_memory_mb=512),
    )
    assert result == {"sum": 6, "rows": 2}


def test_execute_code_in_subprocess_normalizes_numpy_scalars() -> None:
    result = execute_code_in_subprocess(
        code="""
import numpy as np

def run(data, schema, params):
    return {"spend": np.float64(12.5), "count": np.int64(3)}
""",
        data=[],
        schema={"root_path": "$.items"},
        params={},
        runtime=CodeRuntimeConfig(timeout_seconds=5.0, max_memory_mb=512),
    )
    assert result == {"spend": 12.5, "count": 3}


def test_execute_code_in_subprocess_blocks_analytics_when_not_allowed() -> None:
    with pytest.raises(CodeRuntimeError) as exc:
        execute_code_in_subprocess(
            code="""
import numpy as np

def run(data, schema, params):
    return int(np.array([1, 2, 3]).sum())
""",
            data=[],
            schema={"root_path": "$.items"},
            params={},
            runtime=CodeRuntimeConfig(timeout_seconds=3.0, max_memory_mb=256),
            allowed_import_roots=["math", "json", "jmespath"],
        )
    assert exc.value.code == "CODE_IMPORT_NOT_ALLOWED"
    assert exc.value.traceback is None


def test_execute_code_in_subprocess_supports_multi_artifact_signature() -> None:
    result = execute_code_in_subprocess(
        code="""
def run(artifacts, schemas, params):
    return [{"records": len(artifacts["a"]) + len(artifacts["b"])}]
""",
        artifacts={"a": [{"id": 1}], "b": [{"id": 2}, {"id": 3}]},
        schemas={"a": {"root_path": "$.a"}, "b": {"root_path": "$.b"}},
        params={},
        runtime=CodeRuntimeConfig(timeout_seconds=2.0, max_memory_mb=256),
    )
    assert result == [{"records": 3}]


def test_execute_code_in_subprocess_multi_signature_accepts_single_input() -> (
    None
):
    result = execute_code_in_subprocess(
        code="""
def run(artifacts, schemas, params):
    return {"records": len(artifacts["__single__"])}
""",
        data=[{"id": 1}, {"id": 2}],
        schema={"root_path": "$.items"},
        params={},
        runtime=CodeRuntimeConfig(timeout_seconds=2.0, max_memory_mb=256),
    )
    assert result == {"records": 2}


def test_execute_code_in_subprocess_runtime_error_includes_traceback() -> None:
    with pytest.raises(CodeRuntimeError) as exc:
        execute_code_in_subprocess(
            code="""
def run(data, schema, params):
    payload = {"ok": 1}
    return payload["missing"]
""",
            data=[{"id": 1}],
            schema={"root_path": "$.items"},
            params={},
            runtime=CodeRuntimeConfig(timeout_seconds=2.0, max_memory_mb=256),
        )
    assert exc.value.code == "CODE_RUNTIME_EXCEPTION"
    assert exc.value.traceback is not None
    assert "KeyError" in exc.value.traceback
    assert "<generated_code>" in exc.value.traceback


def test_execute_code_in_subprocess_runtime_traceback_is_truncated() -> None:
    with pytest.raises(CodeRuntimeError) as exc:
        execute_code_in_subprocess(
            code="""
def recurse(n):
    if n == 0:
        raise RuntimeError("x" * 5000)
    return recurse(n - 1)

def run(data, schema, params):
    return recurse(40)
""",
            data=[],
            schema={"root_path": "$.items"},
            params={},
            runtime=CodeRuntimeConfig(timeout_seconds=2.0, max_memory_mb=256),
        )
    assert exc.value.code == "CODE_RUNTIME_EXCEPTION"
    assert exc.value.traceback is not None
    assert len(exc.value.traceback) <= 2000


def test_execute_code_in_subprocess_strips_locator_from_dicts() -> None:
    result = execute_code_in_subprocess(
        code="""
def run(data, schema, params):
    return [{"keys": sorted(r.keys())} for r in data]
""",
        data=[
            {"mag": 4.5, "_locator": {"artifact_id": "a1"}},
            {"depth": 10, "_locator": {"artifact_id": "a2"}},
        ],
        schema={"root_path": "$.items"},
        params={},
        runtime=CodeRuntimeConfig(timeout_seconds=2.0, max_memory_mb=256),
    )
    assert result == [{"keys": ["mag"]}, {"keys": ["depth"]}]


def test_execute_code_in_subprocess_unwraps_scalar_records() -> None:
    result = execute_code_in_subprocess(
        code="""
def run(data, schema, params):
    return {"sum": sum(data), "all_float": all(isinstance(v, float) for v in data)}
""",
        data=[
            {"_locator": {"artifact_id": "a1", "_scalar": True}, "value": 1.5},
            {"_locator": {"artifact_id": "a2", "_scalar": True}, "value": 2.5},
        ],
        schema={"root_path": "$.hourly.temperature_2m"},
        params={},
        runtime=CodeRuntimeConfig(timeout_seconds=2.0, max_memory_mb=256),
    )
    assert result == {"sum": 4.0, "all_float": True}


def test_execute_code_in_subprocess_strips_locators_multi_artifact() -> None:
    result = execute_code_in_subprocess(
        code="""
def run(artifacts, schemas, params):
    temps = artifacts["temps"]
    quakes = artifacts["quakes"]
    return {
        "temp_sum": sum(temps),
        "quake_keys": [sorted(r.keys()) for r in quakes],
    }
""",
        artifacts={
            "temps": [
                {
                    "_locator": {"artifact_id": "t1", "_scalar": True},
                    "value": 1.0,
                },
                {
                    "_locator": {"artifact_id": "t2", "_scalar": True},
                    "value": 2.0,
                },
            ],
            "quakes": [
                {"mag": 4.5, "_locator": {"artifact_id": "q1"}},
                {"mag": 5.0, "_locator": {"artifact_id": "q2"}},
            ],
        },
        schemas={
            "temps": {"root_path": "$.hourly.temperature_2m"},
            "quakes": {"root_path": "$.features"},
        },
        params={},
        runtime=CodeRuntimeConfig(timeout_seconds=2.0, max_memory_mb=256),
    )
    assert result == {
        "temp_sum": 3.0,
        "quake_keys": [["mag"], ["mag"]],
    }


def test_build_env_filters_parent_environment(monkeypatch) -> None:
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "top-secret")
    monkeypatch.setenv("LANG", "en_US.UTF-8")
    monkeypatch.setenv("PYTHONPATH", "/tmp/src")

    env = _build_env()

    assert env["PYTHONHASHSEED"] == "0"
    assert env["TZ"] == "UTC"
    assert env.get("LANG") == "en_US.UTF-8"
    assert env.get("PYTHONPATH") == "/tmp/src"
    assert "AWS_SECRET_ACCESS_KEY" not in env
