from __future__ import annotations

from typing import Any

from sift_gateway.core.artifact_code_parse import (
    _parse_code_args,
    _parse_steps,
)


def _base_args(**overrides: Any) -> dict[str, Any]:
    args: dict[str, Any] = {
        "_gateway_context": {"session_id": "sess_1"},
        "artifact_id": "art_1",
        "root_path": "$.items",
    }
    args.update(overrides)
    return args


def test_parse_steps_valid() -> None:
    raw = [
        {"code": "def run(data, schema, params): return data"},
        {"code": "def run(data, schema, params): return len(data)"},
    ]
    steps, err = _parse_steps(raw)
    assert err is None
    assert steps is not None
    assert len(steps) == 2
    assert steps[0].code == raw[0]["code"]
    assert steps[0].params == {}
    assert steps[1].code == raw[1]["code"]


def test_parse_steps_with_params() -> None:
    raw = [
        {
            "code": "def run(data, schema, params): return data",
            "params": {"limit": 5},
        },
    ]
    steps, err = _parse_steps(raw)
    assert err is None
    assert steps is not None
    assert steps[0].params == {"limit": 5}


def test_parse_steps_missing_code() -> None:
    raw = [{"params": {"x": 1}}]
    steps, err = _parse_steps(raw)
    assert steps is None
    assert err is not None
    assert "steps[0] missing code" in err["message"]


def test_parse_steps_invalid_type() -> None:
    steps, err = _parse_steps("not a list")
    assert steps is None
    assert err is not None
    assert "steps must be an array" in err["message"]


def test_parse_steps_empty() -> None:
    steps, err = _parse_steps([])
    assert steps is None
    assert err is not None
    assert "steps cannot be empty" in err["message"]


def test_parse_no_code_no_steps() -> None:
    args = _base_args()
    # No code, no steps.
    parsed, err = _parse_code_args(args)
    assert parsed is None
    assert err is not None
    assert "missing code" in err["message"]


def test_parse_steps_overrides_code() -> None:
    args = _base_args(
        code="def run(data, schema, params): return 'ignored'",
        steps=[
            {"code": "def run(data, schema, params): return 'step0'"},
        ],
    )
    parsed, err = _parse_code_args(args)
    assert err is None
    assert parsed is not None
    # When steps present, top-level code is still set (for backward
    # compat) but steps take precedence at execution time.
    assert parsed.steps is not None
    assert len(parsed.steps) == 1
    assert parsed.steps[0].code == (
        "def run(data, schema, params): return 'step0'"
    )


def test_parse_steps_without_toplevel_code() -> None:
    args = _base_args(
        steps=[
            {"code": "def run(data, schema, params): return data"},
        ],
    )
    parsed, err = _parse_code_args(args)
    assert err is None
    assert parsed is not None
    # Code falls back to first step's code.
    assert parsed.code == ("def run(data, schema, params): return data")
    assert parsed.steps is not None


def test_parse_steps_invalid_params_type() -> None:
    raw = [{"code": "def run(data, schema, params): pass", "params": 42}]
    steps, err = _parse_steps(raw)
    assert steps is None
    assert err is not None
    assert "steps[0] params must be an object" in err["message"]


def test_parse_steps_non_object_entry() -> None:
    raw = ["not an object"]
    steps, err = _parse_steps(raw)
    assert steps is None
    assert err is not None
    assert "steps[0] must be an object" in err["message"]


def test_parse_steps_with_valid_name() -> None:
    raw = [
        {
            "code": "def run(data, schema, params): return data",
            "name": "scout",
        },
        {
            "code": "def run(data, schema, params): return len(data)",
            "name": "refine",
        },
    ]
    steps, err = _parse_steps(raw)
    assert err is None
    assert steps is not None
    assert steps[0].name == "scout"
    assert steps[1].name == "refine"


def test_parse_steps_name_is_optional() -> None:
    raw = [
        {"code": "def run(data, schema, params): return data"},
    ]
    steps, err = _parse_steps(raw)
    assert err is None
    assert steps is not None
    assert steps[0].name is None


def test_parse_steps_invalid_name_type() -> None:
    raw = [
        {
            "code": "def run(data, schema, params): return data",
            "name": 42,
        },
    ]
    steps, err = _parse_steps(raw)
    assert steps is None
    assert err is not None
    assert "steps[0] name must be a non-empty string" in err["message"]


def test_parse_steps_empty_name_rejected() -> None:
    raw = [
        {
            "code": "def run(data, schema, params): return data",
            "name": "  ",
        },
    ]
    steps, err = _parse_steps(raw)
    assert steps is None
    assert err is not None
    assert "steps[0] name must be a non-empty string" in err["message"]
