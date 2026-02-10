from __future__ import annotations

from mcp_artifact_gateway.cursor.sample_set_hash import (
    SampleSetHashBindingError,
    assert_sample_set_hash_binding,
)


def test_cursor_sample_set_hash_binding_ok() -> None:
    payload = {"sample_set_hash": "abc"}
    assert_sample_set_hash_binding(payload, "abc")


def test_cursor_sample_set_hash_binding_rejects_mismatch() -> None:
    payload = {"sample_set_hash": "abc"}
    try:
        assert_sample_set_hash_binding(payload, "def")
    except SampleSetHashBindingError as exc:
        assert "mismatch" in str(exc)
    else:
        raise AssertionError("expected SampleSetHashBindingError")
