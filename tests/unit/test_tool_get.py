"""Tests for artifact.get tool implementation."""

from __future__ import annotations

from mcp_artifact_gateway.tools.artifact_get import (
    check_get_preconditions,
    is_sampled_only,
    validate_get_args,
)


# ---- validate_get_args ----

def test_validate_get_args_requires_session_id() -> None:
    result = validate_get_args({})
    assert result is not None
    assert result["code"] == "INVALID_ARGUMENT"
    assert "session_id" in result["message"]


def test_validate_get_args_requires_artifact_id() -> None:
    result = validate_get_args(
        {"_gateway_context": {"session_id": "sess_1"}}
    )
    assert result is not None
    assert result["code"] == "INVALID_ARGUMENT"
    assert "artifact_id" in result["message"]


def test_validate_get_args_rejects_invalid_target() -> None:
    result = validate_get_args(
        {
            "_gateway_context": {"session_id": "sess_1"},
            "artifact_id": "art_1",
            "target": "invalid",
        }
    )
    assert result is not None
    assert result["code"] == "INVALID_ARGUMENT"
    assert "invalid target" in result["message"]


def test_validate_get_args_accepts_envelope_target() -> None:
    result = validate_get_args(
        {
            "_gateway_context": {"session_id": "sess_1"},
            "artifact_id": "art_1",
            "target": "envelope",
        }
    )
    assert result is None


def test_validate_get_args_accepts_mapped_target() -> None:
    result = validate_get_args(
        {
            "_gateway_context": {"session_id": "sess_1"},
            "artifact_id": "art_1",
            "target": "mapped",
        }
    )
    assert result is None


def test_validate_get_args_defaults_to_envelope_target() -> None:
    result = validate_get_args(
        {
            "_gateway_context": {"session_id": "sess_1"},
            "artifact_id": "art_1",
        }
    )
    assert result is None


# ---- check_get_preconditions ----

def test_check_get_preconditions_returns_not_found_for_none() -> None:
    result = check_get_preconditions(None, "envelope")
    assert result is not None
    assert result["code"] == "NOT_FOUND"


def test_check_get_preconditions_returns_gone_for_deleted() -> None:
    row = {"deleted_at": "2024-01-01T00:00:00Z"}
    result = check_get_preconditions(row, "envelope")
    assert result is not None
    assert result["code"] == "GONE"


def test_check_get_preconditions_requires_map_status_ready_for_mapped() -> None:
    row = {
        "deleted_at": None,
        "map_status": "pending",
        "map_kind": "full",
    }
    result = check_get_preconditions(row, "mapped")
    assert result is not None
    assert result["code"] == "INVALID_ARGUMENT"
    assert "map_status" in result["message"]


def test_check_get_preconditions_requires_valid_map_kind_for_mapped() -> None:
    row = {
        "deleted_at": None,
        "map_status": "ready",
        "map_kind": "none",
    }
    result = check_get_preconditions(row, "mapped")
    assert result is not None
    assert result["code"] == "INVALID_ARGUMENT"
    assert "map_kind" in result["message"]


def test_check_get_preconditions_passes_for_mapped_ready_full() -> None:
    row = {
        "deleted_at": None,
        "map_status": "ready",
        "map_kind": "full",
    }
    result = check_get_preconditions(row, "mapped")
    assert result is None


def test_check_get_preconditions_passes_for_mapped_ready_partial() -> None:
    row = {
        "deleted_at": None,
        "map_status": "ready",
        "map_kind": "partial",
    }
    result = check_get_preconditions(row, "mapped")
    assert result is None


def test_check_get_preconditions_passes_for_envelope_target() -> None:
    row = {
        "deleted_at": None,
        "map_status": "pending",
        "map_kind": "none",
    }
    result = check_get_preconditions(row, "envelope")
    assert result is None


# ---- is_sampled_only ----

def test_is_sampled_only_partial() -> None:
    assert is_sampled_only({"map_kind": "partial"}) is True


def test_is_sampled_only_full() -> None:
    assert is_sampled_only({"map_kind": "full"}) is False


def test_is_sampled_only_none() -> None:
    assert is_sampled_only({"map_kind": "none"}) is False


def test_is_sampled_only_missing_key() -> None:
    assert is_sampled_only({}) is False
