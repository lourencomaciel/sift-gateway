from __future__ import annotations

from mcp_artifact_gateway.envelope.responses import (
    can_passthrough,
    gateway_error,
    gateway_tool_result,
)


def test_gateway_tool_result_handle_only() -> None:
    response = gateway_tool_result(
        artifact_id="art_1",
        cache_meta={"hit": True},
    )
    assert response == {
        "type": "gateway_tool_result",
        "artifact_id": "art_1",
        "meta": {"cache": {"hit": True}},
    }
    assert "inline" not in response["meta"]


def test_gateway_tool_result_no_cache_meta() -> None:
    response = gateway_tool_result(artifact_id="art_2")
    assert response == {
        "type": "gateway_tool_result",
        "artifact_id": "art_2",
        "meta": {"cache": {}},
    }
    assert "inline" not in response["meta"]


# -- can_passthrough boundary tests --


def test_can_passthrough_small_payload_allowed() -> None:
    assert can_passthrough(
        payload_total_bytes=100,
        contains_binary_refs=False,
        passthrough_allowed=True,
        max_bytes=1000,
    ) is True


def test_can_passthrough_large_payload() -> None:
    assert can_passthrough(
        payload_total_bytes=2000,
        contains_binary_refs=False,
        passthrough_allowed=True,
        max_bytes=1000,
    ) is False


def test_can_passthrough_binary_refs() -> None:
    assert can_passthrough(
        payload_total_bytes=100,
        contains_binary_refs=True,
        passthrough_allowed=True,
        max_bytes=1000,
    ) is False


def test_can_passthrough_not_allowed() -> None:
    assert can_passthrough(
        payload_total_bytes=100,
        contains_binary_refs=False,
        passthrough_allowed=False,
        max_bytes=1000,
    ) is False


def test_can_passthrough_max_bytes_zero_disabled() -> None:
    assert can_passthrough(
        payload_total_bytes=100,
        contains_binary_refs=False,
        passthrough_allowed=True,
        max_bytes=0,
    ) is False


def test_can_passthrough_exactly_at_boundary() -> None:
    """payload_total_bytes == max_bytes is False (strict less-than)."""
    assert can_passthrough(
        payload_total_bytes=1000,
        contains_binary_refs=False,
        passthrough_allowed=True,
        max_bytes=1000,
    ) is False


# -- gateway_error (unchanged) --


def test_gateway_error_shape() -> None:
    response = gateway_error("INVALID_ARGUMENT", "bad request", details={"field": "x"})
    assert response == {
        "type": "gateway_error",
        "code": "INVALID_ARGUMENT",
        "message": "bad request",
        "details": {"field": "x"},
    }
