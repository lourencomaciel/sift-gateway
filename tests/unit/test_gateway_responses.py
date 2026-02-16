from __future__ import annotations

from sift_mcp.envelope.responses import (
    can_passthrough,
    gateway_error,
    gateway_tool_result,
)


def test_gateway_tool_result_handle_only() -> None:
    response = gateway_tool_result(
        artifact_id="art_1",
        cache_meta={"hit": True},
    )
    assert response["type"] == "gateway_tool_result"
    assert response["artifact_id"] == "art_1"
    assert response["meta"] == {"cache": {"hit": True}}
    assert "mapping" not in response
    assert "schemas" not in response
    assert "usage_hint" not in response


def test_gateway_tool_result_no_cache_meta() -> None:
    response = gateway_tool_result(artifact_id="art_2")
    assert response["type"] == "gateway_tool_result"
    assert response["artifact_id"] == "art_2"
    assert response["meta"] == {"cache": {}}
    assert "mapping" not in response
    assert "schemas" not in response
    assert "usage_hint" not in response


def test_gateway_tool_result_with_schema_payload_and_hint() -> None:
    mapping = {"map_kind": "full", "map_status": "ready"}
    schemas = [{"root_path": "$.data", "fields": []}]
    schema_legend = {"field": {"p": "path"}}
    response = gateway_tool_result(
        artifact_id="art_3",
        cache_meta={"reused": False},
        mapping=mapping,
        schemas=schemas,
        schema_legend=schema_legend,
        usage_hint="Use artifact.get to retrieve.",
    )
    assert response["type"] == "gateway_tool_result"
    assert response["artifact_id"] == "art_3"
    assert response["mapping"] is mapping
    assert response["schemas"] is schemas
    assert response["schema_legend"] is schema_legend
    assert response["usage_hint"] == "Use artifact.get to retrieve."
    assert response["meta"] == {"cache": {"reused": False}}


def test_gateway_tool_result_with_canonical_pagination() -> None:
    pagination = {
        "layer": "upstream",
        "retrieval_status": "PARTIAL",
        "partial_reason": "MORE_PAGES_AVAILABLE",
        "has_more": True,
        "page_number": 0,
        "next_action": {
            "tool": "artifact.next_page",
            "arguments": {"artifact_id": "art_3"},
        },
        "warning": "INCOMPLETE_RESULT_SET",
        "has_next_page": True,
        "hint": "More results are available.",
    }
    response = gateway_tool_result(
        artifact_id="art_3",
        pagination=pagination,
    )
    assert response["pagination"] == pagination


# -- can_passthrough boundary tests --


def test_can_passthrough_small_payload_allowed() -> None:
    assert (
        can_passthrough(
            payload_total_bytes=100,
            contains_binary_refs=False,
            passthrough_allowed=True,
            max_bytes=1000,
        )
        is True
    )


def test_can_passthrough_large_payload() -> None:
    assert (
        can_passthrough(
            payload_total_bytes=2000,
            contains_binary_refs=False,
            passthrough_allowed=True,
            max_bytes=1000,
        )
        is False
    )


def test_can_passthrough_binary_refs() -> None:
    assert (
        can_passthrough(
            payload_total_bytes=100,
            contains_binary_refs=True,
            passthrough_allowed=True,
            max_bytes=1000,
        )
        is False
    )


def test_can_passthrough_not_allowed() -> None:
    assert (
        can_passthrough(
            payload_total_bytes=100,
            contains_binary_refs=False,
            passthrough_allowed=False,
            max_bytes=1000,
        )
        is False
    )


def test_can_passthrough_max_bytes_zero_disabled() -> None:
    assert (
        can_passthrough(
            payload_total_bytes=100,
            contains_binary_refs=False,
            passthrough_allowed=True,
            max_bytes=0,
        )
        is False
    )


def test_can_passthrough_exactly_at_boundary() -> None:
    """payload_total_bytes == max_bytes is False (strict less-than)."""
    assert (
        can_passthrough(
            payload_total_bytes=1000,
            contains_binary_refs=False,
            passthrough_allowed=True,
            max_bytes=1000,
        )
        is False
    )


# -- gateway_error (unchanged) --


def test_gateway_error_shape() -> None:
    response = gateway_error(
        "INVALID_ARGUMENT", "bad request", details={"field": "x"}
    )
    assert response == {
        "type": "gateway_error",
        "code": "INVALID_ARGUMENT",
        "message": "bad request",
        "details": {"field": "x"},
    }
