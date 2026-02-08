from __future__ import annotations

from mcp_artifact_gateway.envelope.model import Envelope, JsonContentPart
from mcp_artifact_gateway.envelope.responses import gateway_error, gateway_tool_result


def _envelope() -> Envelope:
    return Envelope(
        upstream_instance_id="up_1",
        upstream_prefix="github",
        tool="search_issues",
        status="ok",
        content=[JsonContentPart(value={"ok": True})],
    )


def test_gateway_tool_result_handle_only_when_not_eligible() -> None:
    response = gateway_tool_result(
        artifact_id="art_1",
        envelope=_envelope(),
        payload_json_bytes=40_000,
        payload_total_bytes=40_000,
        contains_binary_refs=False,
        inline_allowed=True,
    )
    assert response["type"] == "gateway_tool_result"
    assert response["meta"]["inline"] is False
    assert "envelope" not in response


def test_gateway_tool_result_inlines_when_eligible() -> None:
    response = gateway_tool_result(
        artifact_id="art_1",
        envelope=_envelope(),
        payload_json_bytes=100,
        payload_total_bytes=100,
        contains_binary_refs=False,
        inline_allowed=True,
    )
    assert response["meta"]["inline"] is True
    assert response["envelope"]["type"] == "mcp_envelope"


def test_gateway_error_shape() -> None:
    response = gateway_error("INVALID_ARGUMENT", "bad request", details={"field": "x"})
    assert response == {
        "type": "gateway_error",
        "code": "INVALID_ARGUMENT",
        "message": "bad request",
        "details": {"field": "x"},
    }

