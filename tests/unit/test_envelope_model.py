from __future__ import annotations

from sift_gateway.envelope.model import Envelope, JsonContentPart


def test_envelope_to_dict_shape() -> None:
    envelope = Envelope(
        upstream_instance_id="up_1",
        upstream_prefix="github",
        tool="search_issues",
        status="ok",
        content=[JsonContentPart(value={"x": 1})],
    )
    payload = envelope.to_dict()
    assert payload["type"] == "mcp_envelope"
    assert payload["status"] == "ok"
    assert payload["content"][0]["type"] == "json"
    assert payload["content"][0]["value"] == {"x": 1}
