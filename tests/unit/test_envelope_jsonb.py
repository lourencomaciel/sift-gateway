from __future__ import annotations

from mcp_artifact_gateway.envelope.jsonb import envelope_to_jsonb
from mcp_artifact_gateway.envelope.model import Envelope, JsonContentPart


def _sample_envelope() -> Envelope:
    return Envelope(
        upstream_instance_id="up_1",
        upstream_prefix="github",
        tool="search_issues",
        status="ok",
        content=[JsonContentPart(value={"k": "v"})],
        meta={"warnings": []},
    )


def test_envelope_jsonb_full() -> None:
    payload = envelope_to_jsonb(_sample_envelope(), mode="full", minimize_threshold_bytes=1)
    assert payload is not None
    assert payload["content"][0]["type"] == "json"


def test_envelope_jsonb_none() -> None:
    payload = envelope_to_jsonb(_sample_envelope(), mode="none", minimize_threshold_bytes=1)
    assert payload is None


def test_envelope_jsonb_minimal_for_large() -> None:
    envelope = Envelope(
        upstream_instance_id="up_1",
        upstream_prefix="github",
        tool="search_issues",
        status="ok",
        content=[JsonContentPart(value={"k": "x" * 500})],
        meta={"warnings": []},
    )
    payload = envelope_to_jsonb(envelope, mode="minimal_for_large", minimize_threshold_bytes=20)
    assert payload is not None
    assert "content_summary" in payload
    assert payload["content_summary"]["part_count"] == 1

