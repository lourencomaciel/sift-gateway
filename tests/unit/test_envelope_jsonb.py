from mcp_artifact_gateway.envelope.jsonb import minimal_projection, prepare_envelope_jsonb
from mcp_artifact_gateway.envelope.model import (
    ContentPartBinaryRef,
    ContentPartJson,
    ContentPartResourceRef,
    ContentPartText,
    Envelope,
    EnvelopeMeta,
    UpstreamPagination,
)


def _envelope() -> Envelope:
    return Envelope(
        upstream_instance_id="u1",
        upstream_prefix="p",
        tool="tool",
        status="ok",
        content=[
            ContentPartJson(value={"a": 1}),
            ContentPartText(text="hello"),
            ContentPartResourceRef(uri="file://x", durability="external_ref"),
            ContentPartBinaryRef(
                blob_id="bin_123",
                binary_hash="hash",
                mime="application/octet-stream",
                byte_count=10,
            ),
        ],
        meta=EnvelopeMeta(
            warnings=[{"type": "warn"}],
            upstream_pagination=UpstreamPagination(next_cursor="c", has_more=True),
        ),
    )


def test_minimal_projection_drops_values() -> None:
    env = _envelope()
    proj = minimal_projection(env)
    assert proj["status"] == "ok"
    # JSON and text parts should be descriptors only
    assert proj["content"][0] == {"type": "json"}
    assert proj["content"][1] == {"type": "text"}


def test_prepare_envelope_jsonb_modes() -> None:
    env = _envelope()
    full = prepare_envelope_jsonb(env, "full", threshold_bytes=0, payload_json_bytes=0)
    assert "content" in full and "value" in full["content"][0]

    minimal = prepare_envelope_jsonb(env, "minimal_for_large", threshold_bytes=1, payload_json_bytes=10)
    assert minimal["content"][0] == {"type": "json"}

    none_mode = prepare_envelope_jsonb(env, "none", threshold_bytes=0, payload_json_bytes=0)
    assert none_mode["content"][0] == {"type": "json"}
