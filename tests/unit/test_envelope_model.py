from decimal import Decimal

import pytest

from mcp_artifact_gateway.envelope.model import (
    ContentPartJson,
    ContentPartText,
    Envelope,
    ErrorBlock,
)


def test_envelope_invariants_ok_requires_no_error() -> None:
    with pytest.raises(ValueError):
        Envelope(
            upstream_instance_id="u",
            upstream_prefix="p",
            tool="t",
            status="ok",
            content=[],
            error=ErrorBlock(code="UPSTREAM_ERROR", message="boom"),
        )


def test_envelope_invariants_error_requires_error() -> None:
    with pytest.raises(ValueError):
        Envelope(
            upstream_instance_id="u",
            upstream_prefix="p",
            tool="t",
            status="error",
            content=[],
            error=None,
        )


def test_to_dict_preserves_decimal() -> None:
    env = Envelope(
        upstream_instance_id="u",
        upstream_prefix="p",
        tool="t",
        status="ok",
        content=[ContentPartJson(value={"x": Decimal("1.5")})],
    )
    data = env.to_dict()
    assert isinstance(data["content"][0]["value"]["x"], Decimal)


def test_content_parts_types() -> None:
    env = Envelope(
        upstream_instance_id="u",
        upstream_prefix="p",
        tool="t",
        status="ok",
        content=[ContentPartText(text="hi")],
    )
    assert env.content[0].type == "text"
