from __future__ import annotations

import json

from sift_gateway.envelope.content_extract import (
    first_queryable_json_from_content,
    first_queryable_json_from_envelope,
    first_queryable_json_from_payload,
    parse_text_as_json,
    queryable_json_from_part,
)
from sift_gateway.envelope.model import (
    Envelope,
    JsonContentPart,
    TextContentPart,
)


def test_parse_text_as_json_accepts_double_encoded_object() -> None:
    inner = json.dumps({"ok": True})
    double = json.dumps(inner)
    assert parse_text_as_json(double) == {"ok": True}


def test_parse_text_as_json_rejects_scalar_payload() -> None:
    assert parse_text_as_json("42") is None
    assert parse_text_as_json('"plain string"') is None


def test_first_queryable_json_from_content_reads_text_json_with_provenance() -> (
    None
):
    resolved = first_queryable_json_from_content(
        [
            TextContentPart(text='{"results":[{"id":1}]}'),
            JsonContentPart(value={"ignored": True}),
        ]
    )
    assert resolved is not None
    assert resolved.value == {"results": [{"id": 1}]}
    assert resolved.part_index == 0
    assert resolved.part_type == "text"
    assert resolved.source_encoding == "parsed_text_json"


def test_first_queryable_json_from_payload_reads_json_part_with_provenance() -> (
    None
):
    resolved = first_queryable_json_from_payload(
        {
            "content": [
                {"type": "text", "text": "not json"},
                {"type": "json", "value": {"items": [1, 2]}},
            ]
        }
    )
    assert resolved is not None
    assert resolved.value == {"items": [1, 2]}
    assert resolved.part_index == 1
    assert resolved.part_type == "json"
    assert resolved.source_encoding == "native_json"


def test_queryable_json_from_part_supports_mapping_shape() -> None:
    value, part_type, encoding = queryable_json_from_part(
        {"type": "text", "text": '{"items":[1]}'}
    )
    assert value == {"items": [1]}
    assert part_type == "text"
    assert encoding == "parsed_text_json"


def test_first_queryable_json_from_payload_handles_invalid_content_shape() -> None:
    assert first_queryable_json_from_payload({"content": "not-a-list"}) is None


def test_first_queryable_json_from_envelope_reads_json_part() -> None:
    env = Envelope(
        upstream_instance_id="inst",
        upstream_prefix="demo",
        tool="echo",
        status="ok",
        content=[JsonContentPart(value={"ok": True})],
    )
    resolved = first_queryable_json_from_envelope(env)
    assert resolved is not None
    assert resolved.value == {"ok": True}
    assert resolved.part_index == 0
