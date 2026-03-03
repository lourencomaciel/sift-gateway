"""Helpers to extract queryable JSON values from envelope content."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
import json
from typing import Any, Literal

from sift_gateway.envelope.model import (
    Envelope,
    JsonContentPart,
    TextContentPart,
)
from sift_gateway.mapping.json_strings import resolve_json_strings

PartType = Literal["json", "text"]
SourceEncoding = Literal["native_json", "parsed_text_json"]


@dataclass(frozen=True)
class QueryableJsonContent:
    """Resolved queryable JSON content from one envelope content part."""

    value: Any
    part_index: int
    part_type: PartType
    source_encoding: SourceEncoding


def parse_text_as_json(text: str) -> Any | None:
    r"""Parse text as JSON object/array; return ``None`` for scalars/invalid."""
    trimmed = text.strip()
    if not trimmed:
        return None
    try:
        parsed = json.loads(trimmed)
    except (json.JSONDecodeError, ValueError):
        return None
    if isinstance(parsed, str):
        nested = parsed.strip()
        if nested:
            try:
                parsed = json.loads(nested)
            except (json.JSONDecodeError, ValueError):
                return None
    if isinstance(parsed, (dict, list)):
        return parsed
    return None


def queryable_json_from_part(
    part: Any,
) -> tuple[Any | None, PartType | None, SourceEncoding | None]:
    """Resolve queryable JSON value from a content part-like object."""
    if isinstance(part, JsonContentPart):
        return resolve_json_strings(part.value), "json", "native_json"
    if isinstance(part, TextContentPart):
        parsed = parse_text_as_json(part.text)
        if parsed is not None:
            return resolve_json_strings(parsed), "text", "parsed_text_json"
        return None, None, None
    if not isinstance(part, Mapping):
        return None, None, None

    part_type = part.get("type")
    if part_type == "json" and "value" in part:
        return resolve_json_strings(part["value"]), "json", "native_json"
    if part_type == "text" and isinstance(part.get("text"), str):
        parsed = parse_text_as_json(part["text"])
        if parsed is not None:
            return resolve_json_strings(parsed), "text", "parsed_text_json"
    return None, None, None


def first_queryable_json_from_content(
    content: Sequence[Any],
) -> QueryableJsonContent | None:
    """Return first queryable JSON value from a content list."""
    for part_index, part in enumerate(content):
        value, part_type, source_encoding = queryable_json_from_part(part)
        if part_type is None or source_encoding is None:
            continue
        return QueryableJsonContent(
            value=value,
            part_index=part_index,
            part_type=part_type,
            source_encoding=source_encoding,
        )
    return None


def first_queryable_json_from_payload(
    envelope_payload: Mapping[str, Any],
) -> QueryableJsonContent | None:
    """Return first queryable JSON value from serialized envelope payload."""
    content = envelope_payload.get("content")
    if not isinstance(content, list):
        return None
    return first_queryable_json_from_content(content)


def first_queryable_json_from_envelope(
    envelope: Envelope,
) -> QueryableJsonContent | None:
    """Return first queryable JSON value from ``Envelope.content``."""
    return first_queryable_json_from_content(envelope.content)


__all__ = [
    "QueryableJsonContent",
    "first_queryable_json_from_content",
    "first_queryable_json_from_envelope",
    "first_queryable_json_from_payload",
    "parse_text_as_json",
    "queryable_json_from_part",
]
