"""Helpers for representative sample payloads in schema-ref responses."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
import re
from typing import Any

from sift_gateway.envelope.content_extract import (
    first_queryable_json_from_payload,
)
from sift_gateway.mapping.json_strings import resolve_json_strings
from sift_gateway.query.jsonpath import JsonPathError, evaluate_jsonpath

DEFAULT_SAMPLE_MAX_TEXT_CHARS = 160
_DEFAULT_MAX_JSONPATH_LENGTH = 4096
_DEFAULT_MAX_PATH_SEGMENTS = 64
_DEFAULT_MAX_WILDCARD_EXPANSION_TOTAL = 10_000
_JSONPATH_DOT_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _append_field_jsonpath(parent: str, field: str) -> str:
    """Append one object field segment to a JSONPath parent."""
    if _JSONPATH_DOT_IDENT_RE.fullmatch(field):
        return f"{parent}.{field}"
    escaped = (
        field.replace("\\", "\\\\")
        .replace("'", "\\'")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
    )
    return f"{parent}['{escaped}']"


def _schema_shape_signature(value: Any) -> tuple[Any, ...]:
    """Return a structural signature used to compare runtime item schemas."""
    if value is None:
        return ("null",)
    if isinstance(value, bool):
        return ("boolean",)
    if isinstance(value, (int, float)):
        return ("number",)
    if isinstance(value, str):
        return ("string",)
    if isinstance(value, list):
        element_signatures = tuple(
            sorted(
                {_schema_shape_signature(item) for item in value},
                key=repr,
            )
        )
        return ("array", element_signatures)
    if isinstance(value, dict):
        object_signature = tuple(
            (str(key), _schema_shape_signature(value[key]))
            for key in sorted(value, key=str)
        )
        return ("object", object_signature)
    return ("unknown", type(value).__name__)


def _truncate_sample_text(text: str, *, max_chars: int) -> tuple[str, bool]:
    """Truncate one sample text value while preserving truncation metadata."""
    if len(text) <= max_chars:
        return text, False
    truncated_chars = len(text) - max_chars
    head = text[:max_chars]
    return f"[{head}]({truncated_chars} more chars truncated)", True


def _truncate_sample_value(
    value: Any,
    *,
    max_chars: int,
) -> tuple[Any, bool]:
    """Recursively truncate long string values in a sample payload."""
    if isinstance(value, str):
        return _truncate_sample_text(value, max_chars=max_chars)
    if isinstance(value, list):
        truncated_any = False
        normalized_list: list[Any] = []
        for item in value:
            next_item, truncated = _truncate_sample_value(
                item,
                max_chars=max_chars,
            )
            normalized_list.append(next_item)
            truncated_any = truncated_any or truncated
        return normalized_list, truncated_any
    if isinstance(value, dict):
        truncated_any = False
        normalized_dict: dict[Any, Any] = {}
        for key, item in value.items():
            next_item, truncated = _truncate_sample_value(
                item,
                max_chars=max_chars,
            )
            normalized_dict[key] = next_item
            truncated_any = truncated_any or truncated
        return normalized_dict, truncated_any
    return value, False


def has_consistent_item_schema(items: Sequence[Any]) -> bool:
    """Return whether all items share the same structural schema."""
    if not items:
        return False
    first_signature = _schema_shape_signature(items[0])
    return all(
        _schema_shape_signature(item) == first_signature for item in items[1:]
    )


def build_representative_item_sample(
    items: Sequence[Any],
    *,
    max_text_chars: int = DEFAULT_SAMPLE_MAX_TEXT_CHARS,
) -> dict[str, Any] | None:
    """Return sample payload when first item represents all rows."""
    if not has_consistent_item_schema(items):
        return None
    sample_item, text_truncated = _truncate_sample_value(
        items[0],
        max_chars=max_text_chars,
    )
    payload: dict[str, Any] = {
        "sample_item": sample_item,
        "sample_item_source_index": 0,
        "sample_item_count": len(items),
    }
    if text_truncated:
        payload["sample_item_text_truncated"] = True
    return payload


def _as_positive_int(raw_value: Any, default: int) -> int:
    if isinstance(raw_value, int) and raw_value > 0:
        return raw_value
    return default


def resolve_item_sequence_with_path(
    payload: Any,
    *,
    root_path: str | None = None,
    max_jsonpath_length: int = _DEFAULT_MAX_JSONPATH_LENGTH,
    max_path_segments: int = _DEFAULT_MAX_PATH_SEGMENTS,
    max_wildcard_expansion_total: int = _DEFAULT_MAX_WILDCARD_EXPANSION_TOTAL,
) -> tuple[list[Any] | None, str | None]:
    r"""Resolve a representative item array and inferred root path.

    Normalize JSON-encoded strings first so payloads like
    ``{"result": "{\"data\": [...]}"}`` can be sampled directly.
    """
    normalized_payload = resolve_json_strings(payload)
    if isinstance(normalized_payload, list):
        return list(normalized_payload), "$"

    if isinstance(root_path, str) and root_path:
        try:
            matches = evaluate_jsonpath(
                normalized_payload,
                root_path,
                max_length=_as_positive_int(
                    max_jsonpath_length, _DEFAULT_MAX_JSONPATH_LENGTH
                ),
                max_segments=_as_positive_int(
                    max_path_segments, _DEFAULT_MAX_PATH_SEGMENTS
                ),
                max_wildcard_expansion_total=_as_positive_int(
                    max_wildcard_expansion_total,
                    _DEFAULT_MAX_WILDCARD_EXPANSION_TOTAL,
                ),
            )
        except JsonPathError:
            matches = []
        if len(matches) == 1 and isinstance(matches[0], list):
            return list(matches[0]), root_path

    if not isinstance(normalized_payload, Mapping):
        return None, None

    items = normalized_payload.get("items")
    if isinstance(items, list):
        return list(items), "$.items"

    list_fields = [
        (str(key), value)
        for key, value in normalized_payload.items()
        if isinstance(value, list)
    ]
    if len(list_fields) == 1:
        field_name, list_value = list_fields[0]
        return list(list_value), _append_field_jsonpath("$", field_name)
    return None, None


def resolve_item_sequence(
    payload: Any,
    *,
    root_path: str | None = None,
    max_jsonpath_length: int = _DEFAULT_MAX_JSONPATH_LENGTH,
    max_path_segments: int = _DEFAULT_MAX_PATH_SEGMENTS,
    max_wildcard_expansion_total: int = _DEFAULT_MAX_WILDCARD_EXPANSION_TOTAL,
) -> list[Any] | None:
    """Resolve a representative item array from a payload when possible."""
    items, _resolved_root_path = resolve_item_sequence_with_path(
        payload,
        root_path=root_path,
        max_jsonpath_length=max_jsonpath_length,
        max_path_segments=max_path_segments,
        max_wildcard_expansion_total=max_wildcard_expansion_total,
    )
    return items


def first_json_content_value(envelope_payload: Mapping[str, Any]) -> Any | None:
    """Return first ``content`` part JSON value from an envelope payload."""
    resolved = first_queryable_json_from_payload(envelope_payload)
    if resolved is None:
        return None
    return resolved.value


__all__ = [
    "build_representative_item_sample",
    "first_json_content_value",
    "has_consistent_item_schema",
    "resolve_item_sequence",
    "resolve_item_sequence_with_path",
]
