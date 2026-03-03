"""Shared schema-payload normalization helpers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

_MAX_DISTINCT_VALUES = 1
_MAX_DISTINCT_TEXT_CHARS = 30
_TRUNCATED_SUFFIX = " more chars truncated)"


def _coerce_observed_count(raw_value: Any) -> int:
    """Normalize observed-count fields to non-negative integers."""
    if isinstance(raw_value, int):
        return raw_value
    return 0


def _truncate_schema_text(text: str, *, max_chars: int) -> str:
    """Return compact string previews in schema payloads."""
    if _is_truncated_preview(text):
        return text
    if len(text) <= max_chars:
        return text
    remaining = len(text) - max_chars
    return f"[{text[:max_chars]}]({remaining} more chars truncated)"


def _is_truncated_preview(text: str) -> bool:
    """Return True when text already matches truncation marker format."""
    if not text.startswith("[") or not text.endswith(_TRUNCATED_SUFFIX):
        return False
    marker_index = text.rfind("](")
    if marker_index <= 1:
        return False
    remaining = text[marker_index + 2 : -len(_TRUNCATED_SUFFIX)]
    return remaining.isdigit()


def _normalize_distinct_value(value: Any) -> Any:
    """Normalize one distinct value for public schema responses."""
    if isinstance(value, str):
        return _truncate_schema_text(
            value,
            max_chars=_MAX_DISTINCT_TEXT_CHARS,
        )
    return value


def _build_field_entry(
    field: Mapping[str, Any],
    *,
    include_null_example_value: bool,
) -> dict[str, Any]:
    """Normalize one schema field row to public response shape."""
    raw_types = field.get("types")
    types = (
        [str(item) for item in raw_types] if isinstance(raw_types, list) else []
    )
    entry: dict[str, Any] = {
        "path": field.get("field_path"),
        "types": types,
        "nullable": bool(field.get("nullable")),
        "required": bool(field.get("required")),
        "observed_count": _coerce_observed_count(field.get("observed_count")),
    }
    example_value = field.get("example_value")
    if isinstance(example_value, str):
        entry["example_value"] = example_value
    elif include_null_example_value:
        entry["example_value"] = None
    distinct_values = field.get("distinct_values")
    if isinstance(distinct_values, list):
        entry["distinct_values"] = [
            _normalize_distinct_value(value)
            for value in distinct_values[:_MAX_DISTINCT_VALUES]
        ]
    cardinality = field.get("cardinality")
    if isinstance(cardinality, int):
        entry["cardinality"] = cardinality
    return entry


def build_schema_payload(
    *,
    schema_root: Mapping[str, Any],
    field_rows: Sequence[Mapping[str, Any]],
    include_null_example_value: bool = False,
) -> dict[str, Any]:
    """Build normalized schema payload from root + field DB rows."""
    fields = [
        _build_field_entry(
            field,
            include_null_example_value=include_null_example_value,
        )
        for field in field_rows
    ]
    return {
        "version": schema_root.get("schema_version"),
        "schema_hash": schema_root.get("schema_hash"),
        "root_path": schema_root.get("root_path"),
        "mode": schema_root.get("mode"),
        "coverage": {
            "completeness": schema_root.get("completeness"),
            "observed_records": _coerce_observed_count(
                schema_root.get("observed_records")
            ),
        },
        "fields": fields,
        "determinism": {
            "dataset_hash": schema_root.get("dataset_hash"),
            "traversal_contract_version": schema_root.get(
                "traversal_contract_version"
            ),
            "map_budget_fingerprint": schema_root.get("map_budget_fingerprint"),
        },
    }
