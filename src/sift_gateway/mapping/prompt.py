"""Format schema information into LLM-ready prompt text.

Converts the structured ``describe`` result (roots + field schemas)
into a compact, human-readable string that an LLM can use to write
code queries against the dataset.
"""

from __future__ import annotations

import json
from typing import Any

_MAX_NESTING_HINT_KEYS = 4


def _field_path(field: dict[str, Any]) -> str:
    """Extract field path, supporting both ``path`` and ``field_path`` keys."""
    return str(field.get("path") or field.get("field_path") or "?")


def _field_type_names(field: dict[str, Any]) -> list[str]:
    """Extract type names as a list from either list or string format."""
    raw = field.get("types", [])
    if isinstance(raw, list):
        return [str(t) for t in raw]
    if isinstance(raw, str):
        return [raw]
    return []


def _field_has_type(field: dict[str, Any], type_name: str) -> bool:
    """Check if a field contains a specific type (list or string format)."""
    for t in _field_type_names(field):
        if t == type_name or t.startswith(f"{type_name}<"):
            return True
    return False


def _split_path_segments(path: str) -> list[str]:
    """Split a relative schema path into key segments.

    Handles both dot notation (``a.b.c``) and bracket notation
    (``a['key.with.dots'].c``) produced by ``normalize_path_segment``.
    """
    segments: list[str] = []
    i = 0
    while i < len(path):
        if path[i] == ".":
            i += 1
            continue
        if path[i : i + 2] == "['":
            end = path.find("']", i + 2)
            if end == -1:
                break
            segments.append(path[i + 2 : end])
            i = end + 2
        else:
            # Dot-delimited segment: read up to next `.` or `['`.
            end = len(path)
            for delim in (".", "['"):
                pos = path.find(delim, i)
                if pos != -1 and pos < end:
                    end = pos
            segments.append(path[i:end])
            i = end
    return segments


def _is_direct_child(
    parent_path: str,
    child_path: str,
) -> str | None:
    """Return the child key name if *child_path* is a direct child.

    Handles both dot-notation (``.key``) and bracket-notation
    (``['key.with.dots']``) segments produced by
    ``normalize_path_segment``.

    Returns ``None`` when *child_path* is not a direct child.
    """
    if not child_path.startswith(parent_path):
        return None
    tail = child_path[len(parent_path) :]
    if tail.startswith("."):
        key = tail[1:]
        # No further nesting allowed.
        if "." in key or "[" in key:
            return None
        return key
    if tail.startswith("['"):
        # Bracket-notation: ['some.key']
        end = tail.find("']")
        if end == -1:
            return None
        key = tail[2:end]
        # Anything after the closing bracket means deeper nesting.
        if len(tail) > end + 2:
            return None
        return key
    return None


def _build_nesting_hint(
    field_path: str,
    all_fields: list[dict[str, Any]],
) -> str | None:
    """Build inline dict annotation for object-typed fields.

    For a field like ``$[*].birth.place.country`` with type
    ``object``, looks at direct children to produce a compact hint
    such as ``{"en": string, "no": string, "se": string}``.
    """
    children: list[tuple[str, str]] = []

    for f in all_fields:
        fp = _field_path(f)
        key = _is_direct_child(field_path, fp)
        if key is None:
            continue
        types = _field_type_names(f)
        type_str = types[0] if len(types) == 1 else "/".join(types)
        children.append((key, type_str))

    if not children:
        return None

    parts = [
        f"{json.dumps(k)}: {v}" for k, v in children[:_MAX_NESTING_HINT_KEYS]
    ]
    if len(children) > _MAX_NESTING_HINT_KEYS:
        return "{" + ", ".join(parts) + ", ...}"
    return "{" + ", ".join(parts) + "}"


def format_schema_for_prompt(
    describe_result: dict[str, Any],
) -> str:
    """Format schema info from a describe result into prompt text.

    Args:
        describe_result: The dict returned by ``describe_artifact``,
            containing ``schemas`` and ``roots`` keys.

    Returns:
        A multi-line string suitable for inclusion in an LLM prompt.
    """
    schemas = describe_result.get("schemas", [])
    roots = describe_result.get("roots", [])

    parts: list[str] = []

    if roots:
        parts.append("Dataset roots:")
        for root in roots:
            rp = root.get("root_path", "$")
            count = root.get("count_estimate", "?")
            shape = root.get("root_shape", "?")
            parts.append(f"  - root_path: {rp}, count: {count}, shape: {shape}")

    for schema in schemas:
        rp = schema.get("root_path", "$")
        parts.append(f"\nSchema for root '{rp}':")
        fields = schema.get("fields", [])
        for field in fields:
            fp = _field_path(field)
            example = field.get("example_value")
            nullable = field.get("nullable", False)

            # For object-typed fields, show inline nesting hint
            # so the LLM knows to drill into nested keys.
            nesting = None
            if _field_has_type(field, "object"):
                nesting = _build_nesting_hint(fp, fields)

            type_label = "/".join(_field_type_names(field)) or "?"
            if nesting:
                line = f"  - {fp}: {type_label} {nesting}"
            else:
                line = f"  - {fp}: {type_label}"
            if nullable:
                line += " (nullable)"
            if example is not None:
                example_str = json.dumps(example)
                if len(example_str) > 80:
                    example_str = example_str[:77] + "..."
                line += f" — e.g. {example_str}"
            parts.append(line)

        # Resolve the root entry once; reused by the columnar and
        # nested-field hints below.
        matching_root = next(
            (r for r in roots if r.get("root_path") == rp),
            None,
        )

        # Detect columnar layout: object root where most fields
        # are arrays (e.g. weather data stored as parallel arrays).
        if (
            matching_root is not None
            and matching_root.get("root_shape") == "object"
            and fields
        ):
            array_count = sum(1 for f in fields if _field_has_type(f, "array"))
            if array_count >= len(fields) / 2:
                # Pick the first array field name for the example.
                example_field = next(
                    (
                        _field_path(f).rsplit(".", 1)[-1]
                        for f in fields
                        if _field_has_type(f, "array")
                    ),
                    "field",
                )
                has_nullable = any(
                    f.get("nullable", False)
                    for f in fields
                    if _field_has_type(f, "array")
                )
                if has_nullable:
                    example_block = (
                        "\nExample pattern (filter nulls first):"
                        f"\n  valid = [v for v in"
                        f' data["{example_field}"]'
                        " if v is not None]"
                        f"\n  total = sum(valid)"
                        f"\n  n = len(valid)"
                    )
                else:
                    example_block = (
                        "\nExample pattern:"
                        f'\n  total = sum(data["{example_field}"])'
                        f'\n  n = len(data["{example_field}"])'
                    )
                parts.append(
                    "\nIMPORTANT — This root is columnar"
                    " (dict of parallel arrays)."
                    "\n`data` is a dict where each key maps"
                    " to a list of values."
                    "\nAll lists have the same length; index"
                    " i corresponds to the same record."
                    f'\nAccess: data["{example_field}"][i],'
                    ' NOT data[i]["field"].'
                    f"{example_block}"
                )

        # Detect deeply nested fields in array roots and add a
        # path->code hint so the LLM navigates them correctly.
        if (
            matching_root is not None
            and matching_root.get("root_shape") == "array"
            and fields
        ):
            prefix = rp.rstrip(".") + "[*]."
            nested_examples: list[tuple[str, str]] = []
            for f in fields:
                fp = _field_path(f)
                if not fp.startswith(prefix):
                    continue
                relative = fp[len(prefix) :]
                segments = _split_path_segments(relative)
                if len(segments) < 2:
                    continue
                access = "".join(f'["{s}"]' for s in segments)
                nested_examples.append((fp, access))

            if nested_examples:
                nested_examples.sort(
                    key=lambda t: t[0].count("."),
                    reverse=True,
                )
                fp_eg, access_eg = nested_examples[0]
                eg_segments = _split_path_segments(fp_eg[len(prefix) :])
                get_chain = "item"
                for seg in eg_segments[:-1]:
                    get_chain += f'.get("{seg}", {{}})'
                get_chain += f'.get("{eg_segments[-1]}")'
                parts.append(
                    "\nNested field access: schema paths"
                    " use dots for nesting."
                    "\nTranslate each dot segment into"
                    " a dict lookup."
                    f"\n  {fp_eg}"
                    f"\n  → item{access_eg}"
                    "\nUse .get() for safe nested"
                    " traversal to avoid KeyError:"
                    f"\n  {get_chain}"
                )

    return "\n".join(parts)
