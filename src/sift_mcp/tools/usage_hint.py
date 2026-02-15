"""Build heuristic usage hints from describe data.

Generates natural language instructions for the calling model,
describing what the artifact contains and which tools to call
next.  Purely rule-based — no LLM involved.
"""

from __future__ import annotations

import re
from typing import Any

PAGINATION_COMPLETENESS_RULE = (
    "Do not claim completeness until pagination.retrieval_status == COMPLETE."
)


def with_pagination_completeness_rule(text: str) -> str:
    """Append the pagination completion rule to hint text.

    Args:
        text: Base hint text.

    Returns:
        Hint text including the completion rule.
    """
    trimmed = text.strip()
    if not trimmed:
        return PAGINATION_COMPLETENESS_RULE
    if trimmed.endswith(PAGINATION_COMPLETENESS_RULE):
        return trimmed
    if not trimmed.endswith("."):
        trimmed = f"{trimmed}."
    return f"{trimmed} {PAGINATION_COMPLETENESS_RULE}"


_TOP_LEVEL_FIELD_PATH_RE = re.compile(r"^\$\.([A-Za-z_][A-Za-z0-9_]*)$")


def _field_names(root: dict[str, Any], *, limit: int = 8) -> list[str]:
    """Extract up to *limit* top-level fields from root-local schema data."""
    schema = root.get("schema")
    if isinstance(schema, dict):
        raw_fields = schema.get("fields")
        if isinstance(raw_fields, list):
            names: list[str] = []
            seen: set[str] = set()
            for item in raw_fields:
                if not isinstance(item, dict):
                    continue
                path = item.get("path")
                if not isinstance(path, str):
                    continue
                match = _TOP_LEVEL_FIELD_PATH_RE.match(path)
                if match is None:
                    continue
                name = match.group(1)
                if name in seen:
                    continue
                seen.add(name)
                names.append(name)
                if len(names) >= limit:
                    return names
            if names:
                return names
    fields_top = root.get("fields_top")
    if not isinstance(fields_top, dict):
        return []
    return list(fields_top.keys())[:limit]


def _schema_fields_for_root(
    root_path: str,
    schemas_by_path: dict[str, dict[str, Any]],
) -> list[str]:
    """Extract top-level field names from the canonical schema list."""
    schema = schemas_by_path.get(root_path)
    if not isinstance(schema, dict):
        return []
    raw_fields = schema.get("fields")
    if not isinstance(raw_fields, list):
        return []
    names: list[str] = []
    seen: set[str] = set()
    for item in raw_fields:
        if not isinstance(item, dict):
            continue
        path = item.get("path")
        if not isinstance(path, str):
            continue
        match = _TOP_LEVEL_FIELD_PATH_RE.match(path)
        if match is None:
            continue
        name = match.group(1)
        if name in seen:
            continue
        seen.add(name)
        names.append(name)
    return names


def _root_summary(root: dict[str, Any]) -> str:
    """Describe a single root in natural language.

    Args:
        root: A root dict from the describe response.

    Returns:
        A short description like ``$.data (array, 100 items)``.
    """
    path = root.get("root_path", "$")
    shape = root.get("root_shape", "unknown")
    count = root.get("count_estimate")
    if shape == "array" and isinstance(count, int) and count > 0:
        return f"{path} ({shape}, {count} items)"
    return f"{path} ({shape})"


def build_usage_hint(
    artifact_id: str,
    describe: dict[str, Any],
) -> str:
    """Build a natural language usage hint from describe data.

    Heuristic rules applied in order:

    1. Mapping pending → tell the model to wait.
    2. Primary root description (shape, count, fields).
    3. Suggested next tool call (select for arrays, get for
       dicts).
    4. Alternative roots listed.
    5. Sampling note if sample_indices present.

    Args:
        artifact_id: The artifact identifier.
        describe: The full describe response dict with
            ``mapping`` and ``roots`` sections.

    Returns:
        A non-empty hint string.
    """
    mapping = describe.get("mapping", {})
    map_status = mapping.get("map_status", "pending")
    roots: list[dict[str, Any]] = describe.get("roots", [])
    schemas_raw = describe.get("schemas", [])
    schemas_by_path: dict[str, dict[str, Any]] = (
        {
            str(schema["root_path"]): schema
            for schema in schemas_raw
            if isinstance(schema, dict)
            and isinstance(schema.get("root_path"), str)
        }
        if isinstance(schemas_raw, list)
        else {}
    )

    # Mapping failed -- advise raw retrieval
    if map_status == "failed":
        return (
            "Mapping failed. Use "
            f'artifact(action="query", query_kind="get", artifact_id="{artifact_id}", '
            'target="envelope") '
            "to retrieve raw content."
        )

    # Mapping not yet complete
    if map_status not in {"complete", "done", "ready"}:
        return (
            "Mapping in progress. Call "
            f'artifact(action="query", query_kind="describe", artifact_id="{artifact_id}") '
            "to check status later."
        )

    # Schema-first fallback when roots are omitted.
    if not roots and schemas_by_path:
        schema_entries = [
            schema
            for schema in schemas_raw
            if isinstance(schema, dict)
            and isinstance(schema.get("root_path"), str)
        ]
        if schema_entries:
            def _schema_sort_key(schema: dict[str, Any]) -> tuple[int, str]:
                coverage = schema.get("coverage")
                observed_records = 0
                if isinstance(coverage, dict):
                    raw_count = coverage.get("observed_records")
                    if isinstance(raw_count, int):
                        observed_records = raw_count
                root_path = schema.get("root_path")
                path_value = root_path if isinstance(root_path, str) else "$"
                return (-observed_records, path_value)

            ordered = sorted(schema_entries, key=_schema_sort_key)
            primary_schema = ordered[0]
            primary_path = str(primary_schema.get("root_path", "$"))
            coverage = primary_schema.get("coverage")
            observed_records = 0
            if isinstance(coverage, dict):
                raw_count = coverage.get("observed_records")
                if isinstance(raw_count, int):
                    observed_records = raw_count

            schema_parts: list[str] = []
            if observed_records > 0:
                schema_parts.append(
                    f"Contains {observed_records} records at {primary_path}"
                )
            else:
                schema_parts.append(f"Schema available at {primary_path}")

            fields = _schema_fields_for_root(primary_path, schemas_by_path)[:8]
            if fields:
                schema_parts.append(f"Fields: {', '.join(fields)}")

            if fields:
                select_fields = fields[:4]
                select_list = ", ".join(f'"{f}"' for f in select_fields)
            else:
                select_list = '"field1", "field2"'
            schema_parts.append(
                'Use artifact(action="query", '
                'query_kind="select", '
                f'artifact_id="{artifact_id}", '
                f'root_path="{primary_path}", '
                f"select_paths=[{select_list}]"
                ") to project specific fields"
            )
            schema_parts.append(
                "If projection fails for this root shape, use get + jsonpath instead"
            )
            schema_parts.append(
                "Add where to filter (e.g. where='to_number(spend) > 0')"
            )
            schema_parts.append(
                "Use count_only=true for counts, distinct=true for unique values"
            )
            schema_parts.append(
                "Continue partial results with query + cursor (not next_page)"
            )
            schema_parts.append(
                "Minimize context: request only needed "
                "fields and rows, then expand if needed"
            )
            schema_parts.append(
                f'Tip: pass "{artifact_id}" directly as an argument'
                " to another tool. Use"
                f' "{artifact_id}:$.path" to pass a specific field'
                f' (e.g. "{artifact_id}:$.items[0].name")'
            )
            if len(ordered) > 1:
                alternates = [
                    str(item["root_path"])
                    for item in ordered[1:4]
                    if isinstance(item.get("root_path"), str)
                ]
                if alternates:
                    schema_parts.append(
                        f"Also available: {', '.join(alternates)}"
                    )

            return ". ".join(schema_parts) + "."

    # No roots or schemas discovered
    if not roots:
        return (
            "No structured mapping available. Use "
            f'artifact(action="query", query_kind="get", artifact_id="{artifact_id}", '
            'target="envelope") '
            "to retrieve raw content."
        )

    parts: list[str] = []
    primary = roots[0]
    path = primary.get("root_path", "$")
    shape = primary.get("root_shape", "unknown")
    count = primary.get("count_estimate")

    # Describe structure
    if shape == "array" and isinstance(count, int) and count > 0:
        parts.append(f"Contains {count} records at {path}")
    elif shape in {"dict", "object"}:
        parts.append(f"Contains a dict at {path}")
    else:
        parts.append(f"Root at {path} ({shape})")

    # Fields
    fields = _schema_fields_for_root(path, schemas_by_path)[:8]
    if not fields:
        fields = _field_names(primary)
    if fields:
        parts.append(f"Fields: {', '.join(fields)}")

    # Sampling note
    sample_indices = primary.get("sample_indices")
    if isinstance(sample_indices, list) and sample_indices:
        sampled_count = primary.get("sampled_record_count")
        if not isinstance(sampled_count, int):
            sampled_count = len(sample_indices)
        if isinstance(count, int) and count > sampled_count:
            parts.append(f"Sampled {sampled_count} of ~{count} records")

    # Suggested tool call
    if shape == "array":
        if fields:
            select_fields = fields[:4]
            select_list = ", ".join(f'"{f}"' for f in select_fields)
        else:
            select_list = '"field1", "field2"'
        parts.append(
            'Use artifact(action="query", '
            'query_kind="select", '
            f'artifact_id="{artifact_id}", '
            f'root_path="{path}", '
            f"select_paths=[{select_list}]"
            ") to project specific fields"
        )
        parts.append("Add where to filter (e.g. where='to_number(spend) > 0')")
        parts.append(
            "Use count_only=true for counts, distinct=true for unique values"
        )
        parts.append(
            "Continue partial results with query + cursor (not next_page)"
        )
        parts.append(
            "Minimize context: request only needed "
            "fields and rows, then expand if needed"
        )
    else:
        parts.append(
            'Use artifact(action="query", '
            'query_kind="get", '
            f'artifact_id="{artifact_id}", '
            'target="envelope"'
            ") to retrieve the full value"
        )

    # Artifact-as-argument forwarding hint
    parts.append(
        f'Tip: pass "{artifact_id}" directly as an argument'
        " to another tool. Use"
        f' "{artifact_id}:$.path" to pass a specific field'
        f' (e.g. "{artifact_id}:$.items[0].name")'
    )

    # Alternative roots
    if len(roots) > 1:
        alts = [_root_summary(r) for r in roots[1:4]]
        parts.append(f"Also available: {', '.join(alts)}")

    return ". ".join(parts) + "."
