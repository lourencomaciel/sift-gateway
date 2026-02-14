"""Build heuristic usage hints from describe data.

Generates natural language instructions for the calling model,
describing what the artifact contains and which tools to call
next.  Purely rule-based — no LLM involved.
"""

from __future__ import annotations

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


def _field_names(fields_top: Any, *, limit: int = 8) -> list[str]:
    """Extract up to *limit* field names from a fields_top dict.

    Args:
        fields_top: A ``fields_top`` dict mapping field names to
            type distributions, or any non-dict value.
        limit: Maximum number of field names to return.

    Returns:
        List of field name strings (may be empty).
    """
    if not isinstance(fields_top, dict):
        return []
    return list(fields_top.keys())[:limit]


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

    # No roots discovered
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
    elif shape == "dict":
        parts.append(f"Contains a dict at {path}")
    else:
        parts.append(f"Root at {path} ({shape})")

    # Fields
    fields = _field_names(primary.get("fields_top"))
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
