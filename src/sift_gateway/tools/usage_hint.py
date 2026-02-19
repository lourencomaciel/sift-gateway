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
_FILTER_GUIDANCE = (
    'Add where to filter, e.g. where={"path":"$.spend","op":"gt","value":0}. '
    "Ops: eq, ne, gt, gte, lt, lte, in, contains, array_contains, exists, "
    'not_exists. Combine: {"logic":"and"|"or","filters":[...]}. '
    'Negate: {"not":{...}}'
)
_COUNT_GUIDANCE = (
    "Use count_only=true for counts, distinct=true for unique values"
)
_CURSOR_GUIDANCE = (
    "Continue partial results with query + cursor (not next_page)"
)
_CONTEXT_GUIDANCE = "Minimize context: request only needed fields and rows, then expand if needed"


def _top_level_names_from_schema_fields(
    raw_fields: Any,
    *,
    limit: int | None = None,
) -> list[str]:
    """Extract unique top-level field names from schema ``fields`` rows."""
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
        if limit is not None and len(names) >= limit:
            break
    return names


def _field_names(root: dict[str, Any], *, limit: int = 8) -> list[str]:
    """Extract up to *limit* top-level fields from root-local schema data."""
    schema = root.get("schema")
    if isinstance(schema, dict):
        names = _top_level_names_from_schema_fields(
            schema.get("fields"),
            limit=limit,
        )
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
    return _top_level_names_from_schema_fields(schema.get("fields"))


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


def _code_query_hint(
    artifact_id: str,
    root_path: str,
    *,
    code_query_enabled: bool = True,
    code_query_packages: list[str] | None = None,
) -> str:
    """Return a compact hint for root-scoped Python code queries."""
    if not code_query_enabled:
        return ""
    hint = (
        "For aggregations or derived metrics, run root-scoped Python with "
        'artifact(action="query", query_kind="code", '
        f'artifact_id="{artifact_id}", root_path="{root_path}", '
        'code="def run(data, schema, params): ...", params={})'
        ". For multi-artifact analysis, pass artifact_ids=[...] and define "
        "run(artifacts, schemas, params); use root_paths={artifact_id: "
        "root_path, ...} when each artifact has a different root path"
        ". Code queries return all results (no pagination), scalar/dict "
        "returns are auto-wrapped to a list, and runtime failures include "
        "tracebacks with line numbers. Aggregate inside run() to stay within "
        "max_bytes_out"
    )
    if code_query_packages is None:
        return hint
    if code_query_packages:
        return (
            f"{hint}. Available code-query packages in this runtime: "
            + ", ".join(code_query_packages)
        )
    return f"{hint}. No third-party code-query packages are currently available"


def _select_paths_hint(fields: list[str]) -> str:
    """Render a select_paths list literal from candidate field names."""
    if not fields:
        return '"field1", "field2"'
    return ", ".join(f'"{f}"' for f in fields[:4])


def _select_call_hint(
    artifact_id: str,
    root_path: str,
    fields: list[str],
) -> str:
    """Render a select query example for a root path."""
    select_list = _select_paths_hint(fields)
    return (
        'Use artifact(action="query", '
        'query_kind="select", '
        f'artifact_id="{artifact_id}", '
        f'root_path="{root_path}", '
        f"select_paths=[{select_list}]"
        ") to project specific fields"
    )


def _forwarding_hint(artifact_id: str) -> str:
    """Render artifact forwarding usage hint."""
    return (
        f'Tip: pass "{artifact_id}" directly as an argument'
        " to another tool. Use"
        f' "{artifact_id}:$.path" to pass a specific field'
        f' (e.g. "{artifact_id}:$.items[0].name")'
    )


def _append_select_guidance(
    parts: list[str],
    *,
    artifact_id: str,
    root_path: str,
    fields: list[str],
    code_query_enabled: bool,
    code_query_packages: list[str] | None,
) -> None:
    """Append shared select/query guidance lines."""
    parts.append(_select_call_hint(artifact_id, root_path, fields))
    parts.append(_FILTER_GUIDANCE)
    parts.append(_COUNT_GUIDANCE)
    parts.append(_CURSOR_GUIDANCE)
    code_hint = _code_query_hint(
        artifact_id,
        root_path,
        code_query_enabled=code_query_enabled,
        code_query_packages=code_query_packages,
    )
    if code_hint:
        parts.append(code_hint)
    parts.append(_CONTEXT_GUIDANCE)


def _schema_sort_key(schema: dict[str, Any]) -> tuple[int, str]:
    """Sort schemas by observed records (desc), then path (asc)."""
    coverage = schema.get("coverage")
    observed_records = 0
    if isinstance(coverage, dict):
        raw_count = coverage.get("observed_records")
        if isinstance(raw_count, int):
            observed_records = raw_count
    root_path = schema.get("root_path")
    path_value = root_path if isinstance(root_path, str) else "$"
    return (-observed_records, path_value)


def _build_schema_only_hint(
    *,
    artifact_id: str,
    schemas_raw: list[dict[str, Any]],
    schemas_by_path: dict[str, dict[str, Any]],
    code_query_enabled: bool,
    code_query_packages: list[str] | None,
) -> str | None:
    """Build hint when roots are missing but canonical schemas exist."""
    schema_entries = [
        schema
        for schema in schemas_raw
        if isinstance(schema, dict) and isinstance(schema.get("root_path"), str)
    ]
    if not schema_entries:
        return None

    ordered = sorted(schema_entries, key=_schema_sort_key)
    primary_schema = ordered[0]
    primary_path = str(primary_schema.get("root_path", "$"))
    coverage = primary_schema.get("coverage")
    observed_records = 0
    if isinstance(coverage, dict):
        raw_count = coverage.get("observed_records")
        if isinstance(raw_count, int):
            observed_records = raw_count

    parts: list[str] = []
    if observed_records > 0:
        parts.append(f"Contains {observed_records} records at {primary_path}")
    else:
        parts.append(f"Schema available at {primary_path}")

    fields = _schema_fields_for_root(primary_path, schemas_by_path)[:8]
    if fields:
        parts.append(f"Fields: {', '.join(fields)}")
    _append_select_guidance(
        parts,
        artifact_id=artifact_id,
        root_path=primary_path,
        fields=fields,
        code_query_enabled=code_query_enabled,
        code_query_packages=code_query_packages,
    )
    parts.append(
        "If projection fails for this root shape, use get + jsonpath instead"
    )
    parts.append(_forwarding_hint(artifact_id))
    if len(ordered) > 1:
        alternates = [
            str(item["root_path"])
            for item in ordered[1:4]
            if isinstance(item.get("root_path"), str)
        ]
        if alternates:
            parts.append(f"Also available: {', '.join(alternates)}")
    return ". ".join(parts) + "."


def _mapping_status_hint(
    *,
    artifact_id: str,
    map_status: str,
) -> str | None:
    """Return immediate status-based hint for failed/pending mapping."""
    if map_status == "failed":
        return (
            "Mapping failed. Use "
            f'artifact(action="query", query_kind="get", artifact_id="{artifact_id}", '
            'target="envelope") '
            "to retrieve raw content."
        )
    if map_status not in {"complete", "done", "ready"}:
        return (
            "Mapping in progress. Call "
            f'artifact(action="query", query_kind="describe", artifact_id="{artifact_id}") '
            "to check status later."
        )
    return None


def _build_root_usage_hint(
    *,
    artifact_id: str,
    roots: list[dict[str, Any]],
    schemas_by_path: dict[str, dict[str, Any]],
    code_query_enabled: bool,
    code_query_packages: list[str] | None,
) -> str:
    """Build hint when mapped roots are available."""
    parts: list[str] = []
    primary = roots[0]
    path = primary.get("root_path", "$")
    shape = primary.get("root_shape", "unknown")
    count = primary.get("count_estimate")

    if shape == "array" and isinstance(count, int) and count > 0:
        parts.append(f"Contains {count} records at {path}")
    elif shape in {"dict", "object"}:
        parts.append(f"Contains a dict at {path}")
    else:
        parts.append(f"Root at {path} ({shape})")

    fields = _schema_fields_for_root(path, schemas_by_path)[:8]
    if not fields:
        fields = _field_names(primary)
    if fields:
        parts.append(f"Fields: {', '.join(fields)}")

    sample_indices = primary.get("sample_indices")
    if isinstance(sample_indices, list) and sample_indices:
        sampled_count = primary.get("sampled_record_count")
        if not isinstance(sampled_count, int):
            sampled_count = len(sample_indices)
        if isinstance(count, int) and count > sampled_count:
            parts.append(f"Sampled {sampled_count} of ~{count} records")

    if shape == "array":
        _append_select_guidance(
            parts,
            artifact_id=artifact_id,
            root_path=str(path),
            fields=fields,
            code_query_enabled=code_query_enabled,
            code_query_packages=code_query_packages,
        )
    else:
        parts.append(
            'Use artifact(action="query", '
            'query_kind="get", '
            f'artifact_id="{artifact_id}", '
            'target="envelope"'
            ") to retrieve the full value"
        )

    parts.append(_forwarding_hint(artifact_id))
    if len(roots) > 1:
        alts = [_root_summary(r) for r in roots[1:4]]
        parts.append(f"Also available: {', '.join(alts)}")

    return ". ".join(parts) + "."


def build_usage_hint(
    artifact_id: str,
    describe: dict[str, Any],
    *,
    code_query_enabled: bool = True,
    code_query_packages: list[str] | None = None,
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
        code_query_enabled: Whether ``query_kind=code`` is available
            in the current runtime.
        code_query_packages: Optional list of available third-party
            packages in the code-query runtime.

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

    status_hint = _mapping_status_hint(
        artifact_id=artifact_id,
        map_status=str(map_status),
    )
    if status_hint is not None:
        return status_hint

    if not roots and schemas_by_path and isinstance(schemas_raw, list):
        schema_only_hint = _build_schema_only_hint(
            artifact_id=artifact_id,
            schemas_raw=schemas_raw,
            schemas_by_path=schemas_by_path,
            code_query_enabled=code_query_enabled,
            code_query_packages=code_query_packages,
        )
        if schema_only_hint is not None:
            return schema_only_hint

    if not roots:
        return (
            "No structured mapping available. Use "
            f'artifact(action="query", query_kind="get", artifact_id="{artifact_id}", '
            'target="envelope") '
            "to retrieve raw content."
        )
    return _build_root_usage_hint(
        artifact_id=artifact_id,
        roots=roots,
        schemas_by_path=schemas_by_path,
        code_query_enabled=code_query_enabled,
        code_query_packages=code_query_packages,
    )
