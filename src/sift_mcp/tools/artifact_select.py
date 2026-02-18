"""Validate arguments and build responses for ``artifact.select``.

Project and filter artifact data with bounded traversal in two
modes: full (evaluate root_path against the complete envelope) and
sampled-only (iterate pre-materialised sample rows).  Both modes
honour the traversal_v1 determinism contract.  Exports
``validate_select_args``, ``build_select_result``, and fetch SQL.

Typical usage example::

    error = validate_select_args(arguments)
    if error:
        return error
    result = build_select_result(items=items, truncated=False)
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
import re
from typing import Any, Literal

from sift_mcp.pagination.contract import (
    build_retrieval_pagination_meta,
)

# Search-mode order_by values (no spaces, no parentheses).
_SEARCH_ORDER_BY_VALUES = frozenset(
    {
        "created_seq_desc",
        "last_seen_desc",
        "chain_seq_asc",
    }
)

# Pattern for select-style order_by: "field [ASC|DESC]" or
# "to_number(field) [ASC|DESC]".
_SELECT_ORDER_RE = re.compile(
    r"^(?:(?P<cast>to_number|to_string)\((?P<cf>[^)]+)\)"
    r"|(?P<field>\S+))"
    r"(?:\s+(?P<dir>ASC|DESC))?$",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class SelectOrderBy:
    """Parsed select-style order_by specification.

    Attributes:
        field: Field name to sort by (relative, no ``$``).
        direction: Sort direction, ``"ASC"`` or ``"DESC"``.
        cast: Optional cast function (``"to_number"`` or
            ``"to_string"``), or ``None`` for no cast.
    """

    field: str
    direction: Literal["ASC", "DESC"]
    cast: Literal["to_number", "to_string"] | None


def parse_select_order_by(raw: str) -> SelectOrderBy | None:
    """Parse a select-style ``order_by`` string.

    Recognizes formats like ``"spend DESC"``,
    ``"to_number(spend) DESC"``, ``"name"`` (default ASC).
    Returns ``None`` for search-mode values like
    ``"created_seq_desc"``.

    Args:
        raw: Raw order_by string from tool arguments.

    Returns:
        Parsed ``SelectOrderBy``, or ``None`` if the value
        is a search-mode constant or unparseable.
    """
    stripped = raw.strip()
    if not stripped:
        return None
    if stripped.lower() in _SEARCH_ORDER_BY_VALUES:
        return None
    m = _SELECT_ORDER_RE.match(stripped)
    if m is None:
        return None
    cast_fn = m.group("cast")
    cast_field = m.group("cf")
    plain_field = m.group("field")
    direction_raw = m.group("dir")
    field = (cast_field or plain_field or "").strip()
    if not field:
        return None
    direction: Literal["ASC", "DESC"] = "ASC"
    if direction_raw and direction_raw.upper() == "DESC":
        direction = "DESC"
    cast: Literal["to_number", "to_string"] | None = None
    if cast_fn:
        lower = cast_fn.lower()
        if lower == "to_number":
            cast = "to_number"
        elif lower == "to_string":
            cast = "to_string"
    return SelectOrderBy(field=field, direction=direction, cast=cast)


def validate_select_order_by(raw: str) -> dict[str, Any] | None:
    """Validate a select-style ``order_by`` string.

    Args:
        raw: Raw order_by string from tool arguments.

    Returns:
        Error dict if invalid, ``None`` if valid or a
        search-mode constant.
    """
    stripped = raw.strip()
    if not stripped:
        return None
    if stripped.lower() in _SEARCH_ORDER_BY_VALUES:
        return None
    parsed = parse_select_order_by(stripped)
    if parsed is None:
        return {
            "code": "INVALID_ARGUMENT",
            "message": (
                f"invalid order_by: {raw!r}. "
                "Use 'field [ASC|DESC]' or "
                "'to_number(field) [ASC|DESC]'."
            ),
        }
    return None


def _sort_key_for_item(
    item: dict[str, Any],
    field: str,
    cast: Literal["to_number", "to_string"] | None,
) -> tuple[int, Any]:
    """Build a sort key for a projected item.

    Returns ``(0, value)`` for valid values and
    ``(1, "")`` for missing/None/unconvertible, which
    sorts them last regardless of direction.

    Args:
        item: Projected item dict with a ``"projection"`` key.
        field: Field name to extract from projection.
        cast: Optional cast to apply.

    Returns:
        Sort key tuple.
    """
    projection = item.get("projection")
    if not isinstance(projection, dict):
        return (1, "")
    # Projection keys are canonical JSONPaths (e.g. "$.spend").
    # Try the canonical form first, then the bare field name.
    canonical_key = f"$.{field}"
    if canonical_key in projection:
        value = projection[canonical_key]
    elif field in projection:
        value = projection[field]
    else:
        return (1, "")
    if value is None:
        return (1, "")
    if cast == "to_number":
        try:
            return (0, float(value))
        except (ValueError, TypeError):
            return (1, "")
    if cast == "to_string":
        return (0, str(value))
    return (0, value)


def _apply_select_sort(
    items: list[dict[str, Any]],
    order: SelectOrderBy,
) -> list[dict[str, Any]]:
    """Sort items by a select-style order_by specification.

    None/missing values always sort last regardless of direction.

    Args:
        items: List of projected item dicts.
        order: Parsed sort specification.

    Returns:
        New sorted list.
    """
    valid: list[tuple[Any, dict[str, Any]]] = []
    missing: list[dict[str, Any]] = []
    for item in items:
        priority, value = _sort_key_for_item(item, order.field, order.cast)
        if priority == 0:
            valid.append((value, item))
        else:
            missing.append(item)
    try:
        valid.sort(
            key=lambda pair: pair[0],
            reverse=(order.direction == "DESC"),
        )
    except TypeError:
        # Mixed types across rows (e.g. int vs str); fall back
        # to string-coerced comparison for deterministic output.
        valid.sort(
            key=lambda pair: str(pair[0]),
            reverse=(order.direction == "DESC"),
        )
    return [item for _, item in valid] + missing


def validate_select_args(arguments: dict[str, Any]) -> dict[str, Any] | None:
    """Validate ``artifact.select`` arguments.

    Checks for required gateway context, ``artifact_id``,
    ``root_path``, and a non-empty ``select_paths`` list with
    relative (non-``$``) paths.

    When a ``cursor`` is present, ``root_path``, ``select_paths``,
    and ``where`` are optional — they will be extracted from the
    cursor payload by the handler.

    Args:
        arguments: Raw tool arguments.

    Returns:
        Error dict on validation failure, ``None`` when valid.
    """
    ctx = arguments.get("_gateway_context")
    if not isinstance(ctx, dict) or not ctx.get("session_id"):
        return {
            "code": "INVALID_ARGUMENT",
            "message": "missing _gateway_context.session_id",
        }

    if not arguments.get("artifact_id"):
        return {"code": "INVALID_ARGUMENT", "message": "missing artifact_id"}

    has_cursor = isinstance(arguments.get("cursor"), str) and bool(
        arguments["cursor"]
    )
    count_only = arguments.get("count_only") is True

    if not arguments.get("root_path") and not has_cursor:
        return {"code": "INVALID_ARGUMENT", "message": "missing root_path"}

    select_paths = arguments.get("select_paths")
    if (
        not has_cursor
        and not count_only
        and (not isinstance(select_paths, list) or not select_paths)
    ):
        return {
            "code": "INVALID_ARGUMENT",
            "message": "select_paths must be a non-empty list",
        }

    # Validate individual select_paths entries.
    if isinstance(select_paths, list):
        for path in select_paths:
            if not isinstance(path, str):
                continue
            if path == "*":
                return {
                    "code": "INVALID_ARGUMENT",
                    "message": (
                        "Wildcard '*' is not supported in "
                        "select_paths. Use explicit field names "
                        "(e.g. ['ad_name', 'spend']). Run "
                        "artifact(action='query', "
                        "artifact_id='...') to see "
                        "available fields."
                    ),
                }
            if path.startswith("$"):
                return {
                    "code": "INVALID_ARGUMENT",
                    "message": (f"select_path must be relative (no $): {path}"),
                }

    return None


# SQL for fetching root info
FETCH_ROOT_SQL = """
SELECT root_key, root_path, count_estimate, root_shape,
       fields_top, sample_indices, root_summary
FROM artifact_roots
WHERE workspace_id = %s AND artifact_id = %s AND root_path = %s
"""

# SQL for fetching samples for a root
FETCH_SAMPLES_SQL = """
SELECT sample_index, record, record_bytes, record_hash
FROM artifact_samples
WHERE workspace_id = %s AND artifact_id = %s AND root_key = %s
ORDER BY sample_index ASC
"""


def sampled_indices_ascending(
    sample_rows: Sequence[dict[str, Any]],
) -> list[int]:
    """Extract sample indices in ascending order from sample rows.

    Enforces the traversal_v1 contract: sampled indices are
    always enumerated in ascending order.

    Args:
        sample_rows: Sequence of sample row dicts, each
            containing a ``sample_index`` key.

    Returns:
        Sorted list of integer sample indices.
    """
    return sorted(
        int(idx)
        for row in sample_rows
        if isinstance((idx := row.get("sample_index")), int)
    )


def build_select_result(
    *,
    items: list[dict[str, Any]],
    truncated: bool,
    cursor: str | None,
    total_matched: int | None = None,
    sampled_only: bool = False,
    sample_indices_used: list[int] | None = None,
    sampled_prefix_len: int | None = None,
    omitted: dict[str, Any] | None = None,
    stats: dict[str, Any] | None = None,
    determinism: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Build the ``artifact.select`` response dict.

    Args:
        items: Projected records matching the select criteria.
        truncated: Whether the result set was truncated by
            budget limits.
        cursor: Opaque pagination cursor, or ``None``.
        total_matched: Total number of records that passed
            the where filter, before pagination/truncation.
        sampled_only: Whether results come from partial
            (sampled) data rather than the full envelope.
        sample_indices_used: Indices of samples that
            contributed to the result.
        sampled_prefix_len: Length of the contiguous prefix
            of sampled records.
        omitted: Dict describing omitted fields or records.
        stats: Traversal statistics dict.
        determinism: Dict with determinism contract metadata.

    Returns:
        Structured response dict for the ``artifact.select``
        tool.
    """
    result: dict[str, Any] = {
        "items": items,
        "truncated": truncated,
        "pagination": build_retrieval_pagination_meta(
            truncated=truncated,
            cursor=cursor if cursor else None,
        ),
    }
    if total_matched is not None:
        result["total_matched"] = total_matched
    if cursor:
        result["cursor"] = cursor
    if omitted:
        result["omitted"] = omitted
    if stats:
        result["stats"] = stats
    if sampled_only:
        result["sampled_only"] = True
        if sample_indices_used is not None:
            result["sample_indices_used"] = sample_indices_used
        if sampled_prefix_len is not None:
            result["sampled_prefix_len"] = sampled_prefix_len
    if determinism:
        result["determinism"] = determinism
    return result
