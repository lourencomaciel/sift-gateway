"""Structured filters compiled to parameterized SQLite JSON queries.

Define ``Filter``, ``FilterGroup``, and ``FilterNot`` dataclasses
for type-safe query construction, and compile them to parameterized
SQL WHERE clauses using SQLite ``json_extract``.  LLM clients
produce JSON natively — no parsing needed.

Supported operators:
    eq, ne, gt, gte, lt, lte — standard comparison
    in — value membership in list
    contains — substring match (cast to text)
    array_contains — JSON array element membership
    exists, not_exists — field presence checks

Logical combinators:
    FilterGroup — AND / OR over a list of children
    FilterNot — negate any child filter
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from sift_mcp.canon.rfc8785 import canonical_bytes
from sift_mcp.query.jsonpath import reject_wildcards
from sift_mcp.util.hashing import sha256_hex

# ── Operator tables ──────────────────────────────────────────────

_COMPARISON_OPS: dict[str, str] = {
    "eq": "=",
    "ne": "!=",
    "gt": ">",
    "gte": ">=",
    "lt": "<",
    "lte": "<=",
}

_ALL_OPS = frozenset(
    [*_COMPARISON_OPS, "in", "contains", "array_contains",
     "exists", "not_exists"]
)


# ── Data model ───────────────────────────────────────────────────

@dataclass(frozen=True)
class Filter:
    """Single field-level filter predicate.

    Attributes:
        path: JSONPath to the field (e.g. ``"$.status"``).
        op: Comparison operator name.
        value: Comparison value (ignored for ``exists`` /
            ``not_exists``).
    """

    path: str
    op: str
    value: Any = None

    def __post_init__(self) -> None:
        """Validate operator name."""
        if self.op not in _ALL_OPS:
            msg = f"unsupported filter operator: {self.op!r}"
            raise ValueError(msg)


@dataclass(frozen=True)
class FilterGroup:
    """Logical group of filters with AND/OR semantics.

    Attributes:
        logic: ``"and"`` or ``"or"``.
        filters: Child filters or nested groups.
    """

    logic: str
    filters: list[Filter | FilterGroup | FilterNot] = field(
        default_factory=list,
    )

    def __post_init__(self) -> None:
        """Validate logic keyword."""
        if self.logic not in ("and", "or"):
            msg = f"unsupported logic: {self.logic!r}"
            raise ValueError(msg)


@dataclass(frozen=True)
class FilterNot:
    """Negate a child filter or group.

    Attributes:
        child: The filter or group to negate.
    """

    child: Filter | FilterGroup | FilterNot


# ── SQL compilation ──────────────────────────────────────────────

def compile_filter(
    f: Filter | FilterGroup | FilterNot,
) -> tuple[str, list[Any]]:
    """Compile a filter tree to a parameterized SQL WHERE fragment.

    Args:
        f: Filter, FilterGroup, or FilterNot to compile.

    Returns:
        Tuple of ``(sql_fragment, params)`` where *sql_fragment*
        uses ``?`` placeholders and *params* is the corresponding
        bind-parameter list.

    Raises:
        ValueError: If the filter contains an unsupported
            operator or empty ``in`` list.
    """
    if isinstance(f, FilterNot):
        child_sql, child_params = compile_filter(f.child)
        return f"NOT ({child_sql})", child_params
    if isinstance(f, FilterGroup):
        return _compile_group(f)
    return _compile_predicate(f)


def _compile_group(group: FilterGroup) -> tuple[str, list[Any]]:
    """Compile a logical AND/OR group."""
    if not group.filters:
        return ("1", []) if group.logic == "and" else ("0", [])

    parts: list[str] = []
    params: list[Any] = []
    for child in group.filters:
        sql, p = compile_filter(child)
        parts.append(f"({sql})")
        params.extend(p)

    joiner = " AND " if group.logic == "and" else " OR "
    return joiner.join(parts), params


def _compile_comparison_predicate(
    *,
    path: str,
    op: str,
    value: Any,
) -> tuple[str, list[Any]]:
    """Compile comparison operators including SQL NULL semantics."""
    # SQL NULL semantics: = NULL is always NULL (falsy).
    # Use IS NULL / IS NOT NULL for correct null matching.
    if value is None:
        if op == "eq":
            return "json_extract(record, ?) IS NULL", [path]
        if op == "ne":
            return "json_extract(record, ?) IS NOT NULL", [path]
        msg = f"NULL value not supported for operator {op!r}"
        raise ValueError(msg)

    sql_op = _COMPARISON_OPS[op]
    return (
        f"json_extract(record, ?) {sql_op} ?",
        [path, _sql_value(value)],
    )


def _compile_in_predicate(
    *,
    path: str,
    values: Any,
) -> tuple[str, list[Any]]:
    """Compile IN list membership."""
    if not isinstance(values, (list, tuple)):
        msg = "'in' operator requires a list value"
        raise ValueError(msg)
    if not values:
        return "0", []
    placeholders = ", ".join("?" for _ in values)
    return (
        f"json_extract(record, ?) IN ({placeholders})",
        [path, *[_sql_value(v) for v in values]],
    )


def _compile_contains_predicate(
    *,
    path: str,
    value: Any,
) -> tuple[str, list[Any]]:
    """Compile escaped substring match."""
    escaped = (
        str(value)
        .replace("\\", "\\\\")
        .replace("%", "\\%")
        .replace("_", "\\_")
    )
    return (
        "CAST(json_extract(record, ?) AS TEXT)"
        " LIKE '%' || ? || '%' ESCAPE '\\'",
        [path, escaped],
    )


def _compile_array_contains_predicate(
    *,
    path: str,
    value: Any,
) -> tuple[str, list[Any]]:
    """Compile guarded array membership predicate."""
    # Guard: only iterate when the target is a JSON array;
    # non-array values (scalars, objects) are treated as
    # non-matches instead of raising OperationalError.
    return (
        "(json_type(record, ?) = 'array'"
        " AND EXISTS (SELECT 1 FROM json_each("
        "json_extract(record, ?)) WHERE value = ?))",
        [path, path, _sql_value(value)],
    )


def _compile_predicate(f: Filter) -> tuple[str, list[Any]]:
    """Compile a single filter predicate to SQL."""
    if f.op in _COMPARISON_OPS:
        return _compile_comparison_predicate(
            path=f.path,
            op=f.op,
            value=f.value,
        )

    if f.op == "in":
        return _compile_in_predicate(
            path=f.path,
            values=f.value,
        )

    if f.op == "contains":
        return _compile_contains_predicate(
            path=f.path,
            value=f.value,
        )

    if f.op == "array_contains":
        return _compile_array_contains_predicate(
            path=f.path,
            value=f.value,
        )

    if f.op == "exists":
        return "json_type(record, ?) IS NOT NULL", [f.path]

    # not_exists
    return "json_type(record, ?) IS NULL", [f.path]


def _sql_value(value: Any) -> Any:
    """Coerce a Python value to a SQLite-compatible parameter.

    Args:
        value: Raw Python value from a filter.

    Returns:
        SQLite-safe value (bool→int, Decimal→float,
        passthrough otherwise).
    """
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, Decimal):
        return float(value)
    return value


# ── Dict parsing ────────────────────────────────────────────────

_MAX_FILTER_DEPTH = 20


def parse_filter_dict(
    raw: dict[str, Any],
    *,
    _depth: int = 0,
) -> Filter | FilterGroup | FilterNot:
    """Convert a raw dict to a ``Filter`` or ``FilterGroup``.

    Accepts the structured filter format produced by LLM clients:

    - ``{"path": "$.x", "op": "eq", "value": 1}`` → ``Filter``
    - ``{"logic": "and", "filters": [...]}`` → ``FilterGroup``
    - ``{"not": {...}}`` → ``FilterNot``

    Args:
        raw: Dictionary with filter fields.
        _depth: Internal recursion depth counter (callers should
            not set).

    Returns:
        A ``Filter`` or ``FilterGroup`` instance.

    Raises:
        ValueError: If required keys are missing, values are
            invalid, or nesting exceeds ``_MAX_FILTER_DEPTH``.
    """
    if _depth > _MAX_FILTER_DEPTH:
        msg = (
            f"filter nesting exceeds maximum depth"
            f" ({_MAX_FILTER_DEPTH})"
        )
        raise ValueError(msg)
    if "not" in raw:
        child_raw = raw["not"]
        if not isinstance(child_raw, dict):
            msg = "FilterNot requires a dict child"
            raise ValueError(msg)
        child = parse_filter_dict(child_raw, _depth=_depth + 1)
        return FilterNot(child=child)
    if "logic" in raw:
        children_raw = raw.get("filters")
        if not isinstance(children_raw, list):
            msg = "FilterGroup requires a 'filters' list"
            raise ValueError(msg)
        children: list[Filter | FilterGroup | FilterNot] = [
            parse_filter_dict(c, _depth=_depth + 1)
            for c in children_raw
        ]
        return FilterGroup(logic=str(raw["logic"]), filters=children)
    if "path" not in raw or "op" not in raw:
        msg = "Filter requires 'path' and 'op' keys"
        raise ValueError(msg)
    path = str(raw["path"])
    _validate_filter_path(path)
    value = raw.get("value")
    _validate_filter_value(value)
    return Filter(
        path=path,
        op=str(raw["op"]),
        value=value,
    )


def _validate_filter_path(path: str) -> None:
    """Reject filter paths that SQLite ``json_extract`` cannot handle.

    Validates the path is a syntactically correct JSONPath and
    rejects wildcards (``[*]``) which are not supported by SQLite.

    Args:
        path: JSONPath string from filter input.

    Raises:
        ValueError: If the path is malformed or contains wildcards.
    """
    reject_wildcards(path, context="filter")


_SCALAR_TYPES = (str, int, float, bool, type(None))


def _validate_filter_value(value: Any) -> None:
    """Reject filter values that SQLite cannot handle.

    Accepts scalars (str, int, float, bool, None) and flat
    lists of scalars (for the ``in`` operator).

    Args:
        value: Raw filter value from client input.

    Raises:
        TypeError: If value is a dict, nested list, or other
            unsupported type.
    """
    if isinstance(value, _SCALAR_TYPES):
        return
    if isinstance(value, (list, tuple)):
        for item in value:
            if not isinstance(item, _SCALAR_TYPES):
                msg = (
                    f"filter list items must be scalars,"
                    f" got {type(item).__name__}"
                )
                raise TypeError(msg)
        return
    msg = f"unsupported filter value type: {type(value).__name__}"
    raise TypeError(msg)


# ── Filter hashing ───────────────────────────────────────────────

def filter_hash(f: Filter | FilterGroup | FilterNot) -> str:
    """Compute SHA-256 hex digest of the canonical filter.

    Used for cursor binding — if the filter changes between
    paginated requests the cursor is invalidated.

    Args:
        f: Filter or FilterGroup to hash.

    Returns:
        64-character lowercase hex digest string.
    """
    return sha256_hex(canonical_bytes(_to_canonical(f)))


def _to_canonical(
    f: Filter | FilterGroup | FilterNot,
) -> dict[str, Any]:
    """Convert a filter tree to a canonical dict for hashing.

    ``FilterGroup.filters`` are sorted by their canonical byte
    representation so that semantically equivalent groups
    produce the same hash regardless of input ordering.
    """
    if isinstance(f, FilterNot):
        return {"not": _to_canonical(f.child)}
    if isinstance(f, FilterGroup):
        children = [_to_canonical(c) for c in f.filters]
        children.sort(key=canonical_bytes)
        return {"filters": children, "logic": f.logic}
    result: dict[str, Any] = {"op": f.op, "path": f.path}
    if f.op not in ("exists", "not_exists"):
        result["value"] = _hash_safe_value(f.value)
    return result


def _hash_safe_value(value: Any) -> Any:
    """Coerce a value for RFC 8785 canonical encoding.

    Python floats are rejected by ``canonical_bytes``, so
    convert them to ``int`` (if lossless) or ``Decimal``.
    Lists are recursed.
    """
    if isinstance(value, float):
        int_val = int(value)
        if float(int_val) == value:
            return int_val
        return Decimal(str(value))
    if isinstance(value, (list, tuple)):
        return [_hash_safe_value(v) for v in value]
    return value
