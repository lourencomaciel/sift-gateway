"""Compile select paths to SQLite JSON projection SQL.

Build a ``json_object(...)`` expression that projects selected
JSONPaths from the ``record`` column of ``artifact_records``,
returning a single JSON value containing only the requested
fields.
"""

from __future__ import annotations

from sift_gateway.query.jsonpath import reject_wildcards


def compile_select(
    select_paths: list[str],
) -> tuple[str, list[str]]:
    """Compile select paths to a SQL projection expression.

    When *select_paths* is empty the raw ``record`` column is
    returned as-is (no projection).  Wildcard paths (``[*]``)
    are rejected because SQLite ``json_extract`` does not
    support them.

    Args:
        select_paths: Canonical JSONPath strings to project
            (e.g. ``["$.name", "$.age"]``).

    Returns:
        Tuple of ``(sql_expr, params)`` where *sql_expr* is
        a ``json_object(...)`` call with ``?`` placeholders
        and *params* provides the bind values.

    Raises:
        ValueError: If any path contains a wildcard segment.

    Example::

        >>> compile_select(["$.name", "$.age"])
        ("json_object(?, json_extract(record, ?), "
         "?, json_extract(record, ?))",
         ["$.name", "$.name", "$.age", "$.age"])
    """
    if not select_paths:
        return "record", []

    for path in select_paths:
        reject_wildcards(path, context="SQL projection")

    fragments: list[str] = []
    params: list[str] = []
    for path in select_paths:
        fragments.append("?, json_extract(record, ?)")
        params.extend([path, path])

    return f"json_object({', '.join(fragments)})", params
