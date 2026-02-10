"""SQL dialect adaptation helpers for Postgres/SQLite portability."""

from __future__ import annotations

import json as _json
import re

from mcp_artifact_gateway.db.backend import Dialect


def rewrite_param_markers(sql: str, dialect: Dialect) -> str:
    """Replace %s parameter markers with ? for SQLite."""
    if dialect is Dialect.POSTGRES:
        return sql
    return sql.replace("%s", "?")


def rewrite_now(sql: str, dialect: Dialect) -> str:
    """Replace NOW() with datetime('now') for SQLite."""
    if dialect is Dialect.POSTGRES:
        return sql
    return re.sub(r"\bNOW\(\)", "datetime('now')", sql, flags=re.IGNORECASE)


def strip_skip_locked(sql: str) -> str:
    """Remove FOR UPDATE SKIP LOCKED (unsupported in SQLite)."""
    return re.sub(
        r"\s+FOR\s+UPDATE\s+SKIP\s+LOCKED", "", sql, flags=re.IGNORECASE
    )


def expand_any_clause(
    sql: str,
    params: tuple[object, ...],
    *,
    any_param_index: int,
) -> tuple[str, tuple[object, ...]]:
    """Expand = ANY(%s) with array param into IN (?, ?, ...) with flat params.

    The parameter at *any_param_index* must be a list/tuple. All other params
    and ``%s`` markers are rewritten to ``?`` style.
    """
    values = params[any_param_index]
    if not isinstance(values, (list, tuple)):
        msg = (
            f"param at index {any_param_index} "
            f"must be a list, got {type(values)}"
        )
        raise TypeError(msg)

    placeholders = ", ".join("?" for _ in values)
    sql = re.sub(r"=\s*ANY\(\s*%s\s*\)", f"IN ({placeholders})", sql, count=1)
    sql = sql.replace("%s", "?")

    flat: list[object] = []
    for i, param in enumerate(params):
        if i == any_param_index:
            flat.extend(values)
        else:
            flat.append(param)
    return sql, tuple(flat)


def adapt_params(
    sql: str,
    params: tuple[object, ...],
    dialect: Dialect,
) -> tuple[str, tuple[object, ...]]:
    """Apply all dialect transforms to a simple query."""
    sql = rewrite_now(sql, dialect)
    sql = rewrite_param_markers(sql, dialect)
    return sql, params


def wrap_json(value: object | None, dialect: Dialect) -> object | None:
    """Wrap a dict/list for insertion into a JSON column.

    Postgres needs ``psycopg.types.json.Jsonb``; SQLite takes a JSON string.
    """
    if value is None:
        return None
    if dialect is Dialect.POSTGRES:
        from psycopg.types.json import Jsonb

        return Jsonb(value)
    return _json.dumps(value, ensure_ascii=False, sort_keys=True)
