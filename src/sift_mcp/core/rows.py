"""Shared helpers to map DB rows into dictionaries."""

from __future__ import annotations

from collections.abc import Mapping
import logging
from typing import Any

_logger = logging.getLogger(__name__)


def row_to_dict(
    row: tuple[object, ...] | Mapping[str, Any] | None,
    columns: list[str],
) -> dict[str, Any] | None:
    """Convert a DB row to a dict keyed by column name."""
    if row is None:
        return None
    if isinstance(row, Mapping):
        return dict(row)
    if len(row) < len(columns):
        _logger.warning(
            "row has %d values but %d columns expected;"
            " missing columns will be None",
            len(row),
            len(columns),
        )
    return {
        column: row[index] if index < len(row) else None
        for index, column in enumerate(columns)
    }


def rows_to_dicts(
    rows: list[tuple[object, ...] | Mapping[str, Any]],
    columns: list[str],
) -> list[dict[str, Any]]:
    """Convert DB rows to dicts keyed by column name."""
    out: list[dict[str, Any]] = []
    for row in rows:
        mapped = row_to_dict(row, columns)
        if mapped is not None:
            out.append(mapped)
    return out

