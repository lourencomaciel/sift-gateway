"""Tests for session touch policy enforcement.

These are unit tests that verify the touch policy logic and SQL structure
without requiring a live database. They use a mock connection to track
which SQL statements are executed and verify the correct TouchResult
structures are returned.
"""

from __future__ import annotations

import dataclasses
from typing import Any
from unittest.mock import MagicMock, call, patch

from mcp_artifact_gateway.sessions import (
    TouchResult,
    _TOUCH_ARTIFACT_SQL,
    _UPSERT_ARTIFACT_REF_SQL,
    _UPSERT_SESSION_SQL,
    touch_for_creation,
    touch_for_retrieval,
    touch_for_search,
    upsert_artifact_ref,
    upsert_session,
    touch_artifact,
)


def _make_cursor(rowcount: int = 1) -> MagicMock:
    """Create a standalone mock cursor with the given rowcount."""
    cursor = MagicMock()
    cursor.rowcount = rowcount
    cursor.__enter__ = MagicMock(return_value=cursor)
    cursor.__exit__ = MagicMock(return_value=False)
    return cursor


def _make_mock_conn(*, rowcount: int = 1, touch_rowcount: int | None = None) -> MagicMock:
    """Create a mock connection returning distinct cursors per call.

    Each conn.cursor() call returns a fresh cursor. ``rowcount`` applies to
    session-upsert and artifact-ref-upsert cursors.  ``touch_rowcount``
    (defaults to ``rowcount``) applies to the artifact touch cursor so tests
    can simulate deleted artifacts (rowcount=0) on that specific call.
    """
    if touch_rowcount is None:
        touch_rowcount = rowcount

    conn = MagicMock()
    # First two cursor() calls are session + artifact_ref (rowcount=1 normally).
    # Third call is the artifact touch (variable rowcount).
    cursors = [
        _make_cursor(rowcount),       # upsert_session
        _make_cursor(rowcount),       # upsert_artifact_ref
        _make_cursor(touch_rowcount), # touch_artifact
    ]
    # Extra cursors if more calls happen (e.g., search with multiple refs)
    conn.cursor.side_effect = lambda: cursors.pop(0) if cursors else _make_cursor(rowcount)
    return conn


# ---- SQL structure verification ----

def test_upsert_session_sql_contains_on_conflict() -> None:
    assert "ON CONFLICT" in _UPSERT_SESSION_SQL
    assert "last_seen_at" in _UPSERT_SESSION_SQL


def test_upsert_artifact_ref_sql_contains_on_conflict() -> None:
    assert "ON CONFLICT" in _UPSERT_ARTIFACT_REF_SQL
    assert "last_seen_at" in _UPSERT_ARTIFACT_REF_SQL


def test_touch_artifact_sql_checks_not_deleted() -> None:
    """Touch artifact must only update if deleted_at IS NULL."""
    assert "deleted_at IS NULL" in _TOUCH_ARTIFACT_SQL
    assert "last_referenced_at" in _TOUCH_ARTIFACT_SQL


# ---- TouchResult structure ----

def test_touch_result_is_frozen() -> None:
    result = TouchResult(session_updated=True, artifact_ref_updated=True, artifact_touched=True)
    try:
        result.session_updated = False  # type: ignore[misc]
        raise AssertionError("expected FrozenInstanceError")  # pragma: no cover
    except dataclasses.FrozenInstanceError:
        pass


# ---- touch_for_creation ----

def test_touch_for_creation_touches_all_three() -> None:
    """Creation must touch session, artifact_ref, AND artifact.last_referenced_at."""
    conn = _make_mock_conn(rowcount=1)
    result = touch_for_creation(conn, "sess_1", "art_1")
    assert result.session_updated is True
    assert result.artifact_ref_updated is True
    assert result.artifact_touched is True


def test_touch_for_creation_executes_three_statements() -> None:
    conn = _make_mock_conn(rowcount=1)
    touch_for_creation(conn, "sess_1", "art_1")
    # Three distinct cursors are created (one per SQL statement)
    assert conn.cursor.call_count == 3


# ---- touch_for_retrieval ----

def test_touch_for_retrieval_touches_all_three() -> None:
    """Retrieval touches session + refs + artifact (if not deleted)."""
    conn = _make_mock_conn(rowcount=1)
    result = touch_for_retrieval(conn, "sess_1", "art_1")
    assert result.session_updated is True
    assert result.artifact_ref_updated is True
    assert result.artifact_touched is True


def test_touch_for_retrieval_artifact_not_touched_when_deleted() -> None:
    """When artifact is deleted (rowcount=0 on touch), artifact_touched should be False."""
    conn = _make_mock_conn(rowcount=1, touch_rowcount=0)
    result = touch_for_retrieval(conn, "sess_1", "art_1")
    assert result.session_updated is True
    assert result.artifact_ref_updated is True
    assert result.artifact_touched is False


# ---- touch_for_search ----

def test_touch_for_search_never_touches_artifact() -> None:
    """Search must NEVER touch artifacts.last_referenced_at."""
    conn = _make_mock_conn(rowcount=1)
    result = touch_for_search(conn, "sess_1", ["art_1", "art_2"])
    assert result.session_updated is True
    assert result.artifact_ref_updated is True
    assert result.artifact_touched is False


def test_touch_for_search_does_not_touch_artifact_last_referenced() -> None:
    """Verify touch_for_search does not execute the TOUCH_ARTIFACT SQL."""
    conn = _make_mock_conn(rowcount=1)
    result = touch_for_search(conn, "sess_1", ["art_1"])
    # Search never touches artifact.last_referenced_at
    assert result.artifact_touched is False
    # Only session + artifact_ref calls (2 cursors), no touch_artifact call
    assert conn.cursor.call_count == 2


def test_touch_for_search_empty_artifact_list() -> None:
    conn = _make_mock_conn(rowcount=1)
    result = touch_for_search(conn, "sess_1", [])
    assert result.session_updated is True
    assert result.artifact_ref_updated is False
    assert result.artifact_touched is False


def test_touch_for_search_multiple_artifacts() -> None:
    """Search with multiple artifacts should batch-upsert refs in one query."""
    conn = _make_mock_conn(rowcount=1)
    result = touch_for_search(conn, "sess_1", ["art_1", "art_2", "art_3"])
    assert result.artifact_ref_updated is True
    assert result.artifact_touched is False
    # 1 session upsert + 1 batch artifact_ref upsert = 2 cursor calls
    assert conn.cursor.call_count == 2
