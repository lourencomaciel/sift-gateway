"""Tests for db/repos/quota_repo.py — storage usage queries."""

from __future__ import annotations

from mcp_artifact_gateway.constants import WORKSPACE_ID
from mcp_artifact_gateway.db.repos.quota_repo import (
    STORAGE_USAGE_SQL,
    StorageUsage,
    parse_storage_usage,
    query_storage_usage,
    storage_usage_params,
)


class _FakeCursor:
    def __init__(self, row: tuple[object, ...] | None) -> None:
        self._row = row

    def fetchone(self) -> tuple[object, ...] | None:
        return self._row


class _FakeConnection:
    def __init__(self, row: tuple[object, ...] | None = None) -> None:
        self._row = row
        self.executed_sql: str | None = None
        self.executed_params: tuple[object, ...] | None = None

    def execute(
        self, query: str, params: tuple[object, ...] | None = None
    ) -> _FakeCursor:
        self.executed_sql = query
        self.executed_params = params
        return _FakeCursor(self._row)


# ---------------------------------------------------------------------------
# SQL shape
# ---------------------------------------------------------------------------
def test_storage_usage_sql_selects_non_deleted() -> None:
    assert "deleted_at IS NULL" in STORAGE_USAGE_SQL


def test_storage_usage_sql_uses_coalesce() -> None:
    assert "COALESCE" in STORAGE_USAGE_SQL


def test_storage_usage_sql_sums_payload_total_bytes() -> None:
    assert "payload_total_bytes" in STORAGE_USAGE_SQL


def test_storage_usage_params_returns_workspace_id() -> None:
    assert storage_usage_params() == (WORKSPACE_ID,)


# ---------------------------------------------------------------------------
# StorageUsage dataclass
# ---------------------------------------------------------------------------
def test_storage_usage_total_bytes_property() -> None:
    usage = StorageUsage(total_payload_bytes=1000, total_binary_bytes=500, artifact_count=3)
    assert usage.total_bytes == 1000


def test_storage_usage_frozen() -> None:
    usage = StorageUsage(total_payload_bytes=0, total_binary_bytes=0, artifact_count=0)
    try:
        usage.total_payload_bytes = 999  # type: ignore[misc]
        raise AssertionError("expected FrozenInstanceError")
    except AttributeError:
        pass


# ---------------------------------------------------------------------------
# parse_storage_usage
# ---------------------------------------------------------------------------
def test_parse_storage_usage_normal_row() -> None:
    row = (5000, 2000, 10)
    usage = parse_storage_usage(row)
    assert usage.total_payload_bytes == 5000
    assert usage.total_binary_bytes == 2000
    assert usage.artifact_count == 10


def test_parse_storage_usage_none_row() -> None:
    usage = parse_storage_usage(None)
    assert usage.total_payload_bytes == 0
    assert usage.total_binary_bytes == 0
    assert usage.artifact_count == 0


def test_parse_storage_usage_short_row() -> None:
    usage = parse_storage_usage((100,))
    assert usage.total_payload_bytes == 0
    assert usage.artifact_count == 0


def test_parse_storage_usage_non_numeric_values() -> None:
    row = ("not_a_number", None, "bad")
    usage = parse_storage_usage(row)
    assert usage.total_payload_bytes == 0
    assert usage.total_binary_bytes == 0
    assert usage.artifact_count == 0


def test_parse_storage_usage_float_values() -> None:
    row = (1000.5, 500.0, 3.0)
    usage = parse_storage_usage(row)
    assert usage.total_payload_bytes == 1000
    assert usage.total_binary_bytes == 500
    assert usage.artifact_count == 3


def test_parse_storage_usage_zero_values() -> None:
    row = (0, 0, 0)
    usage = parse_storage_usage(row)
    assert usage.total_payload_bytes == 0
    assert usage.total_binary_bytes == 0
    assert usage.artifact_count == 0


# ---------------------------------------------------------------------------
# query_storage_usage
# ---------------------------------------------------------------------------
def test_query_storage_usage_executes_correct_sql() -> None:
    conn = _FakeConnection(row=(5000, 2000, 10))
    usage = query_storage_usage(conn)
    assert conn.executed_sql == STORAGE_USAGE_SQL
    assert conn.executed_params == (WORKSPACE_ID,)
    assert usage.total_payload_bytes == 5000
    assert usage.artifact_count == 10


def test_query_storage_usage_handles_empty_db() -> None:
    conn = _FakeConnection(row=(0, 0, 0))
    usage = query_storage_usage(conn)
    assert usage.total_bytes == 0
    assert usage.artifact_count == 0


def test_query_storage_usage_handles_none_row() -> None:
    conn = _FakeConnection(row=None)
    usage = query_storage_usage(conn)
    assert usage.total_bytes == 0
