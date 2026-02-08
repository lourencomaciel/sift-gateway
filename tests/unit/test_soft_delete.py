from __future__ import annotations

from mcp_artifact_gateway.constants import WORKSPACE_ID
from mcp_artifact_gateway.jobs.soft_delete import (
    SOFT_DELETE_BATCH_SQL,
    SOFT_DELETE_UNREFERENCED_SQL,
    run_soft_delete_expired,
    run_soft_delete_unreferenced,
    soft_delete_expired_params,
    soft_delete_unreferenced_params,
)
from mcp_artifact_gateway.obs.metrics import GatewayMetrics


class _FakeCursor:
    def __init__(self, rows: list[tuple[object, ...]]) -> None:
        self._rows = rows

    def fetchall(self) -> list[tuple[object, ...]]:
        return list(self._rows)


class _FakeConnection:
    def __init__(
        self,
        *,
        rows_sequence: list[list[tuple[object, ...]]],
        fail_on_call: int | None = None,
    ) -> None:
        self._rows_sequence = list(rows_sequence)
        self.fail_on_call = fail_on_call
        self.calls = 0
        self.committed = False
        self.rolled_back = False

    def execute(self, _query: str, _params: tuple[object, ...] | None = None) -> _FakeCursor:
        self.calls += 1
        if self.fail_on_call is not None and self.calls == self.fail_on_call:
            raise RuntimeError("simulated execute failure")
        if not self._rows_sequence:
            return _FakeCursor([])
        return _FakeCursor(self._rows_sequence.pop(0))

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        self.rolled_back = True


def test_soft_delete_batch_sql_contains_skip_locked() -> None:
    assert "SKIP LOCKED" in SOFT_DELETE_BATCH_SQL


def test_soft_delete_batch_sql_rechecks_predicates() -> None:
    """The UPDATE WHERE clause (after FROM candidates) must recheck predicates."""
    # Split on "FROM candidates" to isolate the outer WHERE clause
    parts = SOFT_DELETE_BATCH_SQL.split("FROM candidates")
    assert len(parts) == 2, "Expected exactly one FROM candidates clause"
    outer_where = parts[1]
    assert "a.deleted_at IS NULL" in outer_where
    assert "a.expires_at IS NOT NULL" in outer_where
    assert "a.expires_at <= NOW()" in outer_where


def test_soft_delete_unreferenced_sql_contains_skip_locked() -> None:
    assert "SKIP LOCKED" in SOFT_DELETE_UNREFERENCED_SQL


def test_soft_delete_expired_params_returns_correct_tuple() -> None:
    params = soft_delete_expired_params(batch_size=200)
    assert params == (WORKSPACE_ID, 200, WORKSPACE_ID)


def test_soft_delete_expired_params_default_batch_size() -> None:
    params = soft_delete_expired_params()
    assert params == (WORKSPACE_ID, 100, WORKSPACE_ID)


def test_soft_delete_unreferenced_params_returns_correct_tuple() -> None:
    ts = "2025-01-01T00:00:00Z"
    params = soft_delete_unreferenced_params(ts, batch_size=50)
    assert params == (WORKSPACE_ID, ts, 50, WORKSPACE_ID)


def test_soft_delete_unreferenced_params_default_batch_size() -> None:
    ts = "2025-06-15T12:00:00Z"
    params = soft_delete_unreferenced_params(ts)
    assert params == (WORKSPACE_ID, ts, 100, WORKSPACE_ID)


def test_run_soft_delete_expired_returns_deleted_artifacts() -> None:
    connection = _FakeConnection(rows_sequence=[[("art_1",), ("art_2",)]])
    result = run_soft_delete_expired(connection, batch_size=2)
    assert result.deleted_count == 2
    assert result.artifact_ids == ["art_1", "art_2"]
    assert connection.committed is True
    assert connection.rolled_back is False


def test_run_soft_delete_unreferenced_returns_deleted_artifacts() -> None:
    connection = _FakeConnection(rows_sequence=[[("art_9",)]])
    result = run_soft_delete_unreferenced(
        connection,
        threshold_timestamp="2025-01-01T00:00:00Z",
        batch_size=10,
    )
    assert result.deleted_count == 1
    assert result.artifact_ids == ["art_9"]
    assert connection.committed is True
    assert connection.rolled_back is False


def test_run_soft_delete_updates_metrics() -> None:
    connection = _FakeConnection(rows_sequence=[[("art_1",), ("art_2",)]])
    metrics = GatewayMetrics()
    result = run_soft_delete_expired(connection, batch_size=2, metrics=metrics)
    assert result.deleted_count == 2
    assert metrics.prune_soft_deletes.value == 2


def test_run_soft_delete_rolls_back_on_error() -> None:
    connection = _FakeConnection(rows_sequence=[[]], fail_on_call=1)
    try:
        run_soft_delete_expired(connection, batch_size=1)
    except RuntimeError as exc:
        assert "simulated execute failure" in str(exc)
    else:
        raise AssertionError("expected RuntimeError")
    assert connection.committed is False
    assert connection.rolled_back is True


def test_run_soft_delete_unreferenced_updates_metrics() -> None:
    """run_soft_delete_unreferenced correctly updates prune_soft_deletes metric."""
    connection = _FakeConnection(rows_sequence=[[("art_a",), ("art_b",), ("art_c",)]])
    metrics = GatewayMetrics()
    result = run_soft_delete_unreferenced(
        connection,
        threshold_timestamp="2025-01-01T00:00:00Z",
        batch_size=10,
        metrics=metrics,
    )
    assert result.deleted_count == 3
    assert metrics.prune_soft_deletes.value == 3
