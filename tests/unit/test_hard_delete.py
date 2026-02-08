from __future__ import annotations

from mcp_artifact_gateway.constants import WORKSPACE_ID
from mcp_artifact_gateway.jobs.hard_delete import (
    DELETE_ARTIFACT_SQL,
    DELETE_BLOB_SQL,
    DELETE_PAYLOAD_SQL,
    FIND_HARD_DELETE_CANDIDATES_SQL,
    FIND_UNREFERENCED_BLOBS_SQL,
    FIND_UNREFERENCED_PAYLOADS_SQL,
    hard_delete_candidates_params,
    run_hard_delete_batch,
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
        candidate_rows: list[tuple[object, ...]] | None = None,
        payload_rows: list[tuple[object, ...]] | None = None,
        blob_rows: list[tuple[object, ...]] | None = None,
        fail_on_contains: str | None = None,
    ) -> None:
        self.candidate_rows = list(candidate_rows or [])
        self.payload_rows = list(payload_rows or [])
        self.blob_rows = list(blob_rows or [])
        self.fail_on_contains = fail_on_contains
        self.executed: list[str] = []
        self.committed = False
        self.rolled_back = False

    def execute(self, query: str, _params: tuple[object, ...] | None = None) -> _FakeCursor:
        self.executed.append(query.strip())
        if self.fail_on_contains and self.fail_on_contains in query:
            raise RuntimeError("simulated execute failure")
        normalized = query.strip()
        if normalized == FIND_HARD_DELETE_CANDIDATES_SQL.strip():
            return _FakeCursor(self.candidate_rows)
        if normalized == FIND_UNREFERENCED_PAYLOADS_SQL.strip():
            return _FakeCursor(self.payload_rows)
        if normalized == FIND_UNREFERENCED_BLOBS_SQL.strip():
            return _FakeCursor(self.blob_rows)
        return _FakeCursor([])

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        self.rolled_back = True


def test_find_hard_delete_candidates_uses_skip_locked() -> None:
    assert "SKIP LOCKED" in FIND_HARD_DELETE_CANDIDATES_SQL


def test_delete_artifact_sql_structure() -> None:
    assert "DELETE FROM artifacts" in DELETE_ARTIFACT_SQL
    assert "workspace_id = %s" in DELETE_ARTIFACT_SQL
    assert "artifact_id = %s" in DELETE_ARTIFACT_SQL


def test_find_unreferenced_payloads_uses_not_exists() -> None:
    assert "NOT EXISTS" in FIND_UNREFERENCED_PAYLOADS_SQL


def test_find_unreferenced_blobs_uses_not_exists() -> None:
    assert "NOT EXISTS" in FIND_UNREFERENCED_BLOBS_SQL


def test_delete_payload_sql_structure() -> None:
    assert "DELETE FROM payload_blobs" in DELETE_PAYLOAD_SQL
    assert "workspace_id = %s" in DELETE_PAYLOAD_SQL
    assert "payload_hash_full = %s" in DELETE_PAYLOAD_SQL


def test_delete_blob_sql_structure() -> None:
    assert "DELETE FROM binary_blobs" in DELETE_BLOB_SQL
    assert "workspace_id = %s" in DELETE_BLOB_SQL
    assert "binary_hash = %s" in DELETE_BLOB_SQL


def test_hard_delete_candidates_params_returns_correct_tuple() -> None:
    ts = "2025-01-01T00:00:00Z"
    params = hard_delete_candidates_params(ts, batch_size=25)
    assert params == (WORKSPACE_ID, ts, 25)


def test_hard_delete_candidates_params_default_batch_size() -> None:
    ts = "2025-06-15T12:00:00Z"
    params = hard_delete_candidates_params(ts)
    assert params == (WORKSPACE_ID, ts, 50)


def test_run_hard_delete_batch_removes_records_and_fs_blobs(tmp_path) -> None:
    blob_one = tmp_path / "blob_1.bin"
    blob_two = tmp_path / "blob_2.bin"
    blob_one.write_bytes(b"a" * 30)
    blob_two.write_bytes(b"b" * 40)

    connection = _FakeConnection(
        candidate_rows=[("art_1", "payload_a"), ("art_2", "payload_b")],
        payload_rows=[("payload_orphan", 100)],
        blob_rows=[
            ("hash_1", "bin_1", str(blob_one), 30),
            ("hash_2", "bin_2", str(blob_two), 40),
        ],
    )

    metrics = GatewayMetrics()
    result = run_hard_delete_batch(
        connection,
        grace_period_timestamp="2025-01-01T00:00:00Z",
        batch_size=10,
        remove_fs_blobs=True,
        metrics=metrics,
    )

    assert result.artifacts_deleted == 2
    assert result.payloads_deleted == 1
    assert result.binary_blobs_deleted == 2
    assert result.fs_blobs_removed == 2
    assert result.bytes_reclaimed == 170
    assert connection.committed is True
    assert connection.rolled_back is False
    assert not blob_one.exists()
    assert not blob_two.exists()
    assert metrics.prune_hard_deletes.value == 2
    assert metrics.prune_bytes_reclaimed.value == 170
    assert metrics.prune_fs_orphans_removed.value == 2


def test_run_hard_delete_batch_rolls_back_on_error() -> None:
    connection = _FakeConnection(
        candidate_rows=[("art_1", "payload_a")],
        fail_on_contains="DELETE FROM artifacts",
    )
    try:
        run_hard_delete_batch(
            connection,
            grace_period_timestamp="2025-01-01T00:00:00Z",
            batch_size=1,
        )
    except RuntimeError as exc:
        assert "simulated execute failure" in str(exc)
    else:
        raise AssertionError("expected RuntimeError")
    assert connection.committed is False
    assert connection.rolled_back is True


def test_run_hard_delete_batch_no_candidates_zero_metrics() -> None:
    """No candidates -> zero metrics increments."""
    connection = _FakeConnection(
        candidate_rows=[],
        payload_rows=[],
        blob_rows=[],
    )
    metrics = GatewayMetrics()
    result = run_hard_delete_batch(
        connection,
        grace_period_timestamp="2025-01-01T00:00:00Z",
        batch_size=10,
        metrics=metrics,
    )
    assert result.artifacts_deleted == 0
    assert result.bytes_reclaimed == 0
    assert metrics.prune_hard_deletes.value == 0
    assert metrics.prune_bytes_reclaimed.value == 0
