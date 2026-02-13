from __future__ import annotations

from sift_mcp.constants import WORKSPACE_ID
from sift_mcp.db.backend import Dialect
from sift_mcp.jobs.hard_delete import (
    DELETE_ARTIFACTS_BATCH_SQL,
    DELETE_BLOBS_BATCH_SQL,
    DELETE_PAYLOADS_BATCH_SQL,
    FIND_HARD_DELETE_CANDIDATES_SQL,
    FIND_UNREFERENCED_BLOBS_SQL,
    FIND_UNREFERENCED_PAYLOADS_SQL,
    hard_delete_candidates_params,
    run_hard_delete_batch,
)
from sift_mcp.obs.metrics import GatewayMetrics, counter_value


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

    def execute(
        self, query: str, _params: tuple[object, ...] | None = None
    ) -> _FakeCursor:
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
    assert "DELETE FROM artifacts" in DELETE_ARTIFACTS_BATCH_SQL
    assert "workspace_id = %s" in DELETE_ARTIFACTS_BATCH_SQL
    assert "ANY(%s)" in DELETE_ARTIFACTS_BATCH_SQL


def test_find_unreferenced_payloads_uses_not_exists() -> None:
    assert "NOT EXISTS" in FIND_UNREFERENCED_PAYLOADS_SQL


def test_find_unreferenced_blobs_uses_not_exists() -> None:
    assert "NOT EXISTS" in FIND_UNREFERENCED_BLOBS_SQL


def test_delete_payload_sql_structure() -> None:
    assert "DELETE FROM payload_blobs" in DELETE_PAYLOADS_BATCH_SQL
    assert "workspace_id = %s" in DELETE_PAYLOADS_BATCH_SQL
    assert "ANY(%s)" in DELETE_PAYLOADS_BATCH_SQL


def test_delete_blob_sql_structure() -> None:
    assert "DELETE FROM binary_blobs" in DELETE_BLOBS_BATCH_SQL
    assert "workspace_id = %s" in DELETE_BLOBS_BATCH_SQL
    assert "ANY(%s)" in DELETE_BLOBS_BATCH_SQL


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
    assert counter_value(metrics.prune_hard_deletes) == 2
    assert counter_value(metrics.prune_bytes_reclaimed) == 170
    assert counter_value(metrics.prune_fs_orphans_removed) == 2


def test_run_hard_delete_batch_constrains_fs_deletes_to_blobs_root(
    tmp_path,
) -> None:
    blobs_root = tmp_path / "blobs_root"
    blobs_root.mkdir(parents=True, exist_ok=True)
    allowed_blob = blobs_root / "allowed.bin"
    outside_blob = tmp_path / "outside.bin"
    allowed_blob.write_bytes(b"a" * 30)
    outside_blob.write_bytes(b"b" * 40)

    connection = _FakeConnection(
        candidate_rows=[("art_1", "payload_a"), ("art_2", "payload_b")],
        payload_rows=[("payload_orphan", 100)],
        blob_rows=[
            ("hash_1", "bin_1", str(allowed_blob), 30),
            ("hash_2", "bin_2", str(outside_blob), 40),
        ],
    )

    result = run_hard_delete_batch(
        connection,
        grace_period_timestamp="2025-01-01T00:00:00Z",
        batch_size=10,
        remove_fs_blobs=True,
        blobs_root=blobs_root,
    )

    assert result.artifacts_deleted == 2
    assert result.payloads_deleted == 1
    assert result.binary_blobs_deleted == 2
    assert result.fs_blobs_removed == 1
    assert result.bytes_reclaimed == 170
    assert not allowed_blob.exists()
    assert outside_blob.exists()


def test_run_hard_delete_batch_blocks_symlink_escape_with_blobs_root(
    tmp_path,
) -> None:
    blobs_root = tmp_path / "blobs_root"
    blobs_root.mkdir(parents=True, exist_ok=True)
    outside_dir = tmp_path / "outside"
    outside_dir.mkdir(parents=True, exist_ok=True)
    outside_blob = outside_dir / "outside.bin"
    outside_blob.write_bytes(b"x" * 40)

    # Symlink path lives under blobs_root but resolves outside it.
    (blobs_root / "ln").symlink_to(outside_dir)
    escaped_blob_path = blobs_root / "ln" / "outside.bin"

    connection = _FakeConnection(
        candidate_rows=[("art_1", "payload_a")],
        payload_rows=[],
        blob_rows=[("hash_1", "bin_1", str(escaped_blob_path), 40)],
    )

    result = run_hard_delete_batch(
        connection,
        grace_period_timestamp="2025-01-01T00:00:00Z",
        batch_size=10,
        remove_fs_blobs=True,
        blobs_root=blobs_root,
    )

    assert result.artifacts_deleted == 1
    assert result.binary_blobs_deleted == 1
    assert result.fs_blobs_removed == 0
    assert outside_blob.exists()


def test_run_hard_delete_batch_unlinks_symlink_path_not_target(
    tmp_path,
) -> None:
    link_path = tmp_path / "blob_link.bin"
    real_target = tmp_path / "outside_target.bin"
    real_target.write_bytes(b"x" * 50)
    link_path.symlink_to(real_target)

    connection = _FakeConnection(
        candidate_rows=[("art_1", "payload_a")],
        payload_rows=[],
        blob_rows=[("hash_1", "bin_1", str(link_path), 50)],
    )

    result = run_hard_delete_batch(
        connection,
        grace_period_timestamp="2025-01-01T00:00:00Z",
        batch_size=10,
        remove_fs_blobs=True,
    )

    assert result.artifacts_deleted == 1
    assert result.binary_blobs_deleted == 1
    assert result.fs_blobs_removed == 1
    assert not link_path.exists()
    assert real_target.exists()


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
    assert counter_value(metrics.prune_hard_deletes) == 0
    assert counter_value(metrics.prune_bytes_reclaimed) == 0


# ---- SQLite dialect tests ----


class _SqliteFakeConnection:
    """Fake connection that matches queries by keyword rather than exact SQL."""

    def __init__(
        self,
        *,
        candidate_rows: list[tuple[object, ...]] | None = None,
        payload_rows: list[tuple[object, ...]] | None = None,
        blob_rows: list[tuple[object, ...]] | None = None,
    ) -> None:
        self.candidate_rows = list(candidate_rows or [])
        self.payload_rows = list(payload_rows or [])
        self.blob_rows = list(blob_rows or [])
        self.executed: list[str] = []
        self.committed = False
        self.rolled_back = False

    def execute(
        self, query: str, _params: tuple[object, ...] | None = None
    ) -> "_SqliteFakeCursor":
        self.executed.append(query.strip())
        normalized = query.strip().upper()
        if "DELETED_AT IS NOT NULL" in normalized and "SELECT" in normalized:
            return _SqliteFakeCursor(self.candidate_rows)
        if "NOT EXISTS" in normalized and "PAYLOAD_BLOBS" in normalized:
            return _SqliteFakeCursor(self.payload_rows)
        if "NOT EXISTS" in normalized and "BINARY_BLOBS" in normalized:
            return _SqliteFakeCursor(self.blob_rows)
        return _SqliteFakeCursor([])

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        self.rolled_back = True


class _SqliteFakeCursor:
    def __init__(self, rows: list[tuple[object, ...]]) -> None:
        self._rows = rows

    def fetchall(self) -> list[tuple[object, ...]]:
        return list(self._rows)


def test_run_hard_delete_batch_sqlite_strips_skip_locked() -> None:
    """Hard delete with SQLite dialect strips SKIP LOCKED from queries."""
    connection = _SqliteFakeConnection(
        candidate_rows=[("art_sq1",)],
        payload_rows=[],
        blob_rows=[],
    )
    metrics = GatewayMetrics()
    result = run_hard_delete_batch(
        connection,
        grace_period_timestamp="2025-01-01T00:00:00Z",
        batch_size=5,
        dialect=Dialect.SQLITE,
        metrics=metrics,
    )
    assert result.artifacts_deleted == 1
    assert connection.committed is True
    # Verify no executed SQL contains SKIP LOCKED
    for sql in connection.executed:
        assert "SKIP LOCKED" not in sql


def test_run_hard_delete_batch_sqlite_expands_any_clause() -> None:
    """Hard delete with SQLite dialect expands ANY() to IN placeholders."""
    connection = _SqliteFakeConnection(
        candidate_rows=[("art_a1",), ("art_a2",)],
        payload_rows=[("pay_1", 200)],
        blob_rows=[("blob_1", "bin_1", "/tmp/b.bin", 50)],
    )
    metrics = GatewayMetrics()
    result = run_hard_delete_batch(
        connection,
        grace_period_timestamp="2025-01-01T00:00:00Z",
        batch_size=10,
        dialect=Dialect.SQLITE,
        remove_fs_blobs=False,
        metrics=metrics,
    )
    assert result.artifacts_deleted == 2
    assert result.payloads_deleted == 1
    assert result.binary_blobs_deleted == 1
    assert connection.committed is True
    # Verify ANY() was expanded to IN(?, ?) in the delete queries
    delete_sqls = [s for s in connection.executed if "DELETE" in s.upper()]
    for sql in delete_sqls:
        assert "ANY(" not in sql


def test_run_hard_delete_batch_sqlite_no_candidates() -> None:
    """Hard delete with SQLite dialect and no candidates produces zero results."""
    connection = _SqliteFakeConnection(
        candidate_rows=[],
        payload_rows=[],
        blob_rows=[],
    )
    result = run_hard_delete_batch(
        connection,
        grace_period_timestamp="2025-01-01T00:00:00Z",
        batch_size=10,
        dialect=Dialect.SQLITE,
    )
    assert result.artifacts_deleted == 0
    assert result.payloads_deleted == 0
    assert result.binary_blobs_deleted == 0
    assert connection.committed is True
