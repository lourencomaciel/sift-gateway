"""Tests for quota enforcement module."""
from __future__ import annotations

import datetime as dt
from decimal import Decimal

from mcp_artifact_gateway.constants import WORKSPACE_ID
from mcp_artifact_gateway.jobs.quota import (
    SOFT_DELETE_LRU_FOR_QUOTA_SQL,
    STORAGE_USAGE_SQL,
    QuotaBreaches,
    QuotaEnforcementResult,
    StorageUsage,
    _parse_storage_usage,
    check_breaches,
    enforce_quota,
    query_storage_usage,
    soft_delete_lru_batch,
    soft_delete_lru_params,
    storage_usage_params,
)
from mcp_artifact_gateway.jobs.hard_delete import (
    FIND_HARD_DELETE_CANDIDATES_SQL,
    FIND_UNREFERENCED_BLOBS_SQL,
    FIND_UNREFERENCED_PAYLOADS_SQL,
    HardDeleteResult,
)
from mcp_artifact_gateway.obs.metrics import GatewayMetrics


# ---------------------------------------------------------------------------
# Fake DB helpers
# ---------------------------------------------------------------------------
class _FakeCursor:
    def __init__(
        self,
        rows: list[tuple[object, ...]] | None = None,
        one: tuple[object, ...] | None = None,
    ) -> None:
        self._rows = list(rows or [])
        self._one = one

    def fetchone(self) -> tuple[object, ...] | None:
        return self._one

    def fetchall(self) -> list[tuple[object, ...]]:
        return list(self._rows)


class _FakeConnection:
    """Query-dispatching fake connection for quota tests."""

    def __init__(
        self,
        *,
        usage_row: tuple[object, ...] | None = (0, 0, 0),
        soft_delete_rows: list[tuple[object, ...]] | None = None,
        candidate_rows: list[tuple[object, ...]] | None = None,
        payload_rows: list[tuple[object, ...]] | None = None,
        blob_rows: list[tuple[object, ...]] | None = None,
        fail_on_contains: str | None = None,
        usage_sequence: list[tuple[object, ...]] | None = None,
    ) -> None:
        self.usage_row = usage_row
        self.soft_delete_rows = list(soft_delete_rows or [])
        self.candidate_rows = list(candidate_rows or [])
        self.payload_rows = list(payload_rows or [])
        self.blob_rows = list(blob_rows or [])
        self.fail_on_contains = fail_on_contains
        self._usage_sequence = list(usage_sequence or [])
        self._usage_call_count = 0
        self.executed: list[str] = []
        self.committed = 0
        self.rolled_back = 0

    def execute(self, query: str, _params: tuple[object, ...] | None = None) -> _FakeCursor:
        self.executed.append(query.strip())
        if self.fail_on_contains and self.fail_on_contains in query:
            raise RuntimeError("simulated execute failure")
        normalized = query.strip()
        if normalized == STORAGE_USAGE_SQL.strip():
            if self._usage_sequence:
                row = self._usage_sequence[
                    min(self._usage_call_count, len(self._usage_sequence) - 1)
                ]
                self._usage_call_count += 1
                return _FakeCursor(one=row)
            return _FakeCursor(one=self.usage_row)
        if normalized == SOFT_DELETE_LRU_FOR_QUOTA_SQL.strip():
            return _FakeCursor(rows=self.soft_delete_rows)
        if normalized == FIND_HARD_DELETE_CANDIDATES_SQL.strip():
            return _FakeCursor(rows=self.candidate_rows)
        if normalized == FIND_UNREFERENCED_PAYLOADS_SQL.strip():
            return _FakeCursor(rows=self.payload_rows)
        if normalized == FIND_UNREFERENCED_BLOBS_SQL.strip():
            return _FakeCursor(rows=self.blob_rows)
        return _FakeCursor()

    def commit(self) -> None:
        self.committed += 1

    def rollback(self) -> None:
        self.rolled_back += 1


# ---------------------------------------------------------------------------
# SQL structure tests
# ---------------------------------------------------------------------------
def test_storage_usage_sql_uses_coalesce() -> None:
    assert "COALESCE" in STORAGE_USAGE_SQL


def test_storage_usage_sql_queries_binary_blobs() -> None:
    assert "binary_blobs" in STORAGE_USAGE_SQL


def test_storage_usage_sql_queries_payload_blobs() -> None:
    assert "payload_blobs" in STORAGE_USAGE_SQL


def test_soft_delete_lru_sql_uses_skip_locked() -> None:
    assert "SKIP LOCKED" in SOFT_DELETE_LRU_FOR_QUOTA_SQL


def test_soft_delete_lru_sql_orders_by_last_referenced_at() -> None:
    assert "last_referenced_at ASC" in SOFT_DELETE_LRU_FOR_QUOTA_SQL


def test_soft_delete_lru_sql_rechecks_deleted_at() -> None:
    # Outer WHERE must recheck deleted_at IS NULL
    outer = SOFT_DELETE_LRU_FOR_QUOTA_SQL.split("FROM candidates")[1]
    assert "deleted_at IS NULL" in outer


def test_soft_delete_lru_sql_rechecks_generation() -> None:
    outer = SOFT_DELETE_LRU_FOR_QUOTA_SQL.split("FROM candidates")[1]
    assert "a.generation = c.generation" in outer


# ---------------------------------------------------------------------------
# Param helpers
# ---------------------------------------------------------------------------
def test_storage_usage_params_returns_correct_tuple() -> None:
    params = storage_usage_params()
    assert params == (WORKSPACE_ID, WORKSPACE_ID, WORKSPACE_ID)


def test_soft_delete_lru_params_default_batch() -> None:
    params = soft_delete_lru_params()
    assert params == (WORKSPACE_ID, 100, WORKSPACE_ID)


def test_soft_delete_lru_params_custom_batch() -> None:
    params = soft_delete_lru_params(batch_size=50)
    assert params == (WORKSPACE_ID, 50, WORKSPACE_ID)


# ---------------------------------------------------------------------------
# _parse_storage_usage
# ---------------------------------------------------------------------------
def test_parse_storage_usage_from_valid_row() -> None:
    usage = _parse_storage_usage((500, 1000, 800))
    assert usage.binary_blob_bytes == 500
    assert usage.payload_total_bytes == 1000
    # total = payload_json_bytes(800) + binary_blob_bytes(500)
    assert usage.total_storage_bytes == 1300


def test_parse_storage_usage_from_decimal_row() -> None:
    usage = _parse_storage_usage((Decimal("500"), Decimal("1000"), Decimal("800")))
    assert usage.binary_blob_bytes == 500
    assert usage.payload_total_bytes == 1000
    assert usage.total_storage_bytes == 1300


def test_parse_storage_usage_from_none_row() -> None:
    usage = _parse_storage_usage(None)
    assert usage.binary_blob_bytes == 0
    assert usage.payload_total_bytes == 0
    assert usage.total_storage_bytes == 0


def test_parse_storage_usage_from_short_row() -> None:
    usage = _parse_storage_usage((100,))
    assert usage.binary_blob_bytes == 0
    assert usage.payload_total_bytes == 0
    assert usage.total_storage_bytes == 0


def test_parse_storage_usage_total_avoids_double_counting() -> None:
    # binary_blob_bytes=200, payload_total_bytes=500, payload_json_bytes=300
    # total = 300 + 200 = 500 (not 500 + 200 = 700)
    usage = _parse_storage_usage((200, 500, 300))
    assert usage.total_storage_bytes == 500


# ---------------------------------------------------------------------------
# check_breaches
# ---------------------------------------------------------------------------
def test_check_breaches_no_breach() -> None:
    usage = StorageUsage(binary_blob_bytes=100, payload_total_bytes=200, total_storage_bytes=300)
    breaches = check_breaches(
        usage,
        max_binary_blob_bytes=500,
        max_payload_total_bytes=500,
        max_total_storage_bytes=500,
    )
    assert not breaches.any_exceeded
    assert not breaches.binary_blob_exceeded
    assert not breaches.payload_total_exceeded
    assert not breaches.total_storage_exceeded


def test_check_breaches_binary_only() -> None:
    usage = StorageUsage(binary_blob_bytes=600, payload_total_bytes=200, total_storage_bytes=300)
    breaches = check_breaches(
        usage,
        max_binary_blob_bytes=500,
        max_payload_total_bytes=500,
        max_total_storage_bytes=500,
    )
    assert breaches.any_exceeded
    assert breaches.binary_blob_exceeded
    assert not breaches.payload_total_exceeded
    assert not breaches.total_storage_exceeded


def test_check_breaches_payload_only() -> None:
    usage = StorageUsage(binary_blob_bytes=100, payload_total_bytes=600, total_storage_bytes=300)
    breaches = check_breaches(
        usage,
        max_binary_blob_bytes=500,
        max_payload_total_bytes=500,
        max_total_storage_bytes=500,
    )
    assert breaches.any_exceeded
    assert not breaches.binary_blob_exceeded
    assert breaches.payload_total_exceeded
    assert not breaches.total_storage_exceeded


def test_check_breaches_total_only() -> None:
    usage = StorageUsage(binary_blob_bytes=100, payload_total_bytes=200, total_storage_bytes=600)
    breaches = check_breaches(
        usage,
        max_binary_blob_bytes=500,
        max_payload_total_bytes=500,
        max_total_storage_bytes=500,
    )
    assert breaches.any_exceeded
    assert breaches.total_storage_exceeded


def test_check_breaches_all_exceeded() -> None:
    usage = StorageUsage(binary_blob_bytes=600, payload_total_bytes=600, total_storage_bytes=600)
    breaches = check_breaches(
        usage,
        max_binary_blob_bytes=500,
        max_payload_total_bytes=500,
        max_total_storage_bytes=500,
    )
    assert breaches.binary_blob_exceeded
    assert breaches.payload_total_exceeded
    assert breaches.total_storage_exceeded


def test_check_breaches_at_exact_limit_not_exceeded() -> None:
    usage = StorageUsage(binary_blob_bytes=500, payload_total_bytes=500, total_storage_bytes=500)
    breaches = check_breaches(
        usage,
        max_binary_blob_bytes=500,
        max_payload_total_bytes=500,
        max_total_storage_bytes=500,
    )
    assert not breaches.any_exceeded


# ---------------------------------------------------------------------------
# query_storage_usage
# ---------------------------------------------------------------------------
def test_query_storage_usage_executes_correct_sql() -> None:
    conn = _FakeConnection(usage_row=(100, 200, 150))
    usage = query_storage_usage(conn)
    assert usage.binary_blob_bytes == 100
    assert usage.payload_total_bytes == 200
    assert usage.total_storage_bytes == 250  # 150 + 100
    assert any(STORAGE_USAGE_SQL.strip() in q for q in conn.executed)


# ---------------------------------------------------------------------------
# soft_delete_lru_batch
# ---------------------------------------------------------------------------
def test_soft_delete_lru_batch_returns_count_and_bytes() -> None:
    conn = _FakeConnection(
        soft_delete_rows=[("art_1", 500), ("art_2", 300)],
    )
    metrics = GatewayMetrics()
    count, est_bytes = soft_delete_lru_batch(conn, metrics=metrics)
    assert count == 2
    assert est_bytes == 800
    assert conn.committed == 0
    assert metrics.prune_soft_deletes.value == 2


def test_soft_delete_lru_batch_empty_result() -> None:
    conn = _FakeConnection(soft_delete_rows=[])
    metrics = GatewayMetrics()
    count, est_bytes = soft_delete_lru_batch(conn, metrics=metrics)
    assert count == 0
    assert est_bytes == 0


def test_soft_delete_lru_batch_skips_malformed_rows() -> None:
    conn = _FakeConnection(
        soft_delete_rows=[("art_1", 500), (123, "bad"), ()],
    )
    count, est_bytes = soft_delete_lru_batch(conn)
    assert count == 1
    assert est_bytes == 500


# ---------------------------------------------------------------------------
# enforce_quota — no breach
# ---------------------------------------------------------------------------
def test_enforce_quota_no_breach_returns_immediately() -> None:
    conn = _FakeConnection(usage_row=(100, 200, 150))
    metrics = GatewayMetrics()
    result = enforce_quota(
        conn,
        max_binary_blob_bytes=500,
        max_payload_total_bytes=500,
        max_total_storage_bytes=500,
        metrics=metrics,
    )
    assert result.space_cleared is True
    assert result.pruned is False
    assert result.soft_deleted_count == 0
    assert result.hard_deleted_count == 0
    assert result.bytes_reclaimed == 0
    assert result.usage_after is None
    assert result.breaches_after is None
    assert metrics.quota_checks.value == 1
    assert metrics.quota_breaches.value == 0
    assert metrics.quota_prune_triggered.value == 0


# ---------------------------------------------------------------------------
# enforce_quota — breach triggers prune
# ---------------------------------------------------------------------------
def test_enforce_quota_breach_triggers_prune() -> None:
    conn = _FakeConnection(
        # First usage call: over quota; second (after prune): under quota
        usage_sequence=[(600, 800, 700), (100, 200, 150)],
        soft_delete_rows=[("art_old", 600)],
        candidate_rows=[("art_old", "hash_old")],
        payload_rows=[("hash_old", 600)],
        blob_rows=[],
    )
    metrics = GatewayMetrics()
    result = enforce_quota(
        conn,
        max_binary_blob_bytes=500,
        max_payload_total_bytes=500,
        max_total_storage_bytes=500,
        metrics=metrics,
    )
    assert result.space_cleared is True
    assert result.pruned is True
    assert result.soft_deleted_count == 1
    assert metrics.quota_checks.value == 1
    assert metrics.quota_breaches.value == 1
    assert metrics.quota_prune_triggered.value == 1


def test_enforce_quota_prune_clears_space_after_multiple_rounds() -> None:
    conn = _FakeConnection(
        # Round 1: still over; Round 2: cleared
        usage_sequence=[(600, 800, 700), (400, 600, 500), (100, 200, 150)],
        soft_delete_rows=[("art_1", 200), ("art_2", 200)],
        candidate_rows=[("art_1", "h1"), ("art_2", "h2")],
        payload_rows=[("h1", 200), ("h2", 200)],
        blob_rows=[],
    )
    result = enforce_quota(
        conn,
        max_binary_blob_bytes=500,
        max_payload_total_bytes=500,
        max_total_storage_bytes=500,
    )
    assert result.space_cleared is True
    assert result.pruned is True


def test_enforce_quota_runs_hard_delete_with_no_new_soft_deletes() -> None:
    conn = _FakeConnection(
        # First usage call: over quota; second (after hard-delete): under quota
        usage_sequence=[(600, 800, 700), (100, 200, 150)],
        soft_delete_rows=[],  # nothing newly soft-deleted this round
        candidate_rows=[("art_deleted", "hash_deleted")],  # already soft-deleted
        payload_rows=[("hash_deleted", 600)],
        blob_rows=[],
    )
    result = enforce_quota(
        conn,
        max_binary_blob_bytes=500,
        max_payload_total_bytes=500,
        max_total_storage_bytes=500,
    )
    assert result.space_cleared is True
    assert result.soft_deleted_count == 0
    assert result.hard_deleted_count == 1


def test_enforce_quota_max_rounds_exceeded() -> None:
    conn = _FakeConnection(
        # Always over quota — never clears
        usage_sequence=[(600, 800, 700)] * 10,
        soft_delete_rows=[("art_x", 10)],
        candidate_rows=[("art_x", "hx")],
        payload_rows=[],
        blob_rows=[],
    )
    result = enforce_quota(
        conn,
        max_binary_blob_bytes=500,
        max_payload_total_bytes=500,
        max_total_storage_bytes=500,
        max_prune_rounds=3,
    )
    assert result.space_cleared is False
    assert result.pruned is True
    assert result.soft_deleted_count >= 3


def test_enforce_quota_stops_when_no_candidates_to_prune() -> None:
    conn = _FakeConnection(
        usage_sequence=[(600, 800, 700)] * 10,
        soft_delete_rows=[],  # no candidates
        candidate_rows=[],
        payload_rows=[],
        blob_rows=[],
    )
    result = enforce_quota(
        conn,
        max_binary_blob_bytes=500,
        max_payload_total_bytes=500,
        max_total_storage_bytes=500,
        max_prune_rounds=5,
    )
    assert result.space_cleared is False
    assert result.pruned is True
    assert result.soft_deleted_count == 0


def test_enforce_quota_uses_past_cutoff_for_hard_delete_grace(monkeypatch) -> None:
    conn = _FakeConnection(
        usage_sequence=[(600, 800, 700), (600, 800, 700)],
    )
    captured: dict[str, str] = {}

    def _fake_soft_delete(*_args, **_kwargs):
        return 1, 0

    def _fake_hard_delete(*_args, **kwargs):
        captured["grace_period_timestamp"] = kwargs["grace_period_timestamp"]
        return HardDeleteResult(
            artifacts_deleted=0,
            payloads_deleted=0,
            binary_blobs_deleted=0,
            fs_blobs_removed=0,
            bytes_reclaimed=0,
        )

    monkeypatch.setattr(
        "mcp_artifact_gateway.jobs.quota.soft_delete_lru_batch",
        _fake_soft_delete,
    )
    monkeypatch.setattr(
        "mcp_artifact_gateway.jobs.quota.run_hard_delete_batch",
        _fake_hard_delete,
    )

    before = dt.datetime.now(dt.timezone.utc)
    enforce_quota(
        conn,
        max_binary_blob_bytes=500,
        max_payload_total_bytes=500,
        max_total_storage_bytes=500,
        hard_delete_grace_seconds=60,
        max_prune_rounds=1,
    )
    after = dt.datetime.now(dt.timezone.utc)

    cutoff = dt.datetime.fromisoformat(captured["grace_period_timestamp"])
    assert cutoff <= after - dt.timedelta(seconds=59)
    assert cutoff >= before - dt.timedelta(seconds=61)


def test_enforce_quota_recomputes_cutoff_each_round(monkeypatch) -> None:
    conn = _FakeConnection(
        # initial usage + post-round1 + post-round2
        usage_sequence=[(600, 800, 700), (600, 800, 700), (100, 200, 150)],
    )
    captured: list[str] = []
    cutoff_values = iter(
        [
            dt.datetime(2026, 1, 1, 0, 0, 1, tzinfo=dt.timezone.utc).isoformat(),
            dt.datetime(2026, 1, 1, 0, 0, 4, tzinfo=dt.timezone.utc).isoformat(),
        ]
    )

    def _fake_soft_delete(*_args, **_kwargs):
        return 1, 0

    def _fake_cutoff(hard_delete_grace_seconds: int) -> str:
        assert hard_delete_grace_seconds == 1
        return next(cutoff_values)

    def _fake_hard_delete(*_args, **kwargs):
        captured.append(kwargs["grace_period_timestamp"])
        return HardDeleteResult(
            artifacts_deleted=1,
            payloads_deleted=0,
            binary_blobs_deleted=0,
            fs_blobs_removed=0,
            bytes_reclaimed=0,
        )

    monkeypatch.setattr(
        "mcp_artifact_gateway.jobs.quota._hard_delete_cutoff_timestamp",
        _fake_cutoff,
    )
    monkeypatch.setattr(
        "mcp_artifact_gateway.jobs.quota.soft_delete_lru_batch",
        _fake_soft_delete,
    )
    monkeypatch.setattr(
        "mcp_artifact_gateway.jobs.quota.run_hard_delete_batch",
        _fake_hard_delete,
    )

    result = enforce_quota(
        conn,
        max_binary_blob_bytes=500,
        max_payload_total_bytes=500,
        max_total_storage_bytes=500,
        hard_delete_grace_seconds=1,
        max_prune_rounds=2,
    )
    assert result.space_cleared is True
    assert captured == [
        "2026-01-01T00:00:01+00:00",
        "2026-01-01T00:00:04+00:00",
    ]


def test_enforce_quota_rolls_back_on_usage_query_error() -> None:
    conn = _FakeConnection(fail_on_contains="SUM")
    try:
        enforce_quota(
            conn,
            max_binary_blob_bytes=500,
            max_payload_total_bytes=500,
            max_total_storage_bytes=500,
        )
        assert False, "should have raised"
    except RuntimeError:
        assert conn.rolled_back >= 1


def test_enforce_quota_increments_quota_checks() -> None:
    conn = _FakeConnection(usage_row=(100, 200, 150))
    metrics = GatewayMetrics()
    enforce_quota(
        conn,
        max_binary_blob_bytes=500,
        max_payload_total_bytes=500,
        max_total_storage_bytes=500,
        metrics=metrics,
    )
    assert metrics.quota_checks.value == 1


def test_enforce_quota_increments_quota_breaches_only_when_breached() -> None:
    conn = _FakeConnection(usage_row=(100, 200, 150))
    metrics = GatewayMetrics()
    enforce_quota(
        conn,
        max_binary_blob_bytes=500,
        max_payload_total_bytes=500,
        max_total_storage_bytes=500,
        metrics=metrics,
    )
    assert metrics.quota_breaches.value == 0


def test_enforce_quota_returns_usage_before() -> None:
    conn = _FakeConnection(usage_row=(100, 200, 150))
    result = enforce_quota(
        conn,
        max_binary_blob_bytes=500,
        max_payload_total_bytes=500,
        max_total_storage_bytes=500,
    )
    assert result.usage_before.binary_blob_bytes == 100
    assert result.usage_before.payload_total_bytes == 200
    assert result.usage_before.total_storage_bytes == 250  # 150 + 100


# ---------------------------------------------------------------------------
# Dataclass invariants
# ---------------------------------------------------------------------------
def test_storage_usage_is_frozen() -> None:
    usage = StorageUsage(binary_blob_bytes=0, payload_total_bytes=0, total_storage_bytes=0)
    try:
        usage.binary_blob_bytes = 1  # type: ignore[misc]
        assert False, "should be frozen"
    except AttributeError:
        pass


def test_quota_breaches_is_frozen() -> None:
    breaches = QuotaBreaches(
        binary_blob_exceeded=False,
        payload_total_exceeded=False,
        total_storage_exceeded=False,
    )
    try:
        breaches.binary_blob_exceeded = True  # type: ignore[misc]
        assert False, "should be frozen"
    except AttributeError:
        pass


def test_quota_enforcement_result_is_frozen() -> None:
    result = QuotaEnforcementResult(
        usage_before=StorageUsage(0, 0, 0),
        usage_after=None,
        breaches_before=QuotaBreaches(False, False, False),
        breaches_after=None,
        pruned=False,
        soft_deleted_count=0,
        hard_deleted_count=0,
        bytes_reclaimed=0,
        space_cleared=True,
    )
    try:
        result.space_cleared = False  # type: ignore[misc]
        assert False, "should be frozen"
    except AttributeError:
        pass
