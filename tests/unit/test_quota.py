"""Tests for jobs/quota.py — quota enforcement logic."""

from __future__ import annotations

from mcp_artifact_gateway.jobs.quota import (
    QuotaEnforcementResult,
    check_and_enforce_quota,
)
from mcp_artifact_gateway.obs.metrics import GatewayMetrics


# ---------------------------------------------------------------------------
# Fake DB helpers
# ---------------------------------------------------------------------------
class _FakeCursor:
    def __init__(self, rows: list[tuple[object, ...]] | None = None) -> None:
        self._rows = rows or []
        self._row = self._rows[0] if self._rows else None

    def fetchone(self) -> tuple[object, ...] | None:
        return self._row

    def fetchall(self) -> list[tuple[object, ...]]:
        return list(self._rows)


class _FakeConnection:
    """Fake connection that returns different rows for sequential execute() calls."""

    def __init__(
        self,
        *,
        call_results: list[list[tuple[object, ...]] | tuple[object, ...] | None],
        fail_on_call: int | None = None,
    ) -> None:
        self._call_results = list(call_results)
        self._call_index = 0
        self.fail_on_call = fail_on_call
        self.committed = False
        self.rolled_back = False

    def execute(
        self, _query: str, _params: tuple[object, ...] | None = None
    ) -> _FakeCursor:
        self._call_index += 1
        if self.fail_on_call is not None and self._call_index == self.fail_on_call:
            raise RuntimeError("simulated execute failure")
        if self._call_index - 1 < len(self._call_results):
            result = self._call_results[self._call_index - 1]
            if result is None:
                return _FakeCursor()
            if isinstance(result, tuple):
                return _FakeCursor([result])
            return _FakeCursor(result)
        return _FakeCursor()

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        self.rolled_back = True


# ---------------------------------------------------------------------------
# QuotaEnforcementResult
# ---------------------------------------------------------------------------
def test_quota_enforcement_result_fields() -> None:
    r = QuotaEnforcementResult(
        over_quota=True,
        usage_bytes_before=5000,
        cap_bytes=4000,
        soft_deleted=2,
        hard_deleted=1,
        bytes_reclaimed=1000,
    )
    assert r.over_quota is True
    assert r.usage_bytes_before == 5000
    assert r.cap_bytes == 4000
    assert r.soft_deleted == 2
    assert r.hard_deleted == 1
    assert r.bytes_reclaimed == 1000


# ---------------------------------------------------------------------------
# Under quota — no enforcement
# ---------------------------------------------------------------------------
def test_check_and_enforce_quota_under_quota_returns_immediately() -> None:
    # Storage usage query returns (total_payload_bytes, total_binary_bytes, count)
    conn = _FakeConnection(call_results=[(500, 200, 3)])
    result = check_and_enforce_quota(conn, max_total_storage_bytes=1000)
    assert result.over_quota is False
    assert result.usage_bytes_before == 500
    assert result.cap_bytes == 1000
    assert result.soft_deleted == 0
    assert result.hard_deleted == 0
    assert result.bytes_reclaimed == 0


def test_check_and_enforce_quota_at_exact_cap_returns_not_over() -> None:
    conn = _FakeConnection(call_results=[(1000, 500, 5)])
    result = check_and_enforce_quota(conn, max_total_storage_bytes=1000)
    assert result.over_quota is False


def test_check_and_enforce_quota_under_quota_no_metrics() -> None:
    conn = _FakeConnection(call_results=[(100, 50, 1)])
    metrics = GatewayMetrics()
    check_and_enforce_quota(conn, max_total_storage_bytes=1000, metrics=metrics)
    assert metrics.quota_breaches.value == 0
    assert metrics.quota_enforcements.value == 0


# ---------------------------------------------------------------------------
# Over quota — enforcement triggers
# ---------------------------------------------------------------------------
def test_check_and_enforce_quota_over_quota_triggers_enforcement() -> None:
    # Call sequence:
    # 1. storage_usage query -> over quota
    # 2. soft_delete_unreferenced execute -> returns deleted artifacts
    # 3. soft_delete commit (implicit)
    # 4. hard_delete candidates -> no candidates
    # 5-N. hard_delete subsequent queries
    conn = _FakeConnection(
        call_results=[
            (2000, 1000, 10),          # storage usage: 2000 > 1500 cap
            [("art_old_1",)],          # soft_delete: 1 deleted
            None,                      # hard_delete candidates: empty
            [],                        # unreferenced payloads
            [],                        # unreferenced blobs
        ]
    )
    metrics = GatewayMetrics()
    result = check_and_enforce_quota(
        conn,
        max_total_storage_bytes=1500,
        metrics=metrics,
    )
    assert result.over_quota is True
    assert result.usage_bytes_before == 2000
    assert result.cap_bytes == 1500
    assert metrics.quota_breaches.value == 1
    assert metrics.quota_enforcements.value == 1


def test_check_and_enforce_quota_over_quota_returns_soft_deleted_count() -> None:
    conn = _FakeConnection(
        call_results=[
            (5000, 2000, 20),                      # usage: over 3000 cap
            [("art_1",), ("art_2",), ("art_3",)],  # soft_delete: 3 deleted
            [],                                     # hard_delete candidates: empty
            [],                                     # unreferenced payloads
            [],                                     # unreferenced blobs
        ]
    )
    result = check_and_enforce_quota(conn, max_total_storage_bytes=3000)
    assert result.over_quota is True
    assert result.soft_deleted == 3


# ---------------------------------------------------------------------------
# Failure resilience
# ---------------------------------------------------------------------------
def test_check_and_enforce_quota_soft_delete_failure_does_not_crash() -> None:
    """If soft_delete raises, enforcement continues to hard_delete."""
    conn = _FakeConnection(
        call_results=[
            (2000, 1000, 10),  # usage: over cap
        ],
        fail_on_call=2,        # soft_delete execute fails
    )
    result = check_and_enforce_quota(conn, max_total_storage_bytes=1000)
    assert result.over_quota is True
    assert result.soft_deleted == 0


def test_check_and_enforce_quota_hard_delete_failure_does_not_crash() -> None:
    """If hard_delete raises, enforcement still returns a result."""
    conn = _FakeConnection(
        call_results=[
            (2000, 1000, 10),      # usage: over cap
            [("art_1",)],          # soft_delete: 1 deleted
        ],
        fail_on_call=3,            # hard_delete candidates fails
    )
    result = check_and_enforce_quota(conn, max_total_storage_bytes=1000)
    assert result.over_quota is True
    assert result.soft_deleted == 1
    assert result.hard_deleted == 0
    assert result.bytes_reclaimed == 0


def test_check_and_enforce_quota_usage_bytes_before_is_pre_enforcement() -> None:
    """usage_bytes_before reflects the value BEFORE any deletions."""
    conn = _FakeConnection(
        call_results=[
            (5000, 2000, 20),          # pre-enforcement usage
            [("art_1",), ("art_2",)],  # soft_delete: 2 deleted
            [],                        # hard_delete candidates: empty
            [],                        # unreferenced payloads
            [],                        # unreferenced blobs
        ]
    )
    result = check_and_enforce_quota(conn, max_total_storage_bytes=3000)
    assert result.usage_bytes_before == 5000
    assert result.soft_deleted == 2
