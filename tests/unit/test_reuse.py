from __future__ import annotations

import dataclasses

import asyncio

from mcp_artifact_gateway.cache.reuse import (
    ReuseResult,
    acquire_advisory_lock,
    acquire_advisory_lock_async,
    check_reuse_candidate,
    try_acquire_advisory_lock,
)
from mcp_artifact_gateway.obs.metrics import GatewayMetrics

# advisory_lock_keys is tested comprehensively in test_hashing.py
# (canonical location: util.hashing, re-exported via cache.reuse)


# ---- check_reuse_candidate ----


def test_check_reuse_none_candidate() -> None:
    result = check_reuse_candidate(None, expected_schema_hash="abc123")
    assert result.reused is False
    assert result.artifact_id is None


def test_check_reuse_valid_candidate() -> None:
    row = {
        "artifact_id": "art_abc",
        "payload_hash_full": "phash123",
        "upstream_tool_schema_hash": "schema_v1",
        "map_status": "mapped",
        "generation": 1,
    }
    result = check_reuse_candidate(row, expected_schema_hash="schema_v1")
    assert result.reused is True
    assert result.artifact_id == "art_abc"
    assert result.reason == "request_key_match"


def test_check_reuse_schema_mismatch_strict() -> None:
    row = {
        "artifact_id": "art_abc",
        "payload_hash_full": "phash123",
        "upstream_tool_schema_hash": "schema_v1",
        "map_status": "mapped",
        "generation": 1,
    }
    result = check_reuse_candidate(
        row,
        expected_schema_hash="schema_v2",
        strict_schema_reuse=True,
    )
    assert result.reused is False
    assert result.reason == "schema_hash_mismatch"


def test_check_reuse_schema_mismatch_non_strict() -> None:
    row = {
        "artifact_id": "art_abc",
        "payload_hash_full": "phash123",
        "upstream_tool_schema_hash": "schema_v1",
        "map_status": "mapped",
        "generation": 1,
    }
    result = check_reuse_candidate(
        row,
        expected_schema_hash="schema_v2",
        strict_schema_reuse=False,
    )
    assert result.reused is True
    assert result.artifact_id == "art_abc"


def test_check_reuse_no_expected_schema_hash() -> None:
    """When expected_schema_hash is None, skip schema check even with strict=True."""
    row = {
        "artifact_id": "art_abc",
        "payload_hash_full": "phash123",
        "upstream_tool_schema_hash": "schema_v1",
        "map_status": "mapped",
        "generation": 1,
    }
    result = check_reuse_candidate(
        row,
        expected_schema_hash=None,
        strict_schema_reuse=True,
    )
    assert result.reused is True
    assert result.artifact_id == "art_abc"


# ---- ReuseResult ----


def test_reuse_result_frozen() -> None:
    r = ReuseResult(reused=True, artifact_id="art_1", reason="request_key_match")
    try:
        r.reused = False  # type: ignore[misc]
        raise AssertionError("expected FrozenInstanceError")  # pragma: no cover
    except dataclasses.FrozenInstanceError:
        pass


class _LockCursor:
    def __init__(self, value: bool) -> None:
        self._value = value

    def fetchone(self) -> tuple[bool]:
        return (self._value,)


class _LockConnection:
    def __init__(self, sequence: list[bool]) -> None:
        self.sequence = list(sequence)
        self.calls = 0

    def execute(
        self,
        _query: str,
        _params: tuple[object, ...] | None = None,
    ) -> _LockCursor:
        self.calls += 1
        if self.sequence:
            return _LockCursor(self.sequence.pop(0))
        return _LockCursor(False)


class _Counter:
    def __init__(self) -> None:
        self.value = 0

    def increment(self, amount: int = 1) -> None:
        self.value += amount


class _Metrics:
    def __init__(self) -> None:
        self.advisory_lock_acquired = _Counter()
        self.advisory_lock_timeouts = _Counter()


def test_try_acquire_advisory_lock_true() -> None:
    conn = _LockConnection([True])
    assert try_acquire_advisory_lock(conn, request_key="rk_1") is True
    assert conn.calls == 1


def test_acquire_advisory_lock_with_timeout_success(monkeypatch) -> None:
    conn = _LockConnection([False, False, True])
    metrics = _Metrics()
    times = iter([0.0, 0.001, 0.002, 0.003, 0.004])
    monkeypatch.setattr("mcp_artifact_gateway.cache.reuse.time.monotonic", lambda: next(times))
    monkeypatch.setattr("mcp_artifact_gateway.cache.reuse.time.sleep", lambda _seconds: None)

    acquired = acquire_advisory_lock(
        conn,
        request_key="rk_2",
        timeout_ms=10,
        poll_interval_ms=1,
        metrics=metrics,
    )
    assert acquired is True
    assert metrics.advisory_lock_acquired.value == 1
    assert metrics.advisory_lock_timeouts.value == 0


def test_acquire_advisory_lock_with_timeout_failure(monkeypatch) -> None:
    conn = _LockConnection([False, False, False])
    metrics = _Metrics()
    times = iter([0.0, 0.005, 0.010, 0.011])
    monkeypatch.setattr("mcp_artifact_gateway.cache.reuse.time.monotonic", lambda: next(times))
    monkeypatch.setattr("mcp_artifact_gateway.cache.reuse.time.sleep", lambda _seconds: None)

    acquired = acquire_advisory_lock(
        conn,
        request_key="rk_3",
        timeout_ms=10,
        poll_interval_ms=1,
        metrics=metrics,
    )
    assert acquired is False
    assert metrics.advisory_lock_acquired.value == 0
    assert metrics.advisory_lock_timeouts.value == 1


# ---- check_reuse_candidate metrics wiring ----


def test_check_reuse_candidate_increments_cache_hit_metric() -> None:
    """check_reuse_candidate increments cache_hits on successful reuse."""
    metrics = GatewayMetrics()
    row = {
        "artifact_id": "art_hit",
        "payload_hash_full": "phash",
        "upstream_tool_schema_hash": "schema_v1",
        "map_status": "mapped",
        "generation": 1,
    }
    result = check_reuse_candidate(
        row,
        expected_schema_hash="schema_v1",
        metrics=metrics,
    )
    assert result.reused is True
    assert metrics.cache_hits.value == 1
    assert metrics.cache_misses.value == 0


def test_check_reuse_candidate_increments_cache_miss_on_none() -> None:
    """check_reuse_candidate increments cache_misses when candidate is None."""
    metrics = GatewayMetrics()
    result = check_reuse_candidate(
        None,
        expected_schema_hash="schema_v1",
        metrics=metrics,
    )
    assert result.reused is False
    assert metrics.cache_misses.value == 1
    assert metrics.cache_hits.value == 0


def test_check_reuse_candidate_increments_cache_miss_on_schema_mismatch() -> None:
    """check_reuse_candidate increments cache_misses on schema mismatch."""
    metrics = GatewayMetrics()
    row = {
        "artifact_id": "art_miss",
        "payload_hash_full": "phash",
        "upstream_tool_schema_hash": "schema_v1",
        "map_status": "mapped",
        "generation": 1,
    }
    result = check_reuse_candidate(
        row,
        expected_schema_hash="schema_v2",
        strict_schema_reuse=True,
        metrics=metrics,
    )
    assert result.reused is False
    assert metrics.cache_misses.value == 1
    assert metrics.cache_hits.value == 0


# ---- acquire_advisory_lock_async ----


def test_acquire_advisory_lock_async_success(monkeypatch) -> None:
    conn = _LockConnection([False, False, True])
    metrics = _Metrics()

    async def _noop_sleep(_seconds):
        pass

    monkeypatch.setattr("mcp_artifact_gateway.cache.reuse.asyncio.sleep", _noop_sleep)

    acquired = asyncio.run(
        acquire_advisory_lock_async(
            conn,
            request_key="rk_async_1",
            timeout_ms=5000,
            poll_interval_ms=1,
            metrics=metrics,
        )
    )
    assert acquired is True
    assert conn.calls == 3
    assert metrics.advisory_lock_acquired.value == 1
    assert metrics.advisory_lock_timeouts.value == 0


def test_acquire_advisory_lock_async_timeout(monkeypatch) -> None:
    conn = _LockConnection([False])
    metrics = _Metrics()

    acquired = asyncio.run(
        acquire_advisory_lock_async(
            conn,
            request_key="rk_async_2",
            timeout_ms=0,
            poll_interval_ms=1,
            metrics=metrics,
        )
    )
    assert acquired is False
    assert metrics.advisory_lock_acquired.value == 0
    assert metrics.advisory_lock_timeouts.value == 1


# ---- SQLite advisory lock bypass tests ----


def test_try_acquire_advisory_lock_sqlite_returns_true() -> None:
    """Advisory lock with real SQLite connection always returns True."""
    import sqlite3

    conn = sqlite3.connect(":memory:")
    result = try_acquire_advisory_lock(conn, request_key="rk_sqlite_1")
    assert result is True
    conn.close()


def test_try_acquire_advisory_lock_sqlite_no_sql_executed() -> None:
    """SQLite advisory lock returns True without executing any SQL."""
    import sqlite3

    conn = sqlite3.connect(":memory:")
    # If it tried to execute pg_try_advisory_xact_lock, it would raise
    result = try_acquire_advisory_lock(conn, request_key="rk_sqlite_2")
    assert result is True
    conn.close()
