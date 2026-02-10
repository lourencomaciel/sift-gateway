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
from mcp_artifact_gateway.obs.metrics import GatewayMetrics, counter_value

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

    def inc(self, amount: int = 1) -> None:
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
    assert counter_value(metrics.cache_hits) == 1
    assert counter_value(metrics.cache_misses) == 0


def test_check_reuse_candidate_increments_cache_miss_on_none() -> None:
    """check_reuse_candidate increments cache_misses when candidate is None."""
    metrics = GatewayMetrics()
    result = check_reuse_candidate(
        None,
        expected_schema_hash="schema_v1",
        metrics=metrics,
    )
    assert result.reused is False
    assert counter_value(metrics.cache_misses) == 1
    assert counter_value(metrics.cache_hits) == 0


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
    assert counter_value(metrics.cache_misses) == 1
    assert counter_value(metrics.cache_hits) == 0


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


# ---- SQLite advisory lock tests ----


def test_try_acquire_advisory_lock_sqlite_returns_true() -> None:
    """Advisory lock with real SQLite connection acquires per-key lock."""
    import sqlite3

    from mcp_artifact_gateway.cache.reuse import release_advisory_lock

    conn = sqlite3.connect(":memory:")
    result = try_acquire_advisory_lock(conn, request_key="rk_sqlite_1")
    assert result is True
    release_advisory_lock(conn, request_key="rk_sqlite_1")
    conn.close()


def test_try_acquire_advisory_lock_sqlite_no_sql_executed() -> None:
    """SQLite advisory lock acquires without executing any SQL."""
    import sqlite3

    from mcp_artifact_gateway.cache.reuse import release_advisory_lock

    conn = sqlite3.connect(":memory:")
    # If it tried to execute pg_try_advisory_xact_lock, it would raise
    result = try_acquire_advisory_lock(conn, request_key="rk_sqlite_2")
    assert result is True
    release_advisory_lock(conn, request_key="rk_sqlite_2")
    conn.close()


def test_try_acquire_advisory_lock_sqlite_second_acquire_fails() -> None:
    """Second acquire for same key on SQLite returns False (lock held)."""
    import sqlite3

    from mcp_artifact_gateway.cache.reuse import release_advisory_lock

    conn = sqlite3.connect(":memory:")
    assert try_acquire_advisory_lock(conn, request_key="rk_dup") is True
    assert try_acquire_advisory_lock(conn, request_key="rk_dup") is False
    release_advisory_lock(conn, request_key="rk_dup")
    conn.close()


def test_try_acquire_advisory_lock_sqlite_release_then_reacquire() -> None:
    """After release, acquire succeeds again on SQLite."""
    import sqlite3

    from mcp_artifact_gateway.cache.reuse import release_advisory_lock

    conn = sqlite3.connect(":memory:")
    assert try_acquire_advisory_lock(conn, request_key="rk_rel") is True
    release_advisory_lock(conn, request_key="rk_rel")
    assert try_acquire_advisory_lock(conn, request_key="rk_rel") is True
    release_advisory_lock(conn, request_key="rk_rel")
    conn.close()


def test_try_acquire_advisory_lock_sqlite_different_keys() -> None:
    """Different keys can both acquire on SQLite (per-key granularity)."""
    import sqlite3

    from mcp_artifact_gateway.cache.reuse import release_advisory_lock

    conn = sqlite3.connect(":memory:")
    assert try_acquire_advisory_lock(conn, request_key="rk_a") is True
    assert try_acquire_advisory_lock(conn, request_key="rk_b") is True
    release_advisory_lock(conn, request_key="rk_a")
    release_advisory_lock(conn, request_key="rk_b")
    conn.close()


def test_release_advisory_lock_noop_for_postgres() -> None:
    """release_advisory_lock is a no-op for non-SQLite connections."""
    from mcp_artifact_gateway.cache.reuse import release_advisory_lock

    conn = _LockConnection([True])
    # Should not raise
    release_advisory_lock(conn, request_key="rk_pg")


# ---- SQLite lock contention under concurrent access ----


def test_sqlite_lock_contention_only_one_thread_wins() -> None:
    """When multiple threads race for the same key, exactly one acquires."""
    import sqlite3
    import threading
    from concurrent.futures import ThreadPoolExecutor

    from mcp_artifact_gateway.cache.reuse import release_advisory_lock

    conn = sqlite3.connect(":memory:")
    key = "rk_contention_1"
    num_threads = 20
    barrier = threading.Barrier(num_threads)
    results: list[bool] = []
    lock = threading.Lock()

    def worker() -> None:
        barrier.wait()
        acquired = try_acquire_advisory_lock(conn, request_key=key)
        with lock:
            results.append(acquired)

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(worker) for _ in range(num_threads)]
        for f in futures:
            f.result()

    assert results.count(True) == 1, (
        f"Expected exactly 1 winner, got {results.count(True)} out of {num_threads}"
    )
    assert results.count(False) == num_threads - 1
    release_advisory_lock(conn, request_key=key)
    conn.close()


def test_sqlite_lock_contention_different_keys_independent() -> None:
    """Threads competing for different keys should all succeed."""
    import sqlite3
    import threading
    from concurrent.futures import ThreadPoolExecutor

    from mcp_artifact_gateway.cache.reuse import release_advisory_lock

    conn = sqlite3.connect(":memory:")
    num_threads = 10
    barrier = threading.Barrier(num_threads)
    results: list[tuple[str, bool]] = []
    lock = threading.Lock()

    def worker(idx: int) -> None:
        key = f"rk_independent_{idx}"
        barrier.wait()
        acquired = try_acquire_advisory_lock(conn, request_key=key)
        with lock:
            results.append((key, acquired))

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(worker, i) for i in range(num_threads)]
        for f in futures:
            f.result()

    # All should win since keys are unique
    assert all(acquired for _, acquired in results), (
        f"Expected all threads to acquire, got: {results}"
    )
    for i in range(num_threads):
        release_advisory_lock(conn, request_key=f"rk_independent_{i}")
    conn.close()


def test_sqlite_lock_release_unblocks_next_acquirer() -> None:
    """After thread A releases, thread B can acquire the same key."""
    import sqlite3
    import threading

    from mcp_artifact_gateway.cache.reuse import release_advisory_lock

    conn = sqlite3.connect(":memory:")
    key = "rk_release_unblock"
    b_acquired = threading.Event()
    b_result: list[bool] = []

    # Thread A acquires
    assert try_acquire_advisory_lock(conn, request_key=key) is True

    def thread_b() -> None:
        # Poll until we can acquire (thread A will release)
        for _ in range(100):
            if try_acquire_advisory_lock(conn, request_key=key):
                b_result.append(True)
                b_acquired.set()
                return
            threading.Event().wait(0.01)
        b_result.append(False)
        b_acquired.set()

    t = threading.Thread(target=thread_b)
    t.start()

    # Give thread B a moment to fail its first attempt
    threading.Event().wait(0.05)
    # Release so thread B can acquire
    release_advisory_lock(conn, request_key=key)

    b_acquired.wait(timeout=5.0)
    t.join(timeout=5.0)

    assert b_result == [True], "Thread B should have acquired after A released"
    release_advisory_lock(conn, request_key=key)
    conn.close()


def test_sqlite_lock_acquire_with_timeout_concurrent(monkeypatch) -> None:
    """acquire_advisory_lock with timeout works under contention."""
    import sqlite3

    from mcp_artifact_gateway.cache.reuse import release_advisory_lock

    conn = sqlite3.connect(":memory:")
    key = "rk_timeout_contention"

    # Hold the lock
    assert try_acquire_advisory_lock(conn, request_key=key) is True

    # Another attempt with 0ms timeout should fail immediately
    acquired = acquire_advisory_lock(
        conn,
        request_key=key,
        timeout_ms=0,
        poll_interval_ms=1,
    )
    assert acquired is False

    # Release and try again — should succeed
    release_advisory_lock(conn, request_key=key)
    acquired = acquire_advisory_lock(
        conn,
        request_key=key,
        timeout_ms=1000,
        poll_interval_ms=1,
    )
    assert acquired is True
    release_advisory_lock(conn, request_key=key)
    conn.close()
