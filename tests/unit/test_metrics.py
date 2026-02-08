from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from mcp_artifact_gateway.obs.metrics import Counter, GatewayMetrics, Histogram, get_metrics


def test_counter_increment_and_value() -> None:
    c = Counter()
    assert c.value == 0
    c.increment()
    assert c.value == 1
    c.increment(5)
    assert c.value == 6


def test_counter_reset_returns_previous_value() -> None:
    c = Counter()
    c.increment(10)
    val = c.reset()
    assert val == 10
    assert c.value == 0


def test_counter_reset_when_zero() -> None:
    c = Counter()
    val = c.reset()
    assert val == 0
    assert c.value == 0


def test_histogram_observe_and_snapshot() -> None:
    h = Histogram()
    h.observe(10.0)
    h.observe(20.0)
    h.observe(30.0)

    snap = h.snapshot()
    assert snap["min"] == 10.0
    assert snap["max"] == 30.0
    assert snap["sum"] == 60.0
    assert snap["count"] == 3.0
    assert snap["avg"] == 20.0


def test_histogram_snapshot_empty() -> None:
    h = Histogram()
    snap = h.snapshot()
    assert snap == {"min": 0, "max": 0, "sum": 0, "count": 0, "avg": 0}


def test_histogram_reset() -> None:
    h = Histogram()
    h.observe(5.0)
    h.observe(15.0)

    snap = h.reset()
    assert snap["min"] == 5.0
    assert snap["max"] == 15.0
    assert snap["count"] == 2.0

    # After reset, should be empty
    empty_snap = h.snapshot()
    assert empty_snap["count"] == 0


def test_gateway_metrics_record_stop_reason_all() -> None:
    m = GatewayMetrics()

    reasons = ["none", "max_bytes", "max_compute", "max_depth", "parse_error"]
    for reason in reasons:
        m.record_stop_reason(reason)

    assert m.mapping_stop_none.value == 1
    assert m.mapping_stop_max_bytes.value == 1
    assert m.mapping_stop_max_compute.value == 1
    assert m.mapping_stop_max_depth.value == 1
    assert m.mapping_stop_parse_error.value == 1


def test_gateway_metrics_record_stop_reason_unknown_ignored() -> None:
    m = GatewayMetrics()
    m.record_stop_reason("unknown_reason")
    # Should not raise and all counters should remain 0
    assert m.mapping_stop_none.value == 0


def test_gateway_metrics_record_cursor_stale_reason() -> None:
    m = GatewayMetrics()

    reasons = [
        "sample_set_mismatch",
        "map_budget_mismatch",
        "where_mode_mismatch",
        "traversal_version_mismatch",
        "generation_mismatch",
    ]
    for reason in reasons:
        m.record_cursor_stale_reason(reason)

    assert m.cursor_stale_sample_set.value == 1
    assert m.cursor_stale_map_budget.value == 1
    assert m.cursor_stale_where_mode.value == 1
    assert m.cursor_stale_traversal.value == 1
    assert m.cursor_stale_generation.value == 1


def test_gateway_metrics_record_cursor_stale_reason_unknown_ignored() -> None:
    m = GatewayMetrics()
    m.record_cursor_stale_reason("nonexistent")
    assert m.cursor_stale_sample_set.value == 0


def test_gateway_metrics_snapshot_returns_complete_dict() -> None:
    m = GatewayMetrics()
    m.cache_hits.increment(3)
    m.upstream_calls.increment(1)
    m.prune_soft_deletes.increment(5)

    snap = m.snapshot()

    # Check all top-level keys exist
    assert "cache" in snap
    assert "upstream" in snap
    assert "ingest" in snap
    assert "mapping" in snap
    assert "cursor" in snap
    assert "locks" in snap
    assert "pruning" in snap

    # Check specific values
    assert snap["cache"]["hits"] == 3
    assert snap["upstream"]["calls"] == 1
    assert snap["pruning"]["soft_deletes"] == 5

    # Check nested structures
    assert "stop_reasons" in snap["mapping"]
    assert "stale" in snap["cursor"]
    assert "latency" in snap["upstream"]
    assert "latency" in snap["mapping"]


def test_counter_thread_safety() -> None:
    """Test that concurrent increments produce correct total."""
    c = Counter()
    num_threads = 10
    increments_per_thread = 1000

    def worker() -> None:
        for _ in range(increments_per_thread):
            c.increment()

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(worker) for _ in range(num_threads)]
        for f in futures:
            f.result()

    assert c.value == num_threads * increments_per_thread


def test_histogram_thread_safety() -> None:
    """Test that concurrent observations produce correct count."""
    h = Histogram()
    num_threads = 10
    observations_per_thread = 1000

    def worker() -> None:
        for i in range(observations_per_thread):
            h.observe(float(i))

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(worker) for _ in range(num_threads)]
        for f in futures:
            f.result()

    snap = h.snapshot()
    assert snap["count"] == num_threads * observations_per_thread


def test_get_metrics_returns_singleton() -> None:
    # Note: this tests the global singleton behavior
    m1 = get_metrics()
    m2 = get_metrics()
    assert m1 is m2
    assert isinstance(m1, GatewayMetrics)


def test_get_metrics_thread_safe_singleton_creation(monkeypatch: "pytest.MonkeyPatch") -> None:
    import mcp_artifact_gateway.obs.metrics as metrics_module

    monkeypatch.setattr(metrics_module, "_metrics", None)

    num_threads = 16
    barrier = threading.Barrier(num_threads)
    instance_ids: list[int] = []

    def worker() -> None:
        barrier.wait()
        instance_ids.append(id(get_metrics()))

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(worker) for _ in range(num_threads)]
        for future in futures:
            future.result()

    assert len(set(instance_ids)) == 1


def test_gateway_metrics_reset_returns_snapshot_and_clears() -> None:
    """GatewayMetrics.reset() should return a snapshot and zero all counters."""
    m = GatewayMetrics()
    m.cache_hits.increment(5)
    m.upstream_calls.increment(2)
    m.upstream_latency.observe(100.0)
    m.prune_soft_deletes.increment(3)
    m.mapping_full_count.increment(1)
    m.cursor_stale_sample_set.increment(7)
    m.advisory_lock_timeouts.increment(4)
    m.oversize_json_count.increment(6)

    snap = m.reset()

    # Verify the snapshot captured the values
    assert snap["cache"]["hits"] == 5
    assert snap["upstream"]["calls"] == 2
    assert snap["upstream"]["latency"]["count"] == 1.0
    assert snap["upstream"]["latency"]["sum"] == 100.0
    assert snap["pruning"]["soft_deletes"] == 3
    assert snap["mapping"]["full"] == 1
    assert snap["cursor"]["stale"]["sample_set"] == 7
    assert snap["locks"]["timeouts"] == 4
    assert snap["ingest"]["oversize_json"] == 6

    # Verify all counters are zeroed after reset
    post_snap = m.snapshot()
    assert post_snap["cache"]["hits"] == 0
    assert post_snap["upstream"]["calls"] == 0
    assert post_snap["upstream"]["latency"]["count"] == 0
    assert post_snap["pruning"]["soft_deletes"] == 0
    assert post_snap["mapping"]["full"] == 0
    assert post_snap["cursor"]["stale"]["sample_set"] == 0
    assert post_snap["locks"]["timeouts"] == 0
    assert post_snap["ingest"]["oversize_json"] == 0


def test_gateway_metrics_reset_has_same_keys_as_snapshot() -> None:
    """GatewayMetrics.reset() should return the same top-level structure as snapshot()."""
    m = GatewayMetrics()
    snap_keys = set(m.snapshot().keys())
    reset_keys = set(m.reset().keys())
    assert snap_keys == reset_keys


def test_gateway_metrics_reset_empty_is_safe() -> None:
    """GatewayMetrics.reset() on a fresh instance should not raise."""
    m = GatewayMetrics()
    snap = m.reset()
    assert snap["cache"]["hits"] == 0
    assert snap["upstream"]["latency"]["count"] == 0
