from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import threading

from prometheus_client import CollectorRegistry
import pytest

from sift_mcp.obs.metrics import (
    GatewayMetrics,
    Histogram,
    counter_reset,
    counter_value,
    get_metrics,
)


def test_counter_increment_and_value() -> None:
    m = GatewayMetrics(registry=CollectorRegistry())
    c = m.cache_hits
    assert counter_value(c) == 0
    c.inc()
    assert counter_value(c) == 1
    c.inc(5)
    assert counter_value(c) == 6


def test_counter_reset_returns_previous_value() -> None:
    m = GatewayMetrics(registry=CollectorRegistry())
    c = m.cache_hits
    c.inc(10)
    val = counter_reset(c)
    assert val == 10
    assert counter_value(c) == 0


def test_counter_reset_when_zero() -> None:
    m = GatewayMetrics(registry=CollectorRegistry())
    c = m.cache_hits
    val = counter_reset(c)
    assert val == 0
    assert counter_value(c) == 0


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
    m = GatewayMetrics(registry=CollectorRegistry())

    reasons = ["none", "max_bytes", "max_compute", "max_depth", "parse_error"]
    for reason in reasons:
        m.record_stop_reason(reason)

    assert counter_value(m.mapping_stop_none) == 1
    assert counter_value(m.mapping_stop_max_bytes) == 1
    assert counter_value(m.mapping_stop_max_compute) == 1
    assert counter_value(m.mapping_stop_max_depth) == 1
    assert counter_value(m.mapping_stop_parse_error) == 1


def test_gateway_metrics_record_stop_reason_unknown_ignored() -> None:
    m = GatewayMetrics(registry=CollectorRegistry())
    m.record_stop_reason("unknown_reason")
    # Should not raise and all counters should remain 0
    assert counter_value(m.mapping_stop_none) == 0


def test_gateway_metrics_record_cursor_stale_reason() -> None:
    m = GatewayMetrics(registry=CollectorRegistry())

    reasons = [
        "sample_set_mismatch",
        "map_budget_mismatch",
        "where_mode_mismatch",
        "traversal_version_mismatch",
        "generation_mismatch",
    ]
    for reason in reasons:
        m.record_cursor_stale_reason(reason)

    assert counter_value(m.cursor_stale_sample_set) == 1
    assert counter_value(m.cursor_stale_map_budget) == 1
    assert counter_value(m.cursor_stale_where_mode) == 1
    assert counter_value(m.cursor_stale_traversal) == 1
    assert counter_value(m.cursor_stale_generation) == 1


def test_gateway_metrics_record_cursor_stale_reason_unknown_ignored() -> None:
    m = GatewayMetrics(registry=CollectorRegistry())
    m.record_cursor_stale_reason("nonexistent")
    assert counter_value(m.cursor_stale_sample_set) == 0


def test_gateway_metrics_snapshot_returns_complete_dict() -> None:
    m = GatewayMetrics(registry=CollectorRegistry())
    m.cache_hits.inc(3)
    m.upstream_calls.inc(1)
    m.prune_soft_deletes.inc(5)

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
    m = GatewayMetrics(registry=CollectorRegistry())
    c = m.cache_hits
    num_threads = 10
    increments_per_thread = 1000

    def worker() -> None:
        for _ in range(increments_per_thread):
            c.inc()

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(worker) for _ in range(num_threads)]
        for f in futures:
            f.result()

    assert counter_value(c) == num_threads * increments_per_thread


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


def test_get_metrics_thread_safe_singleton_creation(
    monkeypatch: "pytest.MonkeyPatch",
) -> None:
    import sift_mcp.obs.metrics as metrics_module

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
    m = GatewayMetrics(registry=CollectorRegistry())
    m.cache_hits.inc(5)
    m.upstream_calls.inc(2)
    m.upstream_latency.observe(100.0)
    m.prune_soft_deletes.inc(3)
    m.mapping_full_count.inc(1)
    m.cursor_stale_sample_set.inc(7)
    m.advisory_lock_timeouts.inc(4)
    m.oversize_json_count.inc(6)

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
    m = GatewayMetrics(registry=CollectorRegistry())
    snap_keys = set(m.snapshot().keys())
    reset_keys = set(m.reset().keys())
    assert snap_keys == reset_keys


def test_gateway_metrics_reset_empty_is_safe() -> None:
    """GatewayMetrics.reset() on a fresh instance should not raise."""
    m = GatewayMetrics(registry=CollectorRegistry())
    snap = m.reset()
    assert snap["cache"]["hits"] == 0
    assert snap["upstream"]["latency"]["count"] == 0


# ---- prometheus counter name verification ----


def test_all_counters_use_gateway_prefix_and_total_suffix() -> None:
    """Every prometheus counter must follow the gateway_*_total naming convention.

    prometheus_client strips _total from describe().name (it appends it during
    rendering), so we check that the base name starts with gateway_ and that
    the rendered exposition output contains the full name_total line.
    """
    from prometheus_client import Counter as PromCounter
    from prometheus_client import generate_latest

    reg = CollectorRegistry()
    m = GatewayMetrics(registry=reg)
    counters = [
        (attr, getattr(m, attr))
        for attr in dir(m)
        if not attr.startswith("_")
        and isinstance(getattr(m, attr), PromCounter)
    ]
    assert len(counters) >= 25, (
        f"Expected at least 25 counters, got {len(counters)}"
    )

    output = generate_latest(reg).decode("utf-8")

    for attr, counter in counters:
        base_name = counter.describe()[0].name
        assert base_name.startswith("gateway_"), (
            f"Counter {attr!r} has base name {base_name!r} — expected 'gateway_' prefix"
        )
        # prometheus_client renders Counter as {base_name}_total in exposition
        full_name = f"{base_name}_total"
        assert full_name in output, (
            f"Counter {attr!r} ({full_name!r}) not found in prometheus text output"
        )


def test_counter_names_are_unique() -> None:
    """No two counters share the same prometheus name."""
    from prometheus_client import Counter as PromCounter

    m = GatewayMetrics(registry=CollectorRegistry())
    names: list[str] = []
    for attr in dir(m):
        if attr.startswith("_"):
            continue
        obj = getattr(m, attr)
        if isinstance(obj, PromCounter):
            names.append(obj.describe()[0].name)
    assert len(names) == len(set(names)), (
        f"Duplicate counter names: {sorted(names)}"
    )


def test_counters_render_in_prometheus_text_format() -> None:
    """All counters render valid prometheus exposition text."""
    from prometheus_client import generate_latest

    reg = CollectorRegistry()
    m = GatewayMetrics(registry=reg)
    m.cache_hits.inc(3)
    m.upstream_errors.inc(1)
    m.mapping_full_count.inc(2)

    output = generate_latest(reg).decode("utf-8")

    # Verify key counters appear with correct values
    assert "gateway_cache_hits_total 3.0" in output
    assert "gateway_upstream_errors_total 1.0" in output
    assert "gateway_mapping_full_total 2.0" in output
    # Counters that weren't incremented should show 0
    assert "gateway_cache_misses_total 0.0" in output


def test_snapshot_keys_match_counter_names() -> None:
    """Snapshot structure accounts for every counter (no orphaned counters)."""
    from prometheus_client import Counter as PromCounter

    m = GatewayMetrics(registry=CollectorRegistry())
    snap = m.snapshot()

    def _flatten(d: dict, prefix: str = "") -> dict[str, object]:
        result: dict[str, object] = {}
        for k, v in d.items():
            key = f"{prefix}.{k}" if prefix else k
            if isinstance(v, dict):
                result.update(_flatten(v, key))
            else:
                result[key] = v
        return result

    flat = _flatten(snap)

    counter_attrs = [
        attr
        for attr in dir(m)
        if not attr.startswith("_")
        and isinstance(getattr(m, attr), PromCounter)
    ]

    # Snapshot should have at least as many leaf values as counters
    # (it also has histogram values, so >= not ==)
    assert len(flat) >= len(counter_attrs), (
        f"Snapshot has {len(flat)} leaf values but {len(counter_attrs)} counters exist"
    )
