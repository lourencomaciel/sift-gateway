"""Collect gateway metrics via Prometheus counters and histograms.

Provides ``GatewayMetrics``, a central registry of Prometheus
counters and custom ``Histogram`` objects covering cache, ingest,
mapping, cursor, lock, pruning, and quota subsystems.  Also
exposes ``counter_value``/``counter_reset`` helpers and a
thread-safe ``get_metrics`` singleton accessor.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
import threading
from typing import Any

from prometheus_client import CollectorRegistry
from prometheus_client import Counter as _PromCounter

# ---------------------------------------------------------------------------
# Type alias for counter reader functions
# ---------------------------------------------------------------------------

_CounterFn = Callable[[_PromCounter], int]

# ---------------------------------------------------------------------------
# Helpers for reading / resetting prometheus counters
# ---------------------------------------------------------------------------


def counter_value(counter: _PromCounter) -> int:
    """Read the current value of a label-less Prometheus counter.

    Args:
        counter: A Prometheus Counter without labels.

    Returns:
        Current integer value of the counter.
    """
    return int(counter._value.get())


def counter_reset(counter: _PromCounter) -> int:
    """Reset a Prometheus counter to zero for testing.

    Only intended for use in tests; production counters are
    monotonic.

    Args:
        counter: A Prometheus Counter without labels.

    Returns:
        The counter value immediately before the reset.
    """
    val = int(counter._value.get())
    counter._value.set(0)
    return val


# ---------------------------------------------------------------------------
# Custom Histogram (prometheus has no min/max tracking)
# ---------------------------------------------------------------------------


@dataclass
class Histogram:
    """Thread-safe histogram tracking min, max, sum, and count.

    Unlike Prometheus histograms, this tracks exact min/max
    values and supports atomic snapshot-and-reset for periodic
    reporting.  Uses a threading lock internally.
    """

    _min: float = float("inf")
    _max: float = float("-inf")
    _sum: float = 0.0
    _count: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def observe(self, value: float) -> None:
        """Record a single observation.

        Args:
            value: Numeric value to record.
        """
        with self._lock:
            self._min = min(self._min, value)
            self._max = max(self._max, value)
            self._sum += value
            self._count += 1

    def snapshot(self) -> dict[str, float]:
        """Return current statistics without resetting.

        Returns:
            Dict with min, max, sum, count, and avg keys.
        """
        with self._lock:
            if self._count == 0:
                return {
                    "min": 0.0,
                    "max": 0.0,
                    "sum": 0.0,
                    "count": 0.0,
                    "avg": 0.0,
                }
            return {
                "min": self._min,
                "max": self._max,
                "sum": self._sum,
                "count": float(self._count),
                "avg": self._sum / self._count,
            }

    def reset(self) -> dict[str, float]:
        """Return current statistics and reset to initial state.

        Returns:
            Dict with min, max, sum, count, and avg keys
            reflecting values before the reset.
        """
        with self._lock:
            if self._count == 0:
                snap: dict[str, float] = {
                    "min": 0.0,
                    "max": 0.0,
                    "sum": 0.0,
                    "count": 0.0,
                    "avg": 0.0,
                }
            else:
                snap = {
                    "min": self._min,
                    "max": self._max,
                    "sum": self._sum,
                    "count": float(self._count),
                    "avg": self._sum / self._count,
                }
            self._min = float("inf")
            self._max = float("-inf")
            self._sum = 0.0
            self._count = 0
            return snap


# ---------------------------------------------------------------------------
# GatewayMetrics — counter factory
# ---------------------------------------------------------------------------


def _make(
    name: str,
    doc: str,
    registry: CollectorRegistry,
) -> _PromCounter:
    """Create and register a Prometheus counter.

    Args:
        name: Metric name (e.g. ``gateway_cache_hits_total``).
        doc: Human-readable description of the counter.
        registry: Prometheus collector registry to register in.

    Returns:
        A new Prometheus Counter instance.
    """
    return _PromCounter(name, doc, registry=registry)


# ---------------------------------------------------------------------------
# __init__ helpers — each registers one logical group of counters
# ---------------------------------------------------------------------------


def _init_cache_counters(m: GatewayMetrics, reg: CollectorRegistry) -> None:
    """Register cache and upstream counters.

    Args:
        m: Metrics instance to attach counters to.
        reg: Prometheus collector registry.
    """
    m.cache_hits = _make("gateway_cache_hits_total", "Cache hits", reg)
    m.cache_misses = _make("gateway_cache_misses_total", "Cache misses", reg)
    m.alias_hits = _make("gateway_alias_hits_total", "Alias hits", reg)
    m.upstream_calls = _make(
        "gateway_upstream_calls_total", "Upstream calls", reg
    )
    m.upstream_errors = _make(
        "gateway_upstream_errors_total", "Upstream errors", reg
    )
    m.upstream_latency = Histogram()


def _init_ingest_counters(m: GatewayMetrics, reg: CollectorRegistry) -> None:
    """Register ingest counters.

    Args:
        m: Metrics instance to attach counters to.
        reg: Prometheus collector registry.
    """
    m.oversize_json_count = _make(
        "gateway_oversize_json_total",
        "Oversize JSON parts",
        reg,
    )
    m.binary_blob_writes = _make(
        "gateway_binary_blob_writes_total",
        "Binary blob writes",
        reg,
    )
    m.binary_blob_dedupes = _make(
        "gateway_binary_blob_dedupes_total",
        "Binary blob dedupes",
        reg,
    )


def _init_mapping_counters(m: GatewayMetrics, reg: CollectorRegistry) -> None:
    """Register mapping result counters and latency.

    Args:
        m: Metrics instance to attach counters to.
        reg: Prometheus collector registry.
    """
    m.mapping_full_count = _make(
        "gateway_mapping_full_total", "Full mappings", reg
    )
    m.mapping_partial_count = _make(
        "gateway_mapping_partial_total",
        "Partial mappings",
        reg,
    )
    m.mapping_failed_count = _make(
        "gateway_mapping_failed_total",
        "Failed mappings",
        reg,
    )
    m.mapping_latency = Histogram()


def _init_mapping_stop_counters(
    m: GatewayMetrics, reg: CollectorRegistry
) -> None:
    """Register mapping stop-reason counters.

    Args:
        m: Metrics instance to attach counters to.
        reg: Prometheus collector registry.
    """
    m.mapping_stop_none = _make(
        "gateway_mapping_stop_none_total",
        "Mapping stop: none",
        reg,
    )
    m.mapping_stop_max_bytes = _make(
        "gateway_mapping_stop_max_bytes_total",
        "Mapping stop: max_bytes",
        reg,
    )
    m.mapping_stop_max_compute = _make(
        "gateway_mapping_stop_max_compute_total",
        "Mapping stop: max_compute",
        reg,
    )
    m.mapping_stop_max_depth = _make(
        "gateway_mapping_stop_max_depth_total",
        "Mapping stop: max_depth",
        reg,
    )
    m.mapping_stop_parse_error = _make(
        "gateway_mapping_stop_parse_error_total",
        "Mapping stop: parse_error",
        reg,
    )


def _init_cursor_counters(m: GatewayMetrics, reg: CollectorRegistry) -> None:
    """Register cursor-stale, invalid, and expired counters.

    Args:
        m: Metrics instance to attach counters to.
        reg: Prometheus collector registry.
    """
    m.cursor_stale_sample_set = _make(
        "gateway_cursor_stale_sample_set_total",
        "Cursor stale: sample_set",
        reg,
    )
    m.cursor_stale_map_budget = _make(
        "gateway_cursor_stale_map_budget_total",
        "Cursor stale: map_budget",
        reg,
    )
    m.cursor_stale_where_mode = _make(
        "gateway_cursor_stale_where_mode_total",
        "Cursor stale: where_mode",
        reg,
    )
    m.cursor_stale_traversal = _make(
        "gateway_cursor_stale_traversal_total",
        "Cursor stale: traversal",
        reg,
    )
    m.cursor_stale_generation = _make(
        "gateway_cursor_stale_generation_total",
        "Cursor stale: generation",
        reg,
    )
    m.cursor_invalid = _make(
        "gateway_cursor_invalid_total",
        "Invalid cursors",
        reg,
    )
    m.cursor_expired = _make(
        "gateway_cursor_expired_total",
        "Expired cursors",
        reg,
    )


def _init_lock_counters(m: GatewayMetrics, reg: CollectorRegistry) -> None:
    """Register advisory-lock counters.

    Args:
        m: Metrics instance to attach counters to.
        reg: Prometheus collector registry.
    """
    m.advisory_lock_timeouts = _make(
        "gateway_advisory_lock_timeouts_total",
        "Advisory lock timeouts",
        reg,
    )
    m.advisory_lock_acquired = _make(
        "gateway_advisory_lock_acquired_total",
        "Advisory locks acquired",
        reg,
    )


def _init_pruning_counters(m: GatewayMetrics, reg: CollectorRegistry) -> None:
    """Register pruning counters.

    Args:
        m: Metrics instance to attach counters to.
        reg: Prometheus collector registry.
    """
    m.prune_soft_deletes = _make(
        "gateway_prune_soft_deletes_total",
        "Soft deletes",
        reg,
    )
    m.prune_hard_deletes = _make(
        "gateway_prune_hard_deletes_total",
        "Hard deletes",
        reg,
    )
    m.prune_bytes_reclaimed = _make(
        "gateway_prune_bytes_reclaimed_total",
        "Bytes reclaimed",
        reg,
    )
    m.prune_fs_orphans_removed = _make(
        "gateway_prune_fs_orphans_removed_total",
        "FS orphans removed",
        reg,
    )


def _init_quota_counters(m: GatewayMetrics, reg: CollectorRegistry) -> None:
    """Register quota counters.

    Args:
        m: Metrics instance to attach counters to.
        reg: Prometheus collector registry.
    """
    m.quota_checks = _make("gateway_quota_checks_total", "Quota checks", reg)
    m.quota_breaches = _make(
        "gateway_quota_breaches_total",
        "Quota breaches",
        reg,
    )
    m.quota_prune_triggered = _make(
        "gateway_quota_prune_triggered_total",
        "Quota prune triggered",
        reg,
    )


def _init_codegen_counters(m: GatewayMetrics, reg: CollectorRegistry) -> None:
    """Register code-query execution counters and latency."""
    m.codegen_executions = _make(
        "gateway_codegen_executions_total",
        "Code-query executions",
        reg,
    )
    m.codegen_success = _make(
        "gateway_codegen_success_total",
        "Code-query successes",
        reg,
    )
    m.codegen_failure = _make(
        "gateway_codegen_failure_total",
        "Code-query failures",
        reg,
    )
    m.codegen_timeout = _make(
        "gateway_codegen_timeout_total",
        "Code-query timeouts",
        reg,
    )
    m.codegen_input_records = _make(
        "gateway_codegen_input_records_total",
        "Code-query input records",
        reg,
    )
    m.codegen_output_records = _make(
        "gateway_codegen_output_records_total",
        "Code-query output records",
        reg,
    )
    m.codegen_latency = Histogram()


# ---------------------------------------------------------------------------
# snapshot / reset helpers — parameterized by counter reader fn
# ---------------------------------------------------------------------------


def _gather_cache(m: GatewayMetrics, fn: _CounterFn) -> dict[str, Any]:
    """Gather cache section metrics.

    Args:
        m: Metrics instance to read from.
        fn: Counter reader function (value or reset).

    Returns:
        Dict of cache metric values.
    """
    return {
        "hits": fn(m.cache_hits),
        "misses": fn(m.cache_misses),
        "alias_hits": fn(m.alias_hits),
    }


def _gather_upstream(
    m: GatewayMetrics,
    fn: _CounterFn,
    *,
    reset: bool,
) -> dict[str, Any]:
    """Gather upstream section metrics.

    Args:
        m: Metrics instance to read from.
        fn: Counter reader function (value or reset).
        reset: If True, reset the latency histogram.

    Returns:
        Dict of upstream metric values including latency.
    """
    lat = (
        m.upstream_latency.reset() if reset else (m.upstream_latency.snapshot())
    )
    return {
        "calls": fn(m.upstream_calls),
        "errors": fn(m.upstream_errors),
        "latency": lat,
    }


def _gather_ingest(m: GatewayMetrics, fn: _CounterFn) -> dict[str, Any]:
    """Gather ingest section metrics.

    Args:
        m: Metrics instance to read from.
        fn: Counter reader function (value or reset).

    Returns:
        Dict of ingest metric values.
    """
    return {
        "oversize_json": fn(m.oversize_json_count),
        "blob_writes": fn(m.binary_blob_writes),
        "blob_dedupes": fn(m.binary_blob_dedupes),
    }


def _gather_mapping(
    m: GatewayMetrics,
    fn: _CounterFn,
    *,
    reset: bool,
) -> dict[str, Any]:
    """Gather mapping section metrics.

    Args:
        m: Metrics instance to read from.
        fn: Counter reader function (value or reset).
        reset: If True, reset the latency histogram.

    Returns:
        Dict of mapping metric values including latency.
    """
    lat = m.mapping_latency.reset() if reset else (m.mapping_latency.snapshot())
    return {
        "full": fn(m.mapping_full_count),
        "partial": fn(m.mapping_partial_count),
        "failed": fn(m.mapping_failed_count),
        "latency": lat,
        "stop_reasons": {
            "none": fn(m.mapping_stop_none),
            "max_bytes": fn(m.mapping_stop_max_bytes),
            "max_compute": fn(m.mapping_stop_max_compute),
            "max_depth": fn(m.mapping_stop_max_depth),
            "parse_error": fn(m.mapping_stop_parse_error),
        },
    }


def _gather_cursor(m: GatewayMetrics, fn: _CounterFn) -> dict[str, Any]:
    """Gather cursor section metrics.

    Args:
        m: Metrics instance to read from.
        fn: Counter reader function (value or reset).

    Returns:
        Dict of cursor metric values.
    """
    return {
        "stale": {
            "sample_set": fn(m.cursor_stale_sample_set),
            "map_budget": fn(m.cursor_stale_map_budget),
            "where_mode": fn(m.cursor_stale_where_mode),
            "traversal": fn(m.cursor_stale_traversal),
            "generation": fn(m.cursor_stale_generation),
        },
        "invalid": fn(m.cursor_invalid),
        "expired": fn(m.cursor_expired),
    }


def _gather_locks(m: GatewayMetrics, fn: _CounterFn) -> dict[str, Any]:
    """Gather advisory-lock section metrics.

    Args:
        m: Metrics instance to read from.
        fn: Counter reader function (value or reset).

    Returns:
        Dict of advisory-lock metric values.
    """
    return {
        "timeouts": fn(m.advisory_lock_timeouts),
        "acquired": fn(m.advisory_lock_acquired),
    }


def _gather_pruning(m: GatewayMetrics, fn: _CounterFn) -> dict[str, Any]:
    """Gather pruning section metrics.

    Args:
        m: Metrics instance to read from.
        fn: Counter reader function (value or reset).

    Returns:
        Dict of pruning metric values.
    """
    return {
        "soft_deletes": fn(m.prune_soft_deletes),
        "hard_deletes": fn(m.prune_hard_deletes),
        "bytes_reclaimed": fn(m.prune_bytes_reclaimed),
        "fs_orphans_removed": fn(m.prune_fs_orphans_removed),
    }


def _gather_quota(m: GatewayMetrics, fn: _CounterFn) -> dict[str, Any]:
    """Gather quota section metrics.

    Args:
        m: Metrics instance to read from.
        fn: Counter reader function (value or reset).

    Returns:
        Dict of quota metric values.
    """
    return {
        "checks": fn(m.quota_checks),
        "breaches": fn(m.quota_breaches),
        "prune_triggered": fn(m.quota_prune_triggered),
    }


def _gather_codegen(
    m: GatewayMetrics,
    fn: _CounterFn,
    *,
    reset: bool,
) -> dict[str, Any]:
    """Gather code-query section metrics."""
    lat = m.codegen_latency.reset() if reset else (m.codegen_latency.snapshot())
    return {
        "executions": fn(m.codegen_executions),
        "success": fn(m.codegen_success),
        "failure": fn(m.codegen_failure),
        "timeout": fn(m.codegen_timeout),
        "input_records": fn(m.codegen_input_records),
        "output_records": fn(m.codegen_output_records),
        "latency": lat,
    }


def _gather_all(
    m: GatewayMetrics,
    fn: _CounterFn,
    *,
    reset: bool,
) -> dict[str, Any]:
    """Collect all metric sections into a single dict.

    Args:
        m: Metrics instance to read from.
        fn: Counter reader function (value or reset).
        reset: If True, reset histogram state after reading.

    Returns:
        Nested dict keyed by subsystem name.
    """
    return {
        "cache": _gather_cache(m, fn),
        "upstream": _gather_upstream(m, fn, reset=reset),
        "ingest": _gather_ingest(m, fn),
        "mapping": _gather_mapping(m, fn, reset=reset),
        "cursor": _gather_cursor(m, fn),
        "locks": _gather_locks(m, fn),
        "pruning": _gather_pruning(m, fn),
        "quota": _gather_quota(m, fn),
        "codegen": _gather_codegen(m, fn, reset=reset),
    }


# ---------------------------------------------------------------------------
# GatewayMetrics
# ---------------------------------------------------------------------------


class GatewayMetrics:
    """Central metrics registry for the gateway.

    Registers Prometheus counters and custom Histogram objects
    for every observable subsystem.  Provides ``snapshot()`` and
    ``reset()`` methods that return all metrics as a nested dict,
    plus helpers to increment stop-reason and cursor-stale
    counters by reason string.
    """

    cache_hits: _PromCounter
    cache_misses: _PromCounter
    alias_hits: _PromCounter
    upstream_calls: _PromCounter
    upstream_errors: _PromCounter
    upstream_latency: Histogram

    oversize_json_count: _PromCounter
    binary_blob_writes: _PromCounter
    binary_blob_dedupes: _PromCounter

    mapping_full_count: _PromCounter
    mapping_partial_count: _PromCounter
    mapping_failed_count: _PromCounter
    mapping_latency: Histogram

    mapping_stop_none: _PromCounter
    mapping_stop_max_bytes: _PromCounter
    mapping_stop_max_compute: _PromCounter
    mapping_stop_max_depth: _PromCounter
    mapping_stop_parse_error: _PromCounter

    cursor_stale_sample_set: _PromCounter
    cursor_stale_map_budget: _PromCounter
    cursor_stale_where_mode: _PromCounter
    cursor_stale_traversal: _PromCounter
    cursor_stale_generation: _PromCounter
    cursor_invalid: _PromCounter
    cursor_expired: _PromCounter

    advisory_lock_timeouts: _PromCounter
    advisory_lock_acquired: _PromCounter

    prune_soft_deletes: _PromCounter
    prune_hard_deletes: _PromCounter
    prune_bytes_reclaimed: _PromCounter
    prune_fs_orphans_removed: _PromCounter

    quota_checks: _PromCounter
    quota_breaches: _PromCounter
    quota_prune_triggered: _PromCounter

    codegen_executions: _PromCounter
    codegen_success: _PromCounter
    codegen_failure: _PromCounter
    codegen_timeout: _PromCounter
    codegen_input_records: _PromCounter
    codegen_output_records: _PromCounter
    codegen_latency: Histogram

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        """Initialize and register all gateway metric counters.

        Args:
            registry: Optional Prometheus registry; creates a new
                one if not provided.
        """
        reg = registry or CollectorRegistry()
        _init_cache_counters(self, reg)
        _init_ingest_counters(self, reg)
        _init_mapping_counters(self, reg)
        _init_mapping_stop_counters(self, reg)
        _init_cursor_counters(self, reg)
        _init_lock_counters(self, reg)
        _init_pruning_counters(self, reg)
        _init_quota_counters(self, reg)
        _init_codegen_counters(self, reg)

    def record_stop_reason(self, reason: str) -> None:
        """Increment the counter for a mapping stop reason.

        Args:
            reason: Stop reason key (e.g. ``max_bytes``).
        """
        counter_map = {
            "none": self.mapping_stop_none,
            "max_bytes": self.mapping_stop_max_bytes,
            "max_compute": self.mapping_stop_max_compute,
            "max_depth": self.mapping_stop_max_depth,
            "parse_error": self.mapping_stop_parse_error,
        }
        c = counter_map.get(reason)
        if c is not None:
            c.inc()

    def record_cursor_stale_reason(self, reason: str) -> None:
        """Increment the counter for a cursor stale reason.

        Args:
            reason: Stale reason key (e.g.
                ``sample_set_mismatch``).
        """
        counter_map = {
            "sample_set_mismatch": self.cursor_stale_sample_set,
            "map_budget_mismatch": self.cursor_stale_map_budget,
            "where_mode_mismatch": self.cursor_stale_where_mode,
            "traversal_version_mismatch": (self.cursor_stale_traversal),
            "generation_mismatch": (self.cursor_stale_generation),
        }
        c = counter_map.get(reason)
        if c is not None:
            c.inc()

    def snapshot(self) -> dict[str, Any]:
        """Return a snapshot of all metrics.

        Returns:
            Nested dict of all subsystem metric values.
        """
        return _gather_all(self, counter_value, reset=False)

    def reset(self) -> dict[str, Any]:
        """Reset all metrics and return pre-reset snapshot.

        Returns:
            Nested dict of all subsystem metric values before
            the reset.
        """
        return _gather_all(self, counter_reset, reset=True)


# Global singleton
_metrics: GatewayMetrics | None = None
_metrics_lock = threading.Lock()


def get_metrics() -> GatewayMetrics:
    """Get or create the global metrics singleton (thread-safe).

    Returns:
        The shared GatewayMetrics instance.
    """
    global _metrics
    with _metrics_lock:
        if _metrics is None:
            _metrics = GatewayMetrics()
        return _metrics
