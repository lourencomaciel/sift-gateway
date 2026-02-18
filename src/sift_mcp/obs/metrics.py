"""Collect gateway metrics via counters and histograms.

Provides ``GatewayMetrics``, a central registry of ``Counter``
and ``Histogram`` objects covering ingest, mapping,
cursor, lock, pruning, and quota subsystems.  Also exposes
``counter_value``/``counter_reset`` helpers and a thread-safe
``get_metrics`` singleton accessor.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
import threading
from typing import Any

# ------------------------------------------------------------------
# Pure-Python thread-safe counter (replaces prometheus_client)
# ------------------------------------------------------------------


class _CounterValue:
    """Thread-safe counter value with get/set interface."""

    __slots__ = ("_lock", "_val")

    def __init__(self) -> None:
        self._val: float = 0.0
        self._lock = threading.Lock()

    def get(self) -> float:
        """Return current value."""
        with self._lock:
            return self._val

    def set(self, v: float) -> None:
        """Set value (intended for testing resets)."""
        with self._lock:
            self._val = v


class Counter:
    """Minimal thread-safe monotonic counter."""

    __slots__ = ("_doc", "_name", "_value")

    def __init__(self, name: str, doc: str) -> None:
        """Create a named counter.

        Args:
            name: Metric name (e.g.
                ``gateway_upstream_calls_total``).
            doc: Human-readable description.
        """
        self._name = name
        self._doc = doc
        self._value = _CounterValue()

    def inc(self, amount: float = 1) -> None:
        """Increment counter by *amount*.

        Args:
            amount: Value to add (default ``1``).
        """
        with self._value._lock:
            self._value._val += amount


def counter_value(counter: Counter) -> int:
    """Read the current value of a counter.

    Args:
        counter: A Counter instance.

    Returns:
        Current integer value of the counter.
    """
    return int(counter._value.get())


def counter_reset(counter: Counter) -> int:
    """Reset a counter to zero for testing.

    Only intended for use in tests; production counters are
    monotonic.

    Args:
        counter: A Counter instance.

    Returns:
        The counter value immediately before the reset.
    """
    val = int(counter._value.get())
    counter._value.set(0)
    return val


# ------------------------------------------------------------------
# Custom Histogram (prometheus has no min/max tracking)
# ------------------------------------------------------------------


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
    _lock: threading.Lock = field(
        default_factory=threading.Lock, repr=False
    )

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

    def _snap_dict(self) -> dict[str, float]:
        """Build a stats dict from current state (caller holds lock)."""
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

    def snapshot(self) -> dict[str, float]:
        """Return current statistics without resetting.

        Returns:
            Dict with min, max, sum, count, and avg keys.
        """
        with self._lock:
            return self._snap_dict()

    def reset(self) -> dict[str, float]:
        """Return current statistics and reset to initial state.

        Returns:
            Dict with min, max, sum, count, and avg keys
            reflecting values before the reset.
        """
        with self._lock:
            snap = self._snap_dict()
            self._min = float("inf")
            self._max = float("-inf")
            self._sum = 0.0
            self._count = 0
            return snap


# ------------------------------------------------------------------
# Data-driven metric definitions
# ------------------------------------------------------------------

_HISTOGRAM_ATTRS: frozenset[str] = frozenset({
    "upstream_latency",
    "mapping_latency",
    "codegen_latency",
})

# Snapshot layout: the single source of truth for both counter
# creation and snapshot/reset gathering.  Leaf strings are
# GatewayMetrics attribute names (counters or histograms);
# nested dicts become nested sections in the output.
_SNAPSHOT_LAYOUT: dict[str, dict[str, Any]] = {
    "upstream": {
        "calls": "upstream_calls",
        "errors": "upstream_errors",
        "latency": "upstream_latency",
    },
    "ingest": {
        "oversize_json": "oversize_json_count",
        "blob_writes": "binary_blob_writes",
        "blob_dedupes": "binary_blob_dedupes",
    },
    "mapping": {
        "full": "mapping_full_count",
        "partial": "mapping_partial_count",
        "failed": "mapping_failed_count",
        "latency": "mapping_latency",
        "stop_reasons": {
            "none": "mapping_stop_none",
            "max_bytes": "mapping_stop_max_bytes",
            "max_compute": "mapping_stop_max_compute",
            "max_depth": "mapping_stop_max_depth",
            "parse_error": "mapping_stop_parse_error",
        },
    },
    "cursor": {
        "stale": {
            "sample_set": "cursor_stale_sample_set",
            "map_budget": "cursor_stale_map_budget",

            "traversal": "cursor_stale_traversal",
            "generation": "cursor_stale_generation",
        },
        "invalid": "cursor_invalid",
        "expired": "cursor_expired",
    },
    "locks": {
        "timeouts": "advisory_lock_timeouts",
        "acquired": "advisory_lock_acquired",
    },
    "pruning": {
        "soft_deletes": "prune_soft_deletes",
        "hard_deletes": "prune_hard_deletes",
        "bytes_reclaimed": "prune_bytes_reclaimed",
        "fs_orphans_removed": "prune_fs_orphans_removed",
    },
    "quota": {
        "checks": "quota_checks",
        "breaches": "quota_breaches",
        "prune_triggered": "quota_prune_triggered",
    },
    "codegen": {
        "executions": "codegen_executions",
        "success": "codegen_success",
        "failure": "codegen_failure",
        "timeout": "codegen_timeout",
        "input_records": "codegen_input_records",
        "output_records": "codegen_output_records",
        "latency": "codegen_latency",
    },
}

# Reason-string → attribute lookups for dynamic inc helpers
_STOP_REASON_ATTRS: dict[str, str] = {
    "none": "mapping_stop_none",
    "max_bytes": "mapping_stop_max_bytes",
    "max_compute": "mapping_stop_max_compute",
    "max_depth": "mapping_stop_max_depth",
    "parse_error": "mapping_stop_parse_error",
}

_CURSOR_STALE_ATTRS: dict[str, str] = {
    "sample_set_mismatch": "cursor_stale_sample_set",
    "map_budget_mismatch": "cursor_stale_map_budget",

    "traversal_version_mismatch": "cursor_stale_traversal",
    "generation_mismatch": "cursor_stale_generation",
}


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------


def _metric_name(attr: str) -> str:
    """Derive Prometheus-style metric name from attr name."""
    return f"gateway_{attr.removesuffix('_count')}_total"


def _collect_attrs(
    node: dict[str, Any],
) -> list[str]:
    """Walk layout tree and collect all leaf attr names."""
    attrs: list[str] = []
    for value in node.values():
        if isinstance(value, dict):
            attrs.extend(_collect_attrs(value))
        else:
            attrs.append(value)
    return attrs


_CounterFn = Callable[[Counter], int]


def _gather_node(
    m: GatewayMetrics,
    node: dict[str, Any],
    fn: _CounterFn,
    *,
    reset: bool,
) -> dict[str, Any]:
    """Recursively gather metrics from a layout node."""
    out: dict[str, Any] = {}
    for key, value in node.items():
        if isinstance(value, dict):
            out[key] = _gather_node(
                m, value, fn, reset=reset
            )
        elif value in _HISTOGRAM_ATTRS:
            hist: Histogram = getattr(m, value)
            out[key] = (
                hist.reset() if reset else hist.snapshot()
            )
        else:
            out[key] = fn(getattr(m, value))
    return out


# ------------------------------------------------------------------
# GatewayMetrics
# ------------------------------------------------------------------


class GatewayMetrics:
    """Central metrics registry for the gateway.

    Registers counters and custom Histogram objects for every
    observable subsystem.  Provides ``snapshot()`` and
    ``reset()`` methods that return all metrics as a nested
    dict, plus helpers to increment stop-reason and
    cursor-stale counters by reason string.
    """

    # Type annotations for IDE / mypy (populated in __init__)
    upstream_calls: Counter
    upstream_errors: Counter
    upstream_latency: Histogram
    oversize_json_count: Counter
    binary_blob_writes: Counter
    binary_blob_dedupes: Counter
    mapping_full_count: Counter
    mapping_partial_count: Counter
    mapping_failed_count: Counter
    mapping_latency: Histogram
    mapping_stop_none: Counter
    mapping_stop_max_bytes: Counter
    mapping_stop_max_compute: Counter
    mapping_stop_max_depth: Counter
    mapping_stop_parse_error: Counter
    cursor_stale_sample_set: Counter
    cursor_stale_map_budget: Counter

    cursor_stale_traversal: Counter
    cursor_stale_generation: Counter
    cursor_invalid: Counter
    cursor_expired: Counter
    advisory_lock_timeouts: Counter
    advisory_lock_acquired: Counter
    prune_soft_deletes: Counter
    prune_hard_deletes: Counter
    prune_bytes_reclaimed: Counter
    prune_fs_orphans_removed: Counter
    quota_checks: Counter
    quota_breaches: Counter
    quota_prune_triggered: Counter
    codegen_executions: Counter
    codegen_success: Counter
    codegen_failure: Counter
    codegen_timeout: Counter
    codegen_input_records: Counter
    codegen_output_records: Counter
    codegen_latency: Histogram

    def __init__(self, registry: Any = None) -> None:
        """Initialize all gateway metric counters.

        Args:
            registry: Ignored (kept for backward
                compatibility).
        """
        for attr in _collect_attrs(_SNAPSHOT_LAYOUT):
            if attr in _HISTOGRAM_ATTRS:
                setattr(self, attr, Histogram())
            else:
                setattr(
                    self,
                    attr,
                    Counter(_metric_name(attr), attr),
                )

    def record_stop_reason(self, reason: str) -> None:
        """Increment the counter for a mapping stop reason.

        Args:
            reason: Stop reason key (e.g. ``max_bytes``).
        """
        attr = _STOP_REASON_ATTRS.get(reason)
        if attr is not None:
            getattr(self, attr).inc()

    def record_cursor_stale_reason(self, reason: str) -> None:
        """Increment the counter for a cursor stale reason.

        Args:
            reason: Stale reason key (e.g.
                ``sample_set_mismatch``).
        """
        attr = _CURSOR_STALE_ATTRS.get(reason)
        if attr is not None:
            getattr(self, attr).inc()

    def snapshot(self) -> dict[str, Any]:
        """Return a snapshot of all metrics.

        Returns:
            Nested dict of all subsystem metric values.
        """
        return {
            section: _gather_node(
                self, layout, counter_value, reset=False
            )
            for section, layout in _SNAPSHOT_LAYOUT.items()
        }

    def reset(self) -> dict[str, Any]:
        """Reset all metrics and return pre-reset snapshot.

        Returns:
            Nested dict of all subsystem metric values before
            the reset.
        """
        return {
            section: _gather_node(
                self, layout, counter_reset, reset=True
            )
            for section, layout in _SNAPSHOT_LAYOUT.items()
        }


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
