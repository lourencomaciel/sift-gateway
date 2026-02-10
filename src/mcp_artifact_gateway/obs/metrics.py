"""Gateway metrics: prometheus_client Counters + custom Histogram."""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Any

from prometheus_client import CollectorRegistry, Counter as _PromCounter


# ---------------------------------------------------------------------------
# Helpers for reading / resetting prometheus counters
# ---------------------------------------------------------------------------


def counter_value(counter: _PromCounter) -> int:  # type: ignore[type-arg]
    """Read the current value of a label-less prometheus Counter."""
    return int(counter._value.get())  # type: ignore[attr-defined]


def counter_reset(counter: _PromCounter) -> int:  # type: ignore[type-arg]
    """Reset a prometheus Counter to 0, returning the value before reset.

    Only intended for use in tests — production counters are monotonic.
    """
    val = int(counter._value.get())  # type: ignore[attr-defined]
    counter._value.set(0)  # type: ignore[attr-defined]
    return val


# ---------------------------------------------------------------------------
# Custom Histogram (prometheus has no min/max tracking)
# ---------------------------------------------------------------------------


@dataclass
class Histogram:
    """Simple histogram tracking min/max/sum/count."""

    _min: float = float("inf")
    _max: float = float("-inf")
    _sum: float = 0.0
    _count: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def observe(self, value: float) -> None:
        with self._lock:
            self._min = min(self._min, value)
            self._max = max(self._max, value)
            self._sum += value
            self._count += 1

    def snapshot(self) -> dict[str, float]:
        with self._lock:
            if self._count == 0:
                return {"min": 0, "max": 0, "sum": 0, "count": 0, "avg": 0}
            return {
                "min": self._min,
                "max": self._max,
                "sum": self._sum,
                "count": float(self._count),
                "avg": self._sum / self._count,
            }

    def reset(self) -> dict[str, float]:
        with self._lock:
            if self._count == 0:
                snap = {"min": 0, "max": 0, "sum": 0, "count": 0, "avg": 0}
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
# GatewayMetrics
# ---------------------------------------------------------------------------


def _make(
    name: str,
    doc: str,
    registry: CollectorRegistry,
) -> _PromCounter:  # type: ignore[type-arg]
    return _PromCounter(name, doc, registry=registry)


class GatewayMetrics:
    """Central metrics registry for the gateway."""

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        reg = registry or CollectorRegistry()

        # Cache/reuse
        self.cache_hits = _make("gateway_cache_hits_total", "Cache hits", reg)
        self.cache_misses = _make("gateway_cache_misses_total", "Cache misses", reg)
        self.alias_hits = _make("gateway_alias_hits_total", "Alias hits", reg)
        self.upstream_calls = _make("gateway_upstream_calls_total", "Upstream calls", reg)
        self.upstream_errors = _make("gateway_upstream_errors_total", "Upstream errors", reg)

        # Upstream latency (custom Histogram — prometheus has no min/max)
        self.upstream_latency = Histogram()

        # Ingest
        self.oversize_json_count = _make(
            "gateway_oversize_json_total",
            "Oversize JSON parts",
            reg,
        )
        self.binary_blob_writes = _make(
            "gateway_binary_blob_writes_total",
            "Binary blob writes",
            reg,
        )
        self.binary_blob_dedupes = _make(
            "gateway_binary_blob_dedupes_total",
            "Binary blob dedupes",
            reg,
        )

        # Mapping
        self.mapping_full_count = _make(
            "gateway_mapping_full_total",
            "Full mappings",
            reg,
        )
        self.mapping_partial_count = _make(
            "gateway_mapping_partial_total",
            "Partial mappings",
            reg,
        )
        self.mapping_failed_count = _make(
            "gateway_mapping_failed_total",
            "Failed mappings",
            reg,
        )
        self.mapping_latency = Histogram()

        # Mapping stop reasons
        self.mapping_stop_none = _make(
            "gateway_mapping_stop_none_total",
            "Mapping stop: none",
            reg,
        )
        self.mapping_stop_max_bytes = _make(
            "gateway_mapping_stop_max_bytes_total",
            "Mapping stop: max_bytes",
            reg,
        )
        self.mapping_stop_max_compute = _make(
            "gateway_mapping_stop_max_compute_total",
            "Mapping stop: max_compute",
            reg,
        )
        self.mapping_stop_max_depth = _make(
            "gateway_mapping_stop_max_depth_total",
            "Mapping stop: max_depth",
            reg,
        )
        self.mapping_stop_parse_error = _make(
            "gateway_mapping_stop_parse_error_total",
            "Mapping stop: parse_error",
            reg,
        )

        # Cursor
        self.cursor_stale_sample_set = _make(
            "gateway_cursor_stale_sample_set_total",
            "Cursor stale: sample_set",
            reg,
        )
        self.cursor_stale_map_budget = _make(
            "gateway_cursor_stale_map_budget_total",
            "Cursor stale: map_budget",
            reg,
        )
        self.cursor_stale_where_mode = _make(
            "gateway_cursor_stale_where_mode_total",
            "Cursor stale: where_mode",
            reg,
        )
        self.cursor_stale_traversal = _make(
            "gateway_cursor_stale_traversal_total",
            "Cursor stale: traversal",
            reg,
        )
        self.cursor_stale_generation = _make(
            "gateway_cursor_stale_generation_total",
            "Cursor stale: generation",
            reg,
        )
        self.cursor_invalid = _make(
            "gateway_cursor_invalid_total",
            "Invalid cursors",
            reg,
        )
        self.cursor_expired = _make(
            "gateway_cursor_expired_total",
            "Expired cursors",
            reg,
        )

        # Advisory lock
        self.advisory_lock_timeouts = _make(
            "gateway_advisory_lock_timeouts_total",
            "Advisory lock timeouts",
            reg,
        )
        self.advisory_lock_acquired = _make(
            "gateway_advisory_lock_acquired_total",
            "Advisory locks acquired",
            reg,
        )

        # Pruning
        self.prune_soft_deletes = _make(
            "gateway_prune_soft_deletes_total",
            "Soft deletes",
            reg,
        )
        self.prune_hard_deletes = _make(
            "gateway_prune_hard_deletes_total",
            "Hard deletes",
            reg,
        )
        self.prune_bytes_reclaimed = _make(
            "gateway_prune_bytes_reclaimed_total",
            "Bytes reclaimed",
            reg,
        )
        self.prune_fs_orphans_removed = _make(
            "gateway_prune_fs_orphans_removed_total",
            "FS orphans removed",
            reg,
        )

        # Quota
        self.quota_checks = _make("gateway_quota_checks_total", "Quota checks", reg)
        self.quota_breaches = _make("gateway_quota_breaches_total", "Quota breaches", reg)
        self.quota_prune_triggered = _make(
            "gateway_quota_prune_triggered_total",
            "Quota prune triggered",
            reg,
        )

    def record_stop_reason(self, reason: str) -> None:
        """Record a mapping stop reason."""
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
        """Record a cursor stale reason."""
        counter_map = {
            "sample_set_mismatch": self.cursor_stale_sample_set,
            "map_budget_mismatch": self.cursor_stale_map_budget,
            "where_mode_mismatch": self.cursor_stale_where_mode,
            "traversal_version_mismatch": self.cursor_stale_traversal,
            "generation_mismatch": self.cursor_stale_generation,
        }
        c = counter_map.get(reason)
        if c is not None:
            c.inc()

    def snapshot(self) -> dict[str, Any]:
        """Return a snapshot of all metrics."""
        return {
            "cache": {
                "hits": counter_value(self.cache_hits),
                "misses": counter_value(self.cache_misses),
                "alias_hits": counter_value(self.alias_hits),
            },
            "upstream": {
                "calls": counter_value(self.upstream_calls),
                "errors": counter_value(self.upstream_errors),
                "latency": self.upstream_latency.snapshot(),
            },
            "ingest": {
                "oversize_json": counter_value(self.oversize_json_count),
                "blob_writes": counter_value(self.binary_blob_writes),
                "blob_dedupes": counter_value(self.binary_blob_dedupes),
            },
            "mapping": {
                "full": counter_value(self.mapping_full_count),
                "partial": counter_value(self.mapping_partial_count),
                "failed": counter_value(self.mapping_failed_count),
                "latency": self.mapping_latency.snapshot(),
                "stop_reasons": {
                    "none": counter_value(self.mapping_stop_none),
                    "max_bytes": counter_value(self.mapping_stop_max_bytes),
                    "max_compute": counter_value(self.mapping_stop_max_compute),
                    "max_depth": counter_value(self.mapping_stop_max_depth),
                    "parse_error": counter_value(self.mapping_stop_parse_error),
                },
            },
            "cursor": {
                "stale": {
                    "sample_set": counter_value(self.cursor_stale_sample_set),
                    "map_budget": counter_value(self.cursor_stale_map_budget),
                    "where_mode": counter_value(self.cursor_stale_where_mode),
                    "traversal": counter_value(self.cursor_stale_traversal),
                    "generation": counter_value(self.cursor_stale_generation),
                },
                "invalid": counter_value(self.cursor_invalid),
                "expired": counter_value(self.cursor_expired),
            },
            "locks": {
                "timeouts": counter_value(self.advisory_lock_timeouts),
                "acquired": counter_value(self.advisory_lock_acquired),
            },
            "pruning": {
                "soft_deletes": counter_value(self.prune_soft_deletes),
                "hard_deletes": counter_value(self.prune_hard_deletes),
                "bytes_reclaimed": counter_value(self.prune_bytes_reclaimed),
                "fs_orphans_removed": counter_value(self.prune_fs_orphans_removed),
            },
            "quota": {
                "checks": counter_value(self.quota_checks),
                "breaches": counter_value(self.quota_breaches),
                "prune_triggered": counter_value(self.quota_prune_triggered),
            },
        }

    def reset(self) -> dict[str, Any]:
        """Reset all metrics and return a snapshot of values before reset."""
        return {
            "cache": {
                "hits": counter_reset(self.cache_hits),
                "misses": counter_reset(self.cache_misses),
                "alias_hits": counter_reset(self.alias_hits),
            },
            "upstream": {
                "calls": counter_reset(self.upstream_calls),
                "errors": counter_reset(self.upstream_errors),
                "latency": self.upstream_latency.reset(),
            },
            "ingest": {
                "oversize_json": counter_reset(self.oversize_json_count),
                "blob_writes": counter_reset(self.binary_blob_writes),
                "blob_dedupes": counter_reset(self.binary_blob_dedupes),
            },
            "mapping": {
                "full": counter_reset(self.mapping_full_count),
                "partial": counter_reset(self.mapping_partial_count),
                "failed": counter_reset(self.mapping_failed_count),
                "latency": self.mapping_latency.reset(),
                "stop_reasons": {
                    "none": counter_reset(self.mapping_stop_none),
                    "max_bytes": counter_reset(self.mapping_stop_max_bytes),
                    "max_compute": counter_reset(self.mapping_stop_max_compute),
                    "max_depth": counter_reset(self.mapping_stop_max_depth),
                    "parse_error": counter_reset(self.mapping_stop_parse_error),
                },
            },
            "cursor": {
                "stale": {
                    "sample_set": counter_reset(self.cursor_stale_sample_set),
                    "map_budget": counter_reset(self.cursor_stale_map_budget),
                    "where_mode": counter_reset(self.cursor_stale_where_mode),
                    "traversal": counter_reset(self.cursor_stale_traversal),
                    "generation": counter_reset(self.cursor_stale_generation),
                },
                "invalid": counter_reset(self.cursor_invalid),
                "expired": counter_reset(self.cursor_expired),
            },
            "locks": {
                "timeouts": counter_reset(self.advisory_lock_timeouts),
                "acquired": counter_reset(self.advisory_lock_acquired),
            },
            "pruning": {
                "soft_deletes": counter_reset(self.prune_soft_deletes),
                "hard_deletes": counter_reset(self.prune_hard_deletes),
                "bytes_reclaimed": counter_reset(self.prune_bytes_reclaimed),
                "fs_orphans_removed": counter_reset(self.prune_fs_orphans_removed),
            },
            "quota": {
                "checks": counter_reset(self.quota_checks),
                "breaches": counter_reset(self.quota_breaches),
                "prune_triggered": counter_reset(self.quota_prune_triggered),
            },
        }


# Global singleton
_metrics: GatewayMetrics | None = None
_metrics_lock = threading.Lock()


def get_metrics() -> GatewayMetrics:
    """Get or create the global metrics singleton (thread-safe)."""
    global _metrics
    with _metrics_lock:
        if _metrics is None:
            _metrics = GatewayMetrics()
        return _metrics
