"""Simple internal metrics counters for the gateway."""
from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Any


@dataclass
class Counter:
    """Thread-safe counter."""
    _value: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def increment(self, amount: int = 1) -> None:
        with self._lock:
            self._value += amount

    @property
    def value(self) -> int:
        with self._lock:
            return self._value

    def reset(self) -> int:
        with self._lock:
            val = self._value
            self._value = 0
            return val


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


class GatewayMetrics:
    """Central metrics registry for the gateway."""

    def __init__(self) -> None:
        # Cache/reuse
        self.cache_hits = Counter()
        self.cache_misses = Counter()
        self.alias_hits = Counter()
        self.upstream_calls = Counter()
        self.upstream_errors = Counter()

        # Upstream latency
        self.upstream_latency = Histogram()

        # Ingest
        self.oversize_json_count = Counter()
        self.binary_blob_writes = Counter()
        self.binary_blob_dedupes = Counter()

        # Mapping
        self.mapping_full_count = Counter()
        self.mapping_partial_count = Counter()
        self.mapping_failed_count = Counter()
        self.mapping_latency = Histogram()

        # Mapping stop reasons
        self.mapping_stop_none = Counter()
        self.mapping_stop_max_bytes = Counter()
        self.mapping_stop_max_compute = Counter()
        self.mapping_stop_max_depth = Counter()
        self.mapping_stop_parse_error = Counter()

        # Cursor
        self.cursor_stale_sample_set = Counter()
        self.cursor_stale_map_budget = Counter()
        self.cursor_stale_where_mode = Counter()
        self.cursor_stale_traversal = Counter()
        self.cursor_stale_generation = Counter()
        self.cursor_invalid = Counter()
        self.cursor_expired = Counter()

        # Advisory lock
        self.advisory_lock_timeouts = Counter()
        self.advisory_lock_acquired = Counter()

        # Pruning
        self.prune_soft_deletes = Counter()
        self.prune_hard_deletes = Counter()
        self.prune_bytes_reclaimed = Counter()
        self.prune_fs_orphans_removed = Counter()

    def record_stop_reason(self, reason: str) -> None:
        """Record a mapping stop reason."""
        counter_map = {
            "none": self.mapping_stop_none,
            "max_bytes": self.mapping_stop_max_bytes,
            "max_compute": self.mapping_stop_max_compute,
            "max_depth": self.mapping_stop_max_depth,
            "parse_error": self.mapping_stop_parse_error,
        }
        counter = counter_map.get(reason)
        if counter:
            counter.increment()

    def record_cursor_stale_reason(self, reason: str) -> None:
        """Record a cursor stale reason."""
        counter_map = {
            "sample_set_mismatch": self.cursor_stale_sample_set,
            "map_budget_mismatch": self.cursor_stale_map_budget,
            "where_mode_mismatch": self.cursor_stale_where_mode,
            "traversal_version_mismatch": self.cursor_stale_traversal,
            "generation_mismatch": self.cursor_stale_generation,
        }
        counter = counter_map.get(reason)
        if counter:
            counter.increment()

    def snapshot(self) -> dict[str, Any]:
        """Return a snapshot of all metrics."""
        return {
            "cache": {
                "hits": self.cache_hits.value,
                "misses": self.cache_misses.value,
                "alias_hits": self.alias_hits.value,
            },
            "upstream": {
                "calls": self.upstream_calls.value,
                "errors": self.upstream_errors.value,
                "latency": self.upstream_latency.snapshot(),
            },
            "ingest": {
                "oversize_json": self.oversize_json_count.value,
                "blob_writes": self.binary_blob_writes.value,
                "blob_dedupes": self.binary_blob_dedupes.value,
            },
            "mapping": {
                "full": self.mapping_full_count.value,
                "partial": self.mapping_partial_count.value,
                "failed": self.mapping_failed_count.value,
                "latency": self.mapping_latency.snapshot(),
                "stop_reasons": {
                    "none": self.mapping_stop_none.value,
                    "max_bytes": self.mapping_stop_max_bytes.value,
                    "max_compute": self.mapping_stop_max_compute.value,
                    "max_depth": self.mapping_stop_max_depth.value,
                    "parse_error": self.mapping_stop_parse_error.value,
                },
            },
            "cursor": {
                "stale": {
                    "sample_set": self.cursor_stale_sample_set.value,
                    "map_budget": self.cursor_stale_map_budget.value,
                    "where_mode": self.cursor_stale_where_mode.value,
                    "traversal": self.cursor_stale_traversal.value,
                    "generation": self.cursor_stale_generation.value,
                },
                "invalid": self.cursor_invalid.value,
                "expired": self.cursor_expired.value,
            },
            "locks": {
                "timeouts": self.advisory_lock_timeouts.value,
                "acquired": self.advisory_lock_acquired.value,
            },
            "pruning": {
                "soft_deletes": self.prune_soft_deletes.value,
                "hard_deletes": self.prune_hard_deletes.value,
                "bytes_reclaimed": self.prune_bytes_reclaimed.value,
                "fs_orphans_removed": self.prune_fs_orphans_removed.value,
            },
        }

    def reset(self) -> dict[str, Any]:
        """Reset all metrics and return a snapshot of values before reset."""
        return {
            "cache": {
                "hits": self.cache_hits.reset(),
                "misses": self.cache_misses.reset(),
                "alias_hits": self.alias_hits.reset(),
            },
            "upstream": {
                "calls": self.upstream_calls.reset(),
                "errors": self.upstream_errors.reset(),
                "latency": self.upstream_latency.reset(),
            },
            "ingest": {
                "oversize_json": self.oversize_json_count.reset(),
                "blob_writes": self.binary_blob_writes.reset(),
                "blob_dedupes": self.binary_blob_dedupes.reset(),
            },
            "mapping": {
                "full": self.mapping_full_count.reset(),
                "partial": self.mapping_partial_count.reset(),
                "failed": self.mapping_failed_count.reset(),
                "latency": self.mapping_latency.reset(),
                "stop_reasons": {
                    "none": self.mapping_stop_none.reset(),
                    "max_bytes": self.mapping_stop_max_bytes.reset(),
                    "max_compute": self.mapping_stop_max_compute.reset(),
                    "max_depth": self.mapping_stop_max_depth.reset(),
                    "parse_error": self.mapping_stop_parse_error.reset(),
                },
            },
            "cursor": {
                "stale": {
                    "sample_set": self.cursor_stale_sample_set.reset(),
                    "map_budget": self.cursor_stale_map_budget.reset(),
                    "where_mode": self.cursor_stale_where_mode.reset(),
                    "traversal": self.cursor_stale_traversal.reset(),
                    "generation": self.cursor_stale_generation.reset(),
                },
                "invalid": self.cursor_invalid.reset(),
                "expired": self.cursor_expired.reset(),
            },
            "locks": {
                "timeouts": self.advisory_lock_timeouts.reset(),
                "acquired": self.advisory_lock_acquired.reset(),
            },
            "pruning": {
                "soft_deletes": self.prune_soft_deletes.reset(),
                "hard_deletes": self.prune_hard_deletes.reset(),
                "bytes_reclaimed": self.prune_bytes_reclaimed.reset(),
                "fs_orphans_removed": self.prune_fs_orphans_removed.reset(),
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
