"""Shared test fixtures for partial mapping tests."""

from __future__ import annotations

import io
import json

from mcp_artifact_gateway.mapping.partial import (
    PartialMappingBudgets,
    PartialMappingConfig,
    compute_map_backend_id,
    compute_map_budget_fingerprint,
)


def make_partial_budgets(**overrides: int) -> PartialMappingBudgets:
    """Create PartialMappingBudgets with sensible defaults, overridable per-field."""
    defaults = {
        "max_bytes_read": 50_000_000,
        "max_compute_steps": 5_000_000,
        "max_depth": 64,
        "max_records_sampled": 100,
        "max_record_bytes": 100_000,
        "max_leaf_paths": 500,
        "max_root_discovery_depth": 5,
    }
    defaults.update(overrides)
    return PartialMappingBudgets(**defaults)


def make_json_stream(data: object) -> io.BytesIO:
    """Serialize data to a compact JSON byte stream."""
    return io.BytesIO(
        json.dumps(data, separators=(",", ":"), sort_keys=True).encode("utf-8")
    )


def make_partial_config(
    budgets: PartialMappingBudgets,
    payload_hash: str = "test_hash_default",
) -> PartialMappingConfig:
    """Build a PartialMappingConfig with computed fingerprint."""
    backend_id = compute_map_backend_id()
    fingerprint = compute_map_budget_fingerprint(budgets, backend_id)
    return PartialMappingConfig(
        payload_hash_full=payload_hash,
        budgets=budgets,
        map_budget_fingerprint=fingerprint,
    )
