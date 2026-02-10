from __future__ import annotations

from mcp_artifact_gateway.cursor.sample_set_hash import compute_sample_set_hash


def test_sample_set_hash_is_deterministic() -> None:
    h1 = compute_sample_set_hash(
        root_path="$.items",
        sample_indices=[1, 5, 9],
        map_budget_fingerprint="abc123",
    )
    h2 = compute_sample_set_hash(
        root_path="$.items",
        sample_indices=[1, 5, 9],
        map_budget_fingerprint="abc123",
    )
    assert h1 == h2
    assert len(h1) == 32


def test_sample_set_hash_changes_when_indices_change() -> None:
    h1 = compute_sample_set_hash(
        root_path="$.items",
        sample_indices=[1, 5, 9],
        map_budget_fingerprint="abc123",
    )
    h2 = compute_sample_set_hash(
        root_path="$.items",
        sample_indices=[1, 5, 10],
        map_budget_fingerprint="abc123",
    )
    assert h1 != h2
