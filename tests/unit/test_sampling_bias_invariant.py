"""Tests for sampling bias invariants: oversize record handling."""

from __future__ import annotations

from _helpers import make_json_stream, make_partial_budgets, make_partial_config

from sift_mcp.mapping.partial import run_partial_mapping


def test_oversize_records_skipped_and_counted() -> None:
    """Oversize records are skipped and counted in root_summary."""
    # Create mix of small and large records
    data = []
    for i in range(20):
        if i % 5 == 0:
            # Oversize record: payload larger than max_record_bytes
            data.append({"id": i, "big": "X" * 500})
        else:
            data.append({"id": i, "small": "ok"})

    # Set max_record_bytes very low so the "big" records are skipped
    budgets = make_partial_budgets(max_record_bytes=50, max_records_sampled=100)
    config = make_partial_config(budgets)

    roots, samples = run_partial_mapping(make_json_stream(data), config)

    assert len(roots) > 0
    root = roots[0]

    # Root summary should track skipped oversize records
    assert root.root_summary is not None
    assert root.root_summary.get("skipped_oversize", 0) > 0


def test_sample_indices_exclude_oversize_records() -> None:
    """sample_indices must only include successfully materialized records."""
    data = []
    for i in range(20):
        if i % 4 == 0:
            # Oversize record
            data.append({"id": i, "payload": "Y" * 500})
        else:
            data.append({"id": i, "small": "val"})

    budgets = make_partial_budgets(max_record_bytes=50, max_records_sampled=100)
    config = make_partial_config(budgets)

    roots, samples = run_partial_mapping(make_json_stream(data), config)

    assert len(roots) > 0
    root = roots[0]
    sample_indices = root.sample_indices or []

    # Oversize indices: 0, 4, 8, 12, 16
    oversize_indices = {0, 4, 8, 12, 16}
    for idx in sample_indices:
        assert idx not in oversize_indices, (
            f"sample_indices contains oversize record index {idx}"
        )


def test_sampled_prefix_len_includes_skipped_records() -> None:
    """sampled_prefix_len counts all element boundaries, including skipped ones."""
    data = []
    for i in range(10):
        if i < 3:
            data.append({"id": i, "big": "Z" * 500})
        else:
            data.append({"id": i, "ok": "val"})

    budgets = make_partial_budgets(max_record_bytes=50, max_records_sampled=100)
    config = make_partial_config(budgets)

    roots, _ = run_partial_mapping(make_json_stream(data), config)

    assert len(roots) > 0
    root = roots[0]
    # sampled_prefix_len should count ALL elements including skipped oversize ones
    assert root.sampled_prefix_len is not None
    assert root.sampled_prefix_len == 10


def test_reservoir_respects_max_records_sampled() -> None:
    """Reservoir never exceeds max_records_sampled entries."""
    data = [{"id": i} for i in range(100)]
    budgets = make_partial_budgets(max_records_sampled=5)
    config = make_partial_config(budgets)

    roots, samples = run_partial_mapping(make_json_stream(data), config)

    assert len(roots) > 0
    root = roots[0]
    assert root.sample_indices is not None
    assert len(root.sample_indices) <= 5

    root_samples = [s for s in samples if s.root_key == root.root_key]
    assert len(root_samples) <= 5


def test_all_records_sampled_when_count_below_reservoir_size() -> None:
    """When total records < max_records_sampled, all are sampled."""
    data = [{"id": i} for i in range(5)]
    budgets = make_partial_budgets(max_records_sampled=100)
    config = make_partial_config(budgets)

    roots, samples = run_partial_mapping(make_json_stream(data), config)

    assert len(roots) > 0
    root = roots[0]
    assert root.sample_indices is not None
    assert len(root.sample_indices) == 5
    assert sorted(root.sample_indices) == [0, 1, 2, 3, 4]
