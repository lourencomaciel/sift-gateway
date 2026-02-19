"""Tests for prefix coverage semantics in partial mapping."""

from __future__ import annotations

from _helpers import make_json_stream, make_partial_budgets, make_partial_config

from sift_gateway.mapping.partial import run_partial_mapping


def test_complete_parse_has_count_estimate() -> None:
    """When stop_reason is none, count_estimate should be set."""
    data = [{"id": i} for i in range(10)]
    budgets = make_partial_budgets()
    config = make_partial_config(budgets)

    roots, _ = run_partial_mapping(make_json_stream(data), config)

    assert len(roots) > 0
    root = roots[0]
    assert root.stop_reason is None
    assert root.count_estimate == 10
    assert root.prefix_coverage is False


def test_stopped_parse_has_no_count_estimate() -> None:
    """When stop_reason != none, count_estimate must be None."""
    # Create data large enough to trigger max_compute_steps
    data = [{"id": i, "payload": "x" * 100} for i in range(1000)]
    budgets = make_partial_budgets(max_compute_steps=50)
    config = make_partial_config(budgets)

    roots, _ = run_partial_mapping(make_json_stream(data), config)

    assert len(roots) > 0
    root = roots[0]
    assert root.stop_reason is not None
    assert root.count_estimate is None


def test_stopped_parse_has_prefix_coverage_true() -> None:
    """When stop_reason != none, prefix_coverage must be True."""
    data = [{"id": i} for i in range(1000)]
    budgets = make_partial_budgets(max_compute_steps=50)
    config = make_partial_config(budgets)

    roots, _ = run_partial_mapping(make_json_stream(data), config)

    assert len(roots) > 0
    root = roots[0]
    assert root.stop_reason is not None
    assert root.prefix_coverage is True


def test_sampled_prefix_len_counts_recognized_elements() -> None:
    """sampled_prefix_len should count element boundaries recognized."""
    data = [{"id": i} for i in range(20)]
    budgets = make_partial_budgets()
    config = make_partial_config(budgets)

    roots, _ = run_partial_mapping(make_json_stream(data), config)

    assert len(roots) > 0
    root = roots[0]
    assert root.sampled_prefix_len is not None
    # All elements should be recognized
    assert root.sampled_prefix_len == 20


def test_byte_budget_triggers_stop() -> None:
    """max_bytes_read budget triggers stop_reason=max_bytes."""
    data = [{"id": i, "data": "x" * 200} for i in range(100)]
    # Set byte budget very low
    budgets = make_partial_budgets(max_bytes_read=100)
    config = make_partial_config(budgets)

    roots, _ = run_partial_mapping(make_json_stream(data), config)

    # Either we got roots with max_bytes stop or no roots at all
    # (if bytes exceeded before any root was discovered)
    if len(roots) > 0:
        root = roots[0]
        assert root.stop_reason == "max_bytes"
        assert root.prefix_coverage is True
        assert root.count_estimate is None


def test_compute_steps_budget_triggers_stop() -> None:
    """max_compute_steps budget triggers stop_reason=max_compute."""
    data = [{"id": i} for i in range(1000)]
    budgets = make_partial_budgets(max_compute_steps=30)
    config = make_partial_config(budgets)

    roots, _ = run_partial_mapping(make_json_stream(data), config)

    assert len(roots) > 0
    root = roots[0]
    assert root.stop_reason == "max_compute"
    assert root.prefix_coverage is True


def test_nested_arrays_do_not_replace_active_root() -> None:
    """Array-of-arrays should keep the top-level array as the active root."""
    data = [[{"id": 1}], [{"id": 2}], [{"id": 3}]]
    budgets = make_partial_budgets(max_records_sampled=100)
    config = make_partial_config(budgets)

    roots, _ = run_partial_mapping(make_json_stream(data), config)

    assert len(roots) == 1
    root = roots[0]
    assert root.root_key == "$"
    assert root.root_path == "$"
    assert root.count_estimate == 3
