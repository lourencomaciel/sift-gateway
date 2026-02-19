"""Tests for partial mapping determinism: same inputs produce same outputs."""

from __future__ import annotations

from _helpers import make_json_stream, make_partial_budgets, make_partial_config

from sift_gateway.mapping.partial import (
    compute_map_backend_id,
    compute_map_budget_fingerprint,
    run_partial_mapping,
)


def test_same_payload_same_budgets_produces_identical_sample_indices() -> None:
    """Same payload + same budgets must produce identical sample_indices."""
    data = [{"id": i, "value": f"item_{i}"} for i in range(50)]
    budgets = make_partial_budgets(max_records_sampled=10)
    config = make_partial_config(budgets)

    # Run twice
    roots1, _samples1 = run_partial_mapping(make_json_stream(data), config)
    roots2, _samples2 = run_partial_mapping(make_json_stream(data), config)

    assert len(roots1) > 0
    assert len(roots1) == len(roots2)

    for r1, r2 in zip(roots1, roots2, strict=True):
        assert r1.sample_indices == r2.sample_indices
        assert r1.fields_top == r2.fields_top


def test_same_payload_same_budgets_produces_identical_fields_top() -> None:
    """Same payload + same budgets must produce identical fields_top."""
    data = [{"name": "alice", "age": 30}, {"name": "bob", "age": 25}]
    budgets = make_partial_budgets(max_records_sampled=5)
    config = make_partial_config(budgets)

    roots1, _ = run_partial_mapping(make_json_stream(data), config)
    roots2, _ = run_partial_mapping(make_json_stream(data), config)

    for r1, r2 in zip(roots1, roots2, strict=True):
        assert r1.fields_top == r2.fields_top


def test_different_budgets_produce_different_fingerprint() -> None:
    """Different budgets must produce different map_budget_fingerprint."""
    backend_id = compute_map_backend_id()

    budgets_a = make_partial_budgets(max_records_sampled=10)
    budgets_b = make_partial_budgets(max_records_sampled=20)

    fp_a = compute_map_budget_fingerprint(budgets_a, backend_id)
    fp_b = compute_map_budget_fingerprint(budgets_b, backend_id)

    assert fp_a != fp_b


def test_map_budget_fingerprint_is_deterministic() -> None:
    """map_budget_fingerprint computation is deterministic across calls."""
    backend_id = compute_map_backend_id()
    budgets = make_partial_budgets()

    fp1 = compute_map_budget_fingerprint(budgets, backend_id)
    fp2 = compute_map_budget_fingerprint(budgets, backend_id)

    assert fp1 == fp2
    assert isinstance(fp1, str)
    assert len(fp1) == 32  # SHA-256 hex digest truncated to 32 chars


def test_map_backend_id_is_deterministic() -> None:
    """map_backend_id is deterministic across calls."""
    id1 = compute_map_backend_id()
    id2 = compute_map_backend_id()

    assert id1 == id2
    assert isinstance(id1, str)
    assert len(id1) == 16


def test_different_payload_hash_produces_different_samples() -> None:
    """Different payload_hash_full should produce different reservoir sampling."""
    data = [{"id": i} for i in range(100)]
    budgets = make_partial_budgets(max_records_sampled=5)

    config_a = make_partial_config(budgets, payload_hash="hash_a")
    config_b = make_partial_config(budgets, payload_hash="hash_b")

    roots_a, _ = run_partial_mapping(make_json_stream(data), config_a)
    roots_b, _ = run_partial_mapping(make_json_stream(data), config_b)

    # With different seeds, sample_indices should (very likely) differ
    assert len(roots_a) > 0
    assert len(roots_b) > 0
    # They could theoretically match but it is extremely unlikely with 100 elements
    # sampled down to 5 with different seeds
    indices_a = roots_a[0].sample_indices
    indices_b = roots_b[0].sample_indices
    assert indices_a is not None
    assert indices_b is not None
    # At least one index should differ (probabilistic but near-certain)
    assert indices_a != indices_b


def test_sampled_prefix_len_is_populated() -> None:
    """sampled_prefix_len reflects elements recognized during streaming."""
    data = [{"id": i} for i in range(20)]
    budgets = make_partial_budgets(max_records_sampled=5)
    config = make_partial_config(budgets)
    roots, _ = run_partial_mapping(make_json_stream(data), config)
    assert len(roots) == 1
    assert roots[0].sampled_prefix_len is not None
    assert roots[0].sampled_prefix_len >= 20


def test_prefix_coverage_false_when_fully_parsed() -> None:
    """prefix_coverage is False when entire stream is consumed without budget hit."""
    data = [{"id": i} for i in range(10)]
    budgets = make_partial_budgets(max_records_sampled=20)
    config = make_partial_config(budgets)
    roots, _ = run_partial_mapping(make_json_stream(data), config)
    assert len(roots) == 1
    # Fully consumed stream: no budget hit
    assert roots[0].prefix_coverage is False
    assert roots[0].stop_reason is None


def test_prefix_coverage_true_on_max_compute_stop() -> None:
    """prefix_coverage is True and stop_reason set when max_compute_steps triggers."""
    data = [{"id": i} for i in range(1000)]
    # Allow enough bytes to discover root but limit compute steps
    budgets = make_partial_budgets(max_compute_steps=50, max_records_sampled=5)
    config = make_partial_config(budgets)
    roots, _ = run_partial_mapping(make_json_stream(data), config)
    assert len(roots) >= 1
    root = roots[0]
    assert root.prefix_coverage is True
    assert root.stop_reason == "max_compute"


def test_skipped_oversize_records_tracking() -> None:
    """Records exceeding max_record_bytes are skipped and counted."""
    small = [{"id": i} for i in range(5)]
    big = [{"id": 100, "data": "x" * 200}]
    data = small + big + small
    budgets = make_partial_budgets(
        max_records_sampled=20,
        max_record_bytes=50,
    )
    config = make_partial_config(budgets)
    roots, _samples = run_partial_mapping(make_json_stream(data), config)
    assert len(roots) == 1
    summary = roots[0].root_summary
    assert summary is not None
    assert summary.get("skipped_oversize_records", 0) >= 1


def test_sample_record_hashes_are_deterministic() -> None:
    """Sample record hashes are deterministic across runs."""
    data = [{"id": i, "name": f"item_{i}"} for i in range(30)]
    budgets = make_partial_budgets(max_records_sampled=5)
    config = make_partial_config(budgets)
    _, samples1 = run_partial_mapping(make_json_stream(data), config)
    _, samples2 = run_partial_mapping(make_json_stream(data), config)
    assert len(samples1) == len(samples2)
    for s1, s2 in zip(samples1, samples2, strict=True):
        assert s1.record_hash == s2.record_hash
        assert s1.record_bytes == s2.record_bytes
        assert s1.sample_index == s2.sample_index


def test_count_estimate_is_exact_when_fully_parsed() -> None:
    """count_estimate matches actual element count when stream fully consumed."""
    data = [{"id": i} for i in range(15)]
    budgets = make_partial_budgets(max_records_sampled=20)
    config = make_partial_config(budgets)
    roots, _ = run_partial_mapping(make_json_stream(data), config)
    assert len(roots) == 1
    assert roots[0].count_estimate == 15


def test_count_estimate_none_when_stopped_early() -> None:
    """count_estimate is None when streaming stopped before array close."""
    data = [{"id": i} for i in range(1000)]
    budgets = make_partial_budgets(max_compute_steps=50, max_records_sampled=5)
    config = make_partial_config(budgets)
    roots, _ = run_partial_mapping(make_json_stream(data), config)
    assert len(roots) >= 1
    # Array was not fully read due to compute budget, so count_estimate should be None
    assert roots[0].count_estimate is None
