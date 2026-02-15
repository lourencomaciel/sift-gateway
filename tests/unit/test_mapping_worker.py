"""Tests for mapping worker: safety checks and scheduling."""

from __future__ import annotations

from pathlib import Path

from sift_mcp.config.settings import GatewayConfig
from sift_mcp.mapping.runner import (
    MappingInput,
    MappingResult,
    RootInventory,
    SampleRecord,
)
from sift_mcp.mapping.schema import (
    SchemaFieldInventory,
    SchemaInventory,
)
from sift_mcp.mapping.worker import (
    CONDITIONAL_MAP_UPDATE_SQL,
    DELETE_ROOTS_SQL,
    DELETE_SCHEMA_ROOTS_SQL,
    INSERT_ROOT_SQL,
    INSERT_SAMPLE_SQL,
    INSERT_SCHEMA_FIELD_SQL,
    INSERT_SCHEMA_ROOT_SQL,
    WorkerContext,
    check_worker_safety,
    persist_mapping_result,
    run_mapping_worker,
    should_run_mapping,
)
from sift_mcp.obs.metrics import GatewayMetrics, counter_value


class _FakeCursor:
    def __init__(self, *, rowcount: int = 1) -> None:
        self.rowcount = rowcount

    def fetchone(self) -> tuple[object, ...] | None:
        return None


class _FakeConnection:
    def __init__(self, *, conditional_rowcount: int = 1) -> None:
        self.conditional_rowcount = conditional_rowcount
        self.queries: list[str] = []
        self.committed = False
        self.rolled_back = False

    def execute(
        self,
        query: str,
        _params: tuple[object, ...] | None = None,
    ) -> _FakeCursor:
        self.queries.append(query.strip())
        if query.strip() == CONDITIONAL_MAP_UPDATE_SQL.strip():
            return _FakeCursor(rowcount=self.conditional_rowcount)
        return _FakeCursor(rowcount=1)

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        self.rolled_back = True


def _partial_ready_result() -> MappingResult:
    root = RootInventory(
        root_key="items",
        root_path="$.items",
        count_estimate=2,
        root_shape="array",
        fields_top={"id": {"number": 2}},
        root_summary={"elements_seen": 2},
        inventory_coverage=1.0,
        root_score=2.0,
        sample_indices=[0],
        prefix_coverage=False,
        stop_reason=None,
        sampled_prefix_len=2,
    )
    sample = SampleRecord(
        root_key="items",
        root_path="$.items",
        sample_index=0,
        record={"id": 1},
        record_bytes=8,
        record_hash="a" * 64,
    )
    schema = SchemaInventory(
        root_key="items",
        version="schema_v1",
        schema_hash="sha256:" + ("b" * 64),
        root_path="$.items",
        mode="sampled",
        completeness="partial",
        observed_records=1,
        fields=[
            SchemaFieldInventory(
                path="$.id",
                types=["number"],
                nullable=False,
                required=True,
                observed_count=1,
            )
        ],
        dataset_hash="sha256:" + ("c" * 64),
        traversal_contract_version="traversal_v1",
        map_budget_fingerprint="mbf_1",
    )
    return MappingResult(
        map_kind="partial",
        map_status="ready",
        mapped_part_index=0,
        roots=[root],
        map_budget_fingerprint="mbf_1",
        map_backend_id="backend_1",
        prng_version="prng_xoshiro256ss_v1",
        map_error=None,
        samples=[sample],
        schemas=[schema],
    )


def test_should_run_mapping_pending() -> None:
    """should_run_mapping returns True for pending status."""
    assert should_run_mapping("pending") is True


def test_should_run_mapping_stale() -> None:
    """should_run_mapping returns True for stale status."""
    assert should_run_mapping("stale") is True


def test_should_run_mapping_ready() -> None:
    """should_run_mapping returns False for ready status."""
    assert should_run_mapping("ready") is False


def test_should_run_mapping_failed() -> None:
    """should_run_mapping returns False for failed status."""
    assert should_run_mapping("failed") is False


def test_should_run_mapping_empty() -> None:
    """should_run_mapping returns False for empty string."""
    assert should_run_mapping("") is False


def test_check_worker_safety_rejects_none_row() -> None:
    """check_worker_safety returns False when current_row is None."""
    assert check_worker_safety("art_123", 1, None) is False


def test_check_worker_safety_rejects_deleted_artifact() -> None:
    """check_worker_safety returns False for deleted artifacts."""
    row = {
        "deleted_at": "2025-01-01T00:00:00Z",
        "map_status": "pending",
        "generation": 1,
    }
    assert check_worker_safety("art_123", 1, row) is False


def test_check_worker_safety_rejects_generation_mismatch() -> None:
    """check_worker_safety returns False when generation doesn't match."""
    row = {
        "deleted_at": None,
        "map_status": "pending",
        "generation": 2,
    }
    assert check_worker_safety("art_123", 1, row) is False


def test_check_worker_safety_rejects_ready_status() -> None:
    """check_worker_safety returns False when map_status is ready."""
    row = {
        "deleted_at": None,
        "map_status": "ready",
        "generation": 1,
    }
    assert check_worker_safety("art_123", 1, row) is False


def test_check_worker_safety_accepts_valid_pending() -> None:
    """check_worker_safety returns True for valid pending artifact."""
    row = {
        "deleted_at": None,
        "map_status": "pending",
        "generation": 5,
    }
    assert check_worker_safety("art_123", 5, row) is True


def test_check_worker_safety_accepts_valid_stale() -> None:
    """check_worker_safety returns True for valid stale artifact."""
    row = {
        "deleted_at": None,
        "map_status": "stale",
        "generation": 3,
    }
    assert check_worker_safety("art_123", 3, row) is True


def test_persist_mapping_result_writes_roots_and_samples_transactionally() -> (
    None
):
    connection = _FakeConnection(conditional_rowcount=1)
    result = _partial_ready_result()

    persisted = persist_mapping_result(
        connection,
        worker_ctx=WorkerContext(
            artifact_id="art_123", generation=1, map_status="pending"
        ),
        result=result,
    )

    assert persisted is True
    assert connection.committed is True
    assert connection.rolled_back is False
    assert CONDITIONAL_MAP_UPDATE_SQL.strip() in connection.queries
    assert DELETE_ROOTS_SQL.strip() in connection.queries
    assert INSERT_ROOT_SQL.strip() in connection.queries
    assert INSERT_SAMPLE_SQL.strip() in connection.queries
    assert DELETE_SCHEMA_ROOTS_SQL.strip() in connection.queries
    assert INSERT_SCHEMA_ROOT_SQL.strip() in connection.queries
    assert INSERT_SCHEMA_FIELD_SQL.strip() in connection.queries


def test_persist_mapping_result_discards_when_conditional_update_skips() -> (
    None
):
    connection = _FakeConnection(conditional_rowcount=0)
    result = _partial_ready_result()

    persisted = persist_mapping_result(
        connection,
        worker_ctx=WorkerContext(
            artifact_id="art_123", generation=1, map_status="pending"
        ),
        result=result,
    )

    assert persisted is False
    assert connection.committed is False
    assert connection.rolled_back is True


def test_run_mapping_worker_records_metrics(
    tmp_path: Path, monkeypatch
) -> None:
    connection = _FakeConnection(conditional_rowcount=1)
    metrics = GatewayMetrics()
    result = _partial_ready_result()

    monkeypatch.setattr(
        "sift_mcp.mapping.worker.run_mapping",
        lambda _mapping_input: result,
    )

    persisted = run_mapping_worker(
        connection,
        worker_ctx=WorkerContext(
            artifact_id="art_123", generation=1, map_status="pending"
        ),
        mapping_input=MappingInput(
            artifact_id="art_123",
            payload_hash_full="payload_hash_1",
            envelope={"content": []},
            config=GatewayConfig(data_dir=tmp_path),
        ),
        metrics=metrics,
    )

    assert persisted is True
    assert counter_value(metrics.mapping_partial_count) == 1
    assert counter_value(metrics.mapping_stop_none) == 1
    assert metrics.mapping_latency.snapshot()["count"] == 1.0


def test_run_mapping_worker_rejects_ready_status(
    tmp_path: Path, monkeypatch
) -> None:
    """run_mapping_worker returns False for non-runnable map_status."""
    connection = _FakeConnection(conditional_rowcount=1)
    persisted = run_mapping_worker(
        connection,
        worker_ctx=WorkerContext(
            artifact_id="art_x", generation=1, map_status="ready"
        ),
        mapping_input=MappingInput(
            artifact_id="art_x",
            payload_hash_full="ph_x",
            envelope={"content": []},
            config=GatewayConfig(data_dir=tmp_path),
        ),
    )
    assert persisted is False
    assert connection.committed is False


def _full_ready_result() -> MappingResult:
    """Build a full mapping ready result (no samples)."""
    root = RootInventory(
        root_key="$",
        root_path="$",
        count_estimate=3,
        root_shape="array",
        fields_top={"id": {"number": 3}},
        root_summary={"element_count": 3},
        inventory_coverage=1.0,
        root_score=3.0,
    )
    schema = SchemaInventory(
        root_key="$",
        version="schema_v1",
        schema_hash="sha256:" + ("d" * 64),
        root_path="$",
        mode="exact",
        completeness="complete",
        observed_records=3,
        fields=[
            SchemaFieldInventory(
                path="$.id",
                types=["number"],
                nullable=False,
                required=True,
                observed_count=3,
            )
        ],
        dataset_hash="sha256:" + ("e" * 64),
        traversal_contract_version="traversal_v1",
        map_budget_fingerprint=None,
    )
    return MappingResult(
        map_kind="full",
        map_status="ready",
        mapped_part_index=0,
        roots=[root],
        map_budget_fingerprint=None,
        map_backend_id=None,
        prng_version=None,
        map_error=None,
        schemas=[schema],
    )


def test_persist_full_mapping_writes_roots_no_samples() -> None:
    """Full mapping writes roots but does not write samples."""
    from sift_mcp.mapping.worker import DELETE_SAMPLES_SQL

    connection = _FakeConnection(conditional_rowcount=1)
    result = _full_ready_result()
    persisted = persist_mapping_result(
        connection,
        worker_ctx=WorkerContext(
            artifact_id="art_f", generation=1, map_status="pending"
        ),
        result=result,
    )
    assert persisted is True
    assert connection.committed is True
    assert DELETE_ROOTS_SQL.strip() in connection.queries
    assert INSERT_ROOT_SQL.strip() in connection.queries
    assert DELETE_SCHEMA_ROOTS_SQL.strip() in connection.queries
    assert INSERT_SCHEMA_ROOT_SQL.strip() in connection.queries
    assert INSERT_SCHEMA_FIELD_SQL.strip() in connection.queries
    # Full mapping should NOT write samples
    assert INSERT_SAMPLE_SQL.strip() not in connection.queries
    assert DELETE_SAMPLES_SQL.strip() not in connection.queries


def test_persist_failed_mapping_commits_without_roots() -> None:
    """Failed mapping commits artifact update but skips root and sample writes."""
    failed_result = MappingResult(
        map_kind="full",
        map_status="failed",
        mapped_part_index=None,
        roots=[],
        map_budget_fingerprint=None,
        map_backend_id=None,
        prng_version=None,
        map_error="no JSON content part found in envelope",
    )
    connection = _FakeConnection(conditional_rowcount=1)
    persisted = persist_mapping_result(
        connection,
        worker_ctx=WorkerContext(
            artifact_id="art_fail", generation=1, map_status="pending"
        ),
        result=failed_result,
    )
    assert persisted is True
    assert connection.committed is True
    # Failed mapping should only do the conditional UPDATE + commit
    assert CONDITIONAL_MAP_UPDATE_SQL.strip() in connection.queries
    assert DELETE_ROOTS_SQL.strip() not in connection.queries
    assert INSERT_ROOT_SQL.strip() not in connection.queries


def test_validate_sample_alignment_rejects_mismatch() -> None:
    """_validate_sample_alignment raises ValueError on index mismatch."""
    import pytest

    from sift_mcp.mapping.worker import _validate_sample_alignment

    root = RootInventory(
        root_key="items",
        root_path="$.items",
        count_estimate=3,
        root_shape="array",
        fields_top=None,
        root_summary={},
        inventory_coverage=1.0,
        root_score=3.0,
        sample_indices=[0, 1, 2],
    )
    result = MappingResult(
        map_kind="partial",
        map_status="ready",
        mapped_part_index=0,
        roots=[root],
        map_budget_fingerprint="fp",
        map_backend_id="be",
        prng_version="pv",
        map_error=None,
        samples=[
            SampleRecord(
                root_key="items",
                root_path="$.items",
                sample_index=0,
                record={"id": 0},
                record_bytes=8,
                record_hash="h0",
            ),
            SampleRecord(
                root_key="items",
                root_path="$.items",
                sample_index=1,
                record={"id": 1},
                record_bytes=8,
                record_hash="h1",
            ),
            # Missing index 2
        ],
    )
    samples_by_root = {"items": result.samples}
    with pytest.raises(ValueError, match="sample index mismatch"):
        _validate_sample_alignment(
            result=result, samples_by_root=samples_by_root
        )


def test_run_mapping_worker_records_full_metrics(
    tmp_path: Path, monkeypatch
) -> None:
    """run_mapping_worker records full mapping metrics."""
    connection = _FakeConnection(conditional_rowcount=1)
    metrics = GatewayMetrics()
    result = _full_ready_result()

    monkeypatch.setattr(
        "sift_mcp.mapping.worker.run_mapping",
        lambda _mi: result,
    )
    run_mapping_worker(
        connection,
        worker_ctx=WorkerContext(
            artifact_id="art_fm", generation=1, map_status="pending"
        ),
        mapping_input=MappingInput(
            artifact_id="art_fm",
            payload_hash_full="ph_fm",
            envelope={"content": []},
            config=GatewayConfig(data_dir=tmp_path),
        ),
        metrics=metrics,
    )
    assert counter_value(metrics.mapping_full_count) == 1
    assert counter_value(metrics.mapping_partial_count) == 0
    assert metrics.mapping_latency.snapshot()["count"] == 1.0


def test_run_mapping_worker_records_failed_metrics(
    tmp_path: Path, monkeypatch
) -> None:
    """run_mapping_worker records failed mapping metrics."""
    connection = _FakeConnection(conditional_rowcount=1)
    metrics = GatewayMetrics()
    failed = MappingResult(
        map_kind="full",
        map_status="failed",
        mapped_part_index=None,
        roots=[],
        map_budget_fingerprint=None,
        map_backend_id=None,
        prng_version=None,
        map_error="test error",
    )
    monkeypatch.setattr(
        "sift_mcp.mapping.worker.run_mapping",
        lambda _mi: failed,
    )
    run_mapping_worker(
        connection,
        worker_ctx=WorkerContext(
            artifact_id="art_fl", generation=1, map_status="pending"
        ),
        mapping_input=MappingInput(
            artifact_id="art_fl",
            payload_hash_full="ph_fl",
            envelope={"content": []},
            config=GatewayConfig(data_dir=tmp_path),
        ),
        metrics=metrics,
    )
    assert counter_value(metrics.mapping_failed_count) == 1
    assert counter_value(metrics.mapping_full_count) == 0


def test_run_mapping_worker_emits_structured_log(
    tmp_path: Path, monkeypatch
) -> None:
    """run_mapping_worker emits structured log events for completed mapping."""
    connection = _FakeConnection(conditional_rowcount=1)
    result = _full_ready_result()
    monkeypatch.setattr(
        "sift_mcp.mapping.worker.run_mapping",
        lambda _mi: result,
    )

    log_events: list[str] = []

    class _CapturingLogger:
        def info(self, event: str, **kw: object) -> None:
            log_events.append(event)

        def warning(self, event: str, **kw: object) -> None:
            log_events.append(event)

    logger = _CapturingLogger()
    run_mapping_worker(
        connection,
        worker_ctx=WorkerContext(
            artifact_id="art_log", generation=1, map_status="pending"
        ),
        mapping_input=MappingInput(
            artifact_id="art_log",
            payload_hash_full="ph_log",
            envelope={"content": []},
            config=GatewayConfig(data_dir=tmp_path),
        ),
        logger=logger,
    )
    from sift_mcp.obs.logging import LogEvents

    assert LogEvents.MAPPING_STARTED in log_events
    assert LogEvents.MAPPING_COMPLETED in log_events


def test_run_mapping_worker_emits_failed_log(
    tmp_path: Path, monkeypatch
) -> None:
    """run_mapping_worker emits MAPPING_FAILED log for failed results."""
    connection = _FakeConnection(conditional_rowcount=1)
    failed = MappingResult(
        map_kind="full",
        map_status="failed",
        mapped_part_index=None,
        roots=[],
        map_budget_fingerprint=None,
        map_backend_id=None,
        prng_version=None,
        map_error="test error",
    )
    monkeypatch.setattr(
        "sift_mcp.mapping.worker.run_mapping",
        lambda _mi: failed,
    )

    log_events: list[str] = []

    class _CapturingLogger:
        def info(self, event: str, **kw: object) -> None:
            log_events.append(event)

        def warning(self, event: str, **kw: object) -> None:
            log_events.append(event)

    logger = _CapturingLogger()
    run_mapping_worker(
        connection,
        worker_ctx=WorkerContext(
            artifact_id="art_flog", generation=1, map_status="pending"
        ),
        mapping_input=MappingInput(
            artifact_id="art_flog",
            payload_hash_full="ph_flog",
            envelope={"content": []},
            config=GatewayConfig(data_dir=tmp_path),
        ),
        logger=logger,
    )
    from sift_mcp.obs.logging import LogEvents

    assert LogEvents.MAPPING_STARTED in log_events
    assert LogEvents.MAPPING_FAILED in log_events
