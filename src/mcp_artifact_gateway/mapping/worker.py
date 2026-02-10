"""Mapping worker: schedules and executes mapping with race-safe writes."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
import time
from typing import Any

try:
    from psycopg.types.json import Jsonb
except ImportError:  # SQLite-only install — no psycopg
    Jsonb = lambda v: v  # type: ignore[assignment,misc]  # noqa: E731

from mcp_artifact_gateway.constants import MAPPER_VERSION, WORKSPACE_ID
from mcp_artifact_gateway.db.protocols import ConnectionLike, safe_rollback
from mcp_artifact_gateway.mapping.runner import (
    MappingInput,
    MappingResult,
    SampleRecord,
    run_mapping,
)
from mcp_artifact_gateway.obs.logging import LogEvents, get_logger


def _jsonb(value: Any) -> Any:
    """Wrap non-None values in Jsonb for psycopg3 JSONB columns."""
    if value is None:
        return None
    return Jsonb(value)


@dataclass(frozen=True)
class WorkerContext:
    """Context for a mapping worker run."""

    artifact_id: str
    generation: int  # snapshot at start
    map_status: str  # must be "pending" or "stale"


# SQL for conditional mapping update (race-safe)
CONDITIONAL_MAP_UPDATE_SQL = """
UPDATE artifacts
SET map_kind = %s,
    map_status = %s,
    mapped_part_index = %s,
    mapper_version = %s,
    map_budget_fingerprint = %s,
    map_backend_id = %s,
    prng_version = %s,
    map_error = %s
WHERE workspace_id = %s
  AND artifact_id = %s
  AND deleted_at IS NULL
  AND map_status IN ('pending', 'stale')
  AND generation = %s
"""


DELETE_ROOTS_SQL = """
DELETE FROM artifact_roots
WHERE workspace_id = %s AND artifact_id = %s
"""


# SQL for inserting artifact_roots
INSERT_ROOT_SQL = """
INSERT INTO artifact_roots (
    workspace_id, artifact_id, root_key, root_path,
    count_estimate, inventory_coverage, root_summary,
    root_score, root_shape, fields_top, examples, recipes,
    sample_indices
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (workspace_id, artifact_id, root_key)
DO UPDATE SET
    root_path = EXCLUDED.root_path,
    count_estimate = EXCLUDED.count_estimate,
    inventory_coverage = EXCLUDED.inventory_coverage,
    root_summary = EXCLUDED.root_summary,
    root_score = EXCLUDED.root_score,
    root_shape = EXCLUDED.root_shape,
    fields_top = EXCLUDED.fields_top,
    sample_indices = EXCLUDED.sample_indices
"""


# SQL for inserting samples (Addendum C)
INSERT_SAMPLE_SQL = """
INSERT INTO artifact_samples (
    workspace_id, artifact_id, root_key, root_path,
    sample_index, record, record_bytes, record_hash
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (workspace_id, artifact_id, root_key, sample_index)
DO UPDATE SET
    record = EXCLUDED.record,
    record_bytes = EXCLUDED.record_bytes,
    record_hash = EXCLUDED.record_hash
"""

# SQL for cleaning old samples before rewrite
DELETE_SAMPLES_SQL = """
DELETE FROM artifact_samples
WHERE workspace_id = %s AND artifact_id = %s AND root_key = %s
"""


def should_run_mapping(map_status: str) -> bool:
    """Check if mapping should run for this artifact."""
    return map_status in ("pending", "stale")


def _cursor_rowcount(cursor: object) -> int:
    rowcount = getattr(cursor, "rowcount", None)
    if isinstance(rowcount, int):
        return rowcount
    return 1


def _root_sample_indices(root: Any) -> list[int] | None:
    raw = root.sample_indices
    if raw is None:
        return None
    return [int(index) for index in sorted(raw)]


def _root_insert_params(*, artifact_id: str, root: Any) -> tuple[object, ...]:
    return (
        WORKSPACE_ID,
        artifact_id,
        root.root_key,
        root.root_path,
        root.count_estimate,
        root.inventory_coverage,
        _jsonb(root.root_summary),
        root.root_score,
        root.root_shape,
        _jsonb(root.fields_top),
        None,
        None,
        _jsonb(_root_sample_indices(root)),
    )


def _sample_insert_params(*, artifact_id: str, sample: SampleRecord) -> tuple[object, ...]:
    return (
        WORKSPACE_ID,
        artifact_id,
        sample.root_key,
        sample.root_path,
        sample.sample_index,
        _jsonb(sample.record),
        sample.record_bytes,
        sample.record_hash,
    )


def _group_samples(samples: list[SampleRecord] | None) -> dict[str, list[SampleRecord]]:
    grouped: dict[str, list[SampleRecord]] = defaultdict(list)
    if not samples:
        return grouped
    for sample in samples:
        grouped[sample.root_key].append(sample)
    return grouped


def _validate_sample_alignment(
    *,
    result: MappingResult,
    samples_by_root: dict[str, list[SampleRecord]],
) -> None:
    for root in result.roots:
        if root.sample_indices is None:
            continue
        expected = [int(value) for value in sorted(root.sample_indices)]
        actual = [
            int(sample.sample_index)
            for sample in sorted(
                samples_by_root.get(root.root_key, []),
                key=lambda sample: sample.sample_index,
            )
        ]
        if expected != actual:
            msg = (
                f"sample index mismatch for root {root.root_key}: "
                f"root sample_indices={expected}, sample rows={actual}"
            )
            raise ValueError(msg)


def persist_mapping_result(
    connection: ConnectionLike,
    *,
    worker_ctx: WorkerContext,
    result: MappingResult,
) -> bool:
    """Persist mapping output with generation-safe conditional writes."""
    update_cursor = connection.execute(
        CONDITIONAL_MAP_UPDATE_SQL,
        (
            result.map_kind,
            result.map_status,
            result.mapped_part_index,
            MAPPER_VERSION,
            result.map_budget_fingerprint,
            result.map_backend_id,
            result.prng_version,
            result.map_error,
            WORKSPACE_ID,
            worker_ctx.artifact_id,
            worker_ctx.generation,
        ),
    )
    if _cursor_rowcount(update_cursor) == 0:
        safe_rollback(connection)
        return False

    if result.map_status != "ready":
        connection.commit()
        return True

    connection.execute(
        DELETE_ROOTS_SQL,
        (WORKSPACE_ID, worker_ctx.artifact_id),
    )

    for root in result.roots:
        connection.execute(
            INSERT_ROOT_SQL,
            _root_insert_params(artifact_id=worker_ctx.artifact_id, root=root),
        )

    if result.map_kind == "partial":
        samples_by_root = _group_samples(result.samples)
        _validate_sample_alignment(result=result, samples_by_root=samples_by_root)
        for root in result.roots:
            connection.execute(
                DELETE_SAMPLES_SQL,
                (WORKSPACE_ID, worker_ctx.artifact_id, root.root_key),
            )
            for sample in sorted(
                samples_by_root.get(root.root_key, []),
                key=lambda item: item.sample_index,
            ):
                connection.execute(
                    INSERT_SAMPLE_SQL,
                    _sample_insert_params(artifact_id=worker_ctx.artifact_id, sample=sample),
                )

    connection.commit()
    return True


def run_mapping_worker(
    connection: ConnectionLike,
    *,
    worker_ctx: WorkerContext,
    mapping_input: MappingInput,
    metrics: Any | None = None,
    logger: Any | None = None,
) -> bool:
    """Run mapping and persist results safely; discard on generation/status races."""
    log = logger or get_logger(
        component="mapping.worker",
        artifact_id=worker_ctx.artifact_id,
    )

    if not should_run_mapping(worker_ctx.map_status):
        return False

    log.info(
        LogEvents.MAPPING_STARTED,
        artifact_id=worker_ctx.artifact_id,
        generation=worker_ctx.generation,
        map_status=worker_ctx.map_status,
    )

    started_at = time.monotonic()
    result = run_mapping(mapping_input)
    final_result = result
    try:
        persisted = persist_mapping_result(
            connection,
            worker_ctx=worker_ctx,
            result=result,
        )
    except Exception as exc:
        safe_rollback(connection)
        failure = MappingResult(
            map_kind=result.map_kind,
            map_status="failed",
            mapped_part_index=result.mapped_part_index,
            roots=[],
            map_budget_fingerprint=result.map_budget_fingerprint,
            map_backend_id=result.map_backend_id,
            prng_version=result.prng_version,
            map_error=f"mapping write error: {type(exc).__name__}: {exc}",
            samples=None,
        )
        final_result = failure
        try:
            persisted = persist_mapping_result(
                connection,
                worker_ctx=worker_ctx,
                result=failure,
            )
        except Exception:
            safe_rollback(connection)
            persisted = False

    duration_ms = (time.monotonic() - started_at) * 1000.0
    _record_mapping_metrics(
        metrics=metrics,
        result=final_result,
        duration_ms=duration_ms,
    )

    if final_result.map_status == "failed":
        log.warning(
            LogEvents.MAPPING_FAILED,
            artifact_id=worker_ctx.artifact_id,
            map_kind=final_result.map_kind,
            map_error=final_result.map_error,
            duration_ms=duration_ms,
        )
    else:
        log.info(
            LogEvents.MAPPING_COMPLETED,
            artifact_id=worker_ctx.artifact_id,
            map_kind=final_result.map_kind,
            map_status=final_result.map_status,
            root_count=len(final_result.roots),
            map_budget_fingerprint=final_result.map_budget_fingerprint,
            map_backend_id=final_result.map_backend_id,
            prng_version=final_result.prng_version,
            duration_ms=duration_ms,
        )

    return persisted


def _record_mapping_metrics(
    *,
    metrics: Any | None,
    result: MappingResult,
    duration_ms: float,
) -> None:
    if metrics is None:
        return
    mapping_latency = getattr(metrics, "mapping_latency", None)
    observe = getattr(mapping_latency, "observe", None)
    if callable(observe):
        observe(duration_ms)

    if result.map_status == "failed":
        counter = getattr(metrics, "mapping_failed_count", None)
        increment = getattr(counter, "inc", None)
        if callable(increment):
            increment()
    elif result.map_kind == "full":
        counter = getattr(metrics, "mapping_full_count", None)
        increment = getattr(counter, "inc", None)
        if callable(increment):
            increment()
    elif result.map_kind == "partial":
        counter = getattr(metrics, "mapping_partial_count", None)
        increment = getattr(counter, "inc", None)
        if callable(increment):
            increment()

    if result.map_kind != "partial":
        return
    stop_reasons = {
        root.stop_reason if isinstance(root.stop_reason, str) else "none" for root in result.roots
    }
    if not stop_reasons:
        stop_reasons = {"none"}
    recorder = getattr(metrics, "record_stop_reason", None)
    if callable(recorder):
        for reason in sorted(stop_reasons):
            recorder(reason)


def check_worker_safety(
    artifact_id: str,
    expected_generation: int,
    current_row: dict[str, Any] | None,
) -> bool:
    """Verify it's safe to write mapping results.

    Returns True only if:
    - artifact exists and is not deleted
    - map_status is pending or stale
    - generation matches expected value
    """
    if current_row is None:
        return False
    if current_row.get("deleted_at") is not None:
        return False
    if current_row.get("map_status") not in ("pending", "stale"):
        return False
    if current_row.get("generation") != expected_generation:
        return False
    return True
