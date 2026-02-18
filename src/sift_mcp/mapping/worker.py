"""Schedule and execute mapping with generation-safe DB writes.

Run the mapping pipeline for a single artifact and persist
results using conditional SQL updates guarded by the artifact
generation counter.  Handle write conflicts gracefully and
record latency/outcome metrics.  Key exports are
``run_mapping_worker``, ``persist_mapping_result``,
``WorkerContext``, and ``check_worker_safety``.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
import json
import time
from typing import Any

from sift_mcp.constants import MAPPER_VERSION, WORKSPACE_ID
from sift_mcp.db.protocols import ConnectionLike, safe_rollback
from sift_mcp.mapping.runner import (
    MappingInput,
    MappingResult,
    RecordRow,
    SampleRecord,
    run_mapping,
)
from sift_mcp.mapping.schema import (
    SchemaFieldInventory,
    SchemaInventory,
)
from sift_mcp.obs.logging import LogEvents, get_logger


def _jsonb(value: Any) -> Any:
    """Prepare a value for JSON column insertion.

    SQLite's registered adapters handle ``dict`` and ``list``
    serialization automatically.  Scalar values (``int``,
    ``float``, ``str``, ``bool``, ``None``) must be explicitly
    serialized to JSON text so that ``json_extract`` can operate
    on them.

    Args:
        value: A JSON-compatible Python value.

    Returns:
        The value unchanged for dict/list (adapter handles it),
        or a JSON text string for scalars.
    """
    if isinstance(value, (dict, list)):
        return value
    return json.dumps(value, ensure_ascii=False)


@dataclass(frozen=True)
class WorkerContext:
    """Immutable context snapshot for a mapping worker run.

    Capture the artifact state at the moment mapping is
    scheduled so that the worker can detect concurrent
    mutations via generation comparison.

    Attributes:
        artifact_id: Artifact to map.
        generation: Generation counter at scheduling time.
        map_status: Must be "pending" or "stale" to proceed.
    """

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


DELETE_RECORDS_SQL = """
DELETE FROM artifact_records
WHERE workspace_id = %s AND artifact_id = %s
"""

INSERT_RECORD_SQL = """
INSERT INTO artifact_records (
    workspace_id, artifact_id, root_path, idx, record
) VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (workspace_id, artifact_id, root_path, idx)
DO UPDATE SET record = EXCLUDED.record
"""


DELETE_SCHEMA_ROOTS_SQL = """
DELETE FROM artifact_schema_roots
WHERE workspace_id = %s AND artifact_id = %s
"""


INSERT_SCHEMA_ROOT_SQL = """
INSERT INTO artifact_schema_roots (
    workspace_id, artifact_id, root_key, root_path,
    schema_version, schema_hash, mode, completeness,
    observed_records, dataset_hash, traversal_contract_version,
    map_budget_fingerprint
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (workspace_id, artifact_id, root_key)
DO UPDATE SET
    root_path = EXCLUDED.root_path,
    schema_version = EXCLUDED.schema_version,
    schema_hash = EXCLUDED.schema_hash,
    mode = EXCLUDED.mode,
    completeness = EXCLUDED.completeness,
    observed_records = EXCLUDED.observed_records,
    dataset_hash = EXCLUDED.dataset_hash,
    traversal_contract_version = EXCLUDED.traversal_contract_version,
    map_budget_fingerprint = EXCLUDED.map_budget_fingerprint
"""


INSERT_SCHEMA_FIELD_SQL = """
INSERT INTO artifact_schema_fields (
    workspace_id, artifact_id, root_key, field_path,
    types, nullable, required, observed_count, example_value,
    distinct_values, cardinality
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (workspace_id, artifact_id, root_key, field_path)
DO UPDATE SET
    types = EXCLUDED.types,
    nullable = EXCLUDED.nullable,
    required = EXCLUDED.required,
    observed_count = EXCLUDED.observed_count,
    example_value = EXCLUDED.example_value,
    distinct_values = EXCLUDED.distinct_values,
    cardinality = EXCLUDED.cardinality
"""


def should_run_mapping(map_status: str) -> bool:
    """Check if mapping should run for the given status.

    Args:
        map_status: Current artifact map_status value.

    Returns:
        True if status is "pending" or "stale".
    """
    return map_status in ("pending", "stale")


def _cursor_rowcount(cursor: object) -> int:
    """Extract rowcount from a DB cursor, defaulting to 1.

    Args:
        cursor: Database cursor object with optional rowcount.

    Returns:
        The integer rowcount, or 1 if unavailable.
    """
    rowcount = getattr(cursor, "rowcount", None)
    if isinstance(rowcount, int):
        return rowcount
    return 1


def _root_sample_indices(root: Any) -> list[int] | None:
    """Extract and sort sample indices from a root inventory.

    Args:
        root: A RootInventory with a sample_indices attribute.

    Returns:
        Sorted list of integer indices, or None if not set.
    """
    raw = root.sample_indices
    if raw is None:
        return None
    return [int(index) for index in sorted(raw)]


def _root_insert_params(*, artifact_id: str, root: Any) -> tuple[object, ...]:
    """Build positional SQL parameters for root insertion.

    Args:
        artifact_id: Artifact owning this root.
        root: A RootInventory object.

    Returns:
        Ordered tuple matching INSERT_ROOT_SQL placeholders.
    """
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


def _sample_insert_params(
    *, artifact_id: str, sample: SampleRecord
) -> tuple[object, ...]:
    """Build positional SQL parameters for sample insertion.

    Args:
        artifact_id: Artifact owning this sample.
        sample: A SampleRecord object.

    Returns:
        Ordered tuple matching INSERT_SAMPLE_SQL placeholders.
    """
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


def _record_insert_params(
    *, artifact_id: str, row: RecordRow
) -> tuple[object, ...]:
    """Build positional SQL parameters for record insertion.

    Args:
        artifact_id: Artifact owning this record.
        row: A RecordRow object.

    Returns:
        Ordered tuple matching INSERT_RECORD_SQL placeholders.
    """
    return (
        WORKSPACE_ID,
        artifact_id,
        row.root_path,
        row.idx,
        _jsonb(row.record),
    )


def _schema_root_insert_params(
    *, artifact_id: str, schema: SchemaInventory
) -> tuple[object, ...]:
    """Build positional SQL parameters for schema-root insertion."""
    return (
        WORKSPACE_ID,
        artifact_id,
        schema.root_key,
        schema.root_path,
        schema.version,
        schema.schema_hash,
        schema.mode,
        schema.completeness,
        schema.observed_records,
        schema.dataset_hash,
        schema.traversal_contract_version,
        schema.map_budget_fingerprint,
    )


def _schema_field_insert_params(
    *,
    artifact_id: str,
    root_key: str,
    field: SchemaFieldInventory,
) -> tuple[object, ...]:
    """Build positional SQL parameters for schema-field insertion."""
    return (
        WORKSPACE_ID,
        artifact_id,
        root_key,
        field.path,
        _jsonb(field.types),
        field.nullable,
        field.required,
        field.observed_count,
        field.example_value,
        _jsonb(field.distinct_values),
        field.cardinality,
    )


def _group_samples(
    samples: list[SampleRecord] | None,
) -> dict[str, list[SampleRecord]]:
    """Group sample records by root_key.

    Args:
        samples: List of sample records, or None.

    Returns:
        A dict mapping root_key to list of SampleRecord.
    """
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
    """Verify sample indices match root inventory expectations.

    Args:
        result: Mapping result with root inventories.
        samples_by_root: Samples grouped by root_key.

    Raises:
        ValueError: If any root's sample_indices do not match
            the actual sample rows for that root.
    """
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
    """Persist mapping output with generation-safe conditional writes.

    Update the artifact row only if the generation and status
    still match, then replace roots and samples.  Roll back
    and return False on write conflict.

    Args:
        connection: Active database connection.
        worker_ctx: Worker context with artifact_id and
            expected generation.
        result: The mapping result to persist.

    Returns:
        True if the write succeeded, False on conflict.
    """
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
        _validate_sample_alignment(
            result=result, samples_by_root=samples_by_root
        )
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
                    _sample_insert_params(
                        artifact_id=worker_ctx.artifact_id, sample=sample
                    ),
                )

    connection.execute(
        DELETE_RECORDS_SQL,
        (WORKSPACE_ID, worker_ctx.artifact_id),
    )
    for row in result.record_rows or []:
        connection.execute(
            INSERT_RECORD_SQL,
            _record_insert_params(
                artifact_id=worker_ctx.artifact_id, row=row
            ),
        )

    connection.execute(
        DELETE_SCHEMA_ROOTS_SQL,
        (WORKSPACE_ID, worker_ctx.artifact_id),
    )

    for schema in result.schemas or []:
        connection.execute(
            INSERT_SCHEMA_ROOT_SQL,
            _schema_root_insert_params(
                artifact_id=worker_ctx.artifact_id,
                schema=schema,
            ),
        )
        for field in sorted(schema.fields, key=lambda item: item.path):
            connection.execute(
                INSERT_SCHEMA_FIELD_SQL,
                _schema_field_insert_params(
                    artifact_id=worker_ctx.artifact_id,
                    root_key=schema.root_key,
                    field=field,
                ),
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
    """Run mapping and persist results with conflict handling.

    Execute the mapping pipeline, persist results via
    generation-safe writes, and fall back to a "failed" record
    on write errors.  Record latency and outcome metrics.

    Args:
        connection: Active database connection.
        worker_ctx: Worker context with artifact state snapshot.
        mapping_input: Input bundle for the mapping pipeline.
        metrics: Optional metrics collector for latency and
            outcome counters.
        logger: Optional structured logger override.

    Returns:
        True if results were persisted, False if discarded
        due to generation/status race.
    """
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
    try:
        result = run_mapping(mapping_input)
    except Exception as exc:
        result = MappingResult(
            map_kind="none",
            map_status="failed",
            mapped_part_index=None,
            roots=[],
            map_budget_fingerprint=None,
            map_backend_id=None,
            prng_version=None,
            map_error=(f"mapping execution error: {type(exc).__name__}: {exc}"),
            samples=None,
        )
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
    """Record latency, outcome counters, and stop reasons.

    Args:
        metrics: Metrics collector, or None to skip.
        result: Completed mapping result for classification.
        duration_ms: Elapsed wall-clock milliseconds.
    """
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
        root.stop_reason if isinstance(root.stop_reason, str) else "none"
        for root in result.roots
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
    """Verify it is safe to write mapping results.

    Return True only when the artifact exists, is not deleted,
    has a pending or stale map_status, and the generation
    counter matches the expected value.

    Args:
        artifact_id: Artifact identifier (for context only).
        expected_generation: Generation counter at scheduling.
        current_row: Fresh DB row dict, or None if not found.

    Returns:
        True if all safety checks pass.
    """
    if current_row is None:
        return False
    if current_row.get("deleted_at") is not None:
        return False
    if current_row.get("map_status") not in ("pending", "stale"):
        return False
    return current_row.get("generation") == expected_generation
