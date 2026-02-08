"""Hard delete job: permanently removes artifacts and cleans up storage."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from mcp_artifact_gateway.constants import WORKSPACE_ID
from mcp_artifact_gateway.obs.logging import LogEvents, get_logger


@dataclass(frozen=True)
class HardDeleteResult:
    """Result of a hard delete batch."""
    artifacts_deleted: int
    payloads_deleted: int
    binary_blobs_deleted: int
    fs_blobs_removed: int
    bytes_reclaimed: int


# Step 1: Find artifacts eligible for hard delete
FIND_HARD_DELETE_CANDIDATES_SQL = """
SELECT artifact_id, payload_hash_full
FROM artifacts
WHERE workspace_id = %s
  AND deleted_at IS NOT NULL
  AND deleted_at < %s
ORDER BY deleted_at ASC
LIMIT %s
FOR UPDATE SKIP LOCKED
"""

# Step 2: Delete artifact (cascades to artifact_roots, artifact_refs, artifact_samples)
DELETE_ARTIFACT_SQL = """
DELETE FROM artifacts
WHERE workspace_id = %s AND artifact_id = %s
"""

# Step 3: Find unreferenced payloads
FIND_UNREFERENCED_PAYLOADS_SQL = """
SELECT pb.payload_hash_full, pb.payload_total_bytes
FROM payload_blobs pb
WHERE pb.workspace_id = %s
  AND NOT EXISTS (
    SELECT 1 FROM artifacts a
    WHERE a.workspace_id = pb.workspace_id
      AND a.payload_hash_full = pb.payload_hash_full
  )
"""

# Step 4: Delete unreferenced payloads (cascades payload_binary_refs, payload_hash_aliases)
DELETE_PAYLOAD_SQL = """
DELETE FROM payload_blobs
WHERE workspace_id = %s AND payload_hash_full = %s
"""

# Step 5: Find unreferenced binary blobs
FIND_UNREFERENCED_BLOBS_SQL = """
SELECT bb.binary_hash, bb.blob_id, bb.fs_path, bb.byte_count
FROM binary_blobs bb
WHERE bb.workspace_id = %s
  AND NOT EXISTS (
    SELECT 1 FROM payload_binary_refs pbr
    WHERE pbr.workspace_id = bb.workspace_id
      AND pbr.binary_hash = bb.binary_hash
  )
"""

# Step 6: Delete binary blob DB row
DELETE_BLOB_SQL = """
DELETE FROM binary_blobs
WHERE workspace_id = %s AND binary_hash = %s
"""


def hard_delete_candidates_params(
    grace_period_timestamp: str,
    batch_size: int = 50,
) -> tuple[object, ...]:
    """Params for FIND_HARD_DELETE_CANDIDATES_SQL."""
    return (WORKSPACE_ID, grace_period_timestamp, batch_size)


def _safe_rollback(connection: object) -> None:
    rollback = getattr(connection, "rollback", None)
    if callable(rollback):
        rollback()


def _increment_metric(metrics: Any | None, attr: str, amount: int = 1) -> None:
    if metrics is None:
        return
    counter = getattr(metrics, attr, None)
    increment = getattr(counter, "increment", None)
    if callable(increment):
        increment(amount)


def _remove_blob_file(fs_path: str) -> bool:
    try:
        Path(fs_path).unlink()
        return True
    except FileNotFoundError:
        return False
    except OSError:
        return False


def run_hard_delete_batch(
    connection: Any,
    *,
    grace_period_timestamp: str,
    batch_size: int = 50,
    remove_fs_blobs: bool = True,
    metrics: Any | None = None,
    logger: Any | None = None,
) -> HardDeleteResult:
    """Run one hard-delete batch and cleanup orphaned payload/blob storage."""
    log = logger or get_logger(component="jobs.hard_delete")
    try:
        candidate_rows = connection.execute(
            FIND_HARD_DELETE_CANDIDATES_SQL,
            hard_delete_candidates_params(
                grace_period_timestamp=grace_period_timestamp,
                batch_size=batch_size,
            ),
        ).fetchall()

        artifacts_deleted = 0
        for row in candidate_rows:
            if len(row) < 1:
                continue
            artifact_id = row[0]
            if not isinstance(artifact_id, str):
                continue
            connection.execute(
                DELETE_ARTIFACT_SQL,
                (WORKSPACE_ID, artifact_id),
            )
            artifacts_deleted += 1

        payload_rows = connection.execute(
            FIND_UNREFERENCED_PAYLOADS_SQL,
            (WORKSPACE_ID,),
        ).fetchall()
        payloads_deleted = 0
        payload_bytes_reclaimed = 0
        for row in payload_rows:
            if len(row) < 2:
                continue
            payload_hash_full = row[0]
            payload_total_bytes = row[1]
            if not isinstance(payload_hash_full, str):
                continue
            connection.execute(
                DELETE_PAYLOAD_SQL,
                (WORKSPACE_ID, payload_hash_full),
            )
            payloads_deleted += 1
            if isinstance(payload_total_bytes, int) and payload_total_bytes > 0:
                payload_bytes_reclaimed += payload_total_bytes

        blob_rows = connection.execute(
            FIND_UNREFERENCED_BLOBS_SQL,
            (WORKSPACE_ID,),
        ).fetchall()
        binary_blobs_deleted = 0
        fs_blobs_removed = 0
        blob_bytes_reclaimed = 0
        for row in blob_rows:
            if len(row) < 4:
                continue
            binary_hash = row[0]
            fs_path = row[2]
            byte_count = row[3]
            if not isinstance(binary_hash, str):
                continue
            connection.execute(
                DELETE_BLOB_SQL,
                (WORKSPACE_ID, binary_hash),
            )
            binary_blobs_deleted += 1
            if isinstance(byte_count, int) and byte_count > 0:
                blob_bytes_reclaimed += byte_count
            if remove_fs_blobs and isinstance(fs_path, str):
                if _remove_blob_file(fs_path):
                    fs_blobs_removed += 1

        total_reclaimed = payload_bytes_reclaimed + blob_bytes_reclaimed
        connection.commit()
        _increment_metric(metrics, "prune_hard_deletes", artifacts_deleted)
        _increment_metric(metrics, "prune_bytes_reclaimed", total_reclaimed)
        _increment_metric(metrics, "prune_fs_orphans_removed", fs_blobs_removed)
        if artifacts_deleted > 0:
            log.info(
                LogEvents.PRUNE_HARD_DELETE,
                artifacts_deleted=artifacts_deleted,
                payloads_deleted=payloads_deleted,
                binary_blobs_deleted=binary_blobs_deleted,
                fs_blobs_removed=fs_blobs_removed,
            )
        if total_reclaimed > 0:
            log.info(
                LogEvents.PRUNE_BYTES_RECLAIMED,
                bytes_reclaimed=total_reclaimed,
            )
        return HardDeleteResult(
            artifacts_deleted=artifacts_deleted,
            payloads_deleted=payloads_deleted,
            binary_blobs_deleted=binary_blobs_deleted,
            fs_blobs_removed=fs_blobs_removed,
            bytes_reclaimed=total_reclaimed,
        )
    except Exception:
        _safe_rollback(connection)
        raise
