"""Permanently remove soft-deleted artifacts and reclaim storage.

Executes a multi-step pipeline: find hard-delete candidates past
the grace period, delete artifact rows (cascading to refs and
samples), remove unreferenced payload blobs and binary blob
rows, and optionally unlink orphaned files from the filesystem.
Exports ``HardDeleteResult`` and ``run_hard_delete_batch``.
"""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
from typing import Any

from sift_gateway.constants import (
    BLOBS_PAYLOAD_SUBDIR,
    DEFAULT_DATA_DIR,
    WORKSPACE_ID,
)
from sift_gateway.db.protocols import increment_metric, safe_rollback
from sift_gateway.obs.logging import LogEvents, get_logger


@dataclass(frozen=True)
class HardDeleteResult:
    """Result of a hard delete batch.

    Attributes:
        artifacts_deleted: Number of artifact rows removed.
        payloads_deleted: Number of payload blob rows removed.
        binary_blobs_deleted: Number of binary blob rows removed.
        fs_blobs_removed: Number of blob files unlinked from
            the filesystem.
        bytes_reclaimed: Total bytes freed (payload + binary).
    """

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

# Step 2: Delete artifacts (cascades to roots, refs, samples)
DELETE_ARTIFACTS_BATCH_SQL = """
DELETE FROM artifacts
WHERE workspace_id = %s AND artifact_id = ANY(%s)
"""

# Step 3: Find unreferenced payloads
FIND_UNREFERENCED_PAYLOADS_SQL = """
SELECT pb.payload_hash_full, pb.payload_total_bytes, pb.payload_fs_path
FROM payload_blobs pb
WHERE pb.workspace_id = %s
  AND NOT EXISTS (
    SELECT 1 FROM artifacts a
    WHERE a.workspace_id = pb.workspace_id
      AND a.payload_hash_full = pb.payload_hash_full
  )
"""

# Step 4: Delete unreferenced payloads (cascades binary_refs, aliases)
DELETE_PAYLOADS_BATCH_SQL = """
DELETE FROM payload_blobs
WHERE workspace_id = %s AND payload_hash_full = ANY(%s)
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

# Step 6: Delete binary blob DB rows
DELETE_BLOBS_BATCH_SQL = """
DELETE FROM binary_blobs
WHERE workspace_id = %s AND binary_hash = ANY(%s)
"""


def hard_delete_candidates_params(
    grace_period_timestamp: str,
    batch_size: int = 50,
) -> tuple[object, ...]:
    """Build parameter tuple for FIND_HARD_DELETE_CANDIDATES_SQL.

    Args:
        grace_period_timestamp: ISO timestamp cutoff for the
            deleted_at grace period.
        batch_size: Maximum candidates to return.

    Returns:
        Parameter tuple for the hard-delete candidate query.
    """
    return (WORKSPACE_ID, grace_period_timestamp, batch_size)


def _remove_blob_file(fs_path: str) -> bool:
    """Attempt to unlink a blob file from the filesystem.

    Args:
        fs_path: Absolute path to the blob file.

    Returns:
        True if the file was removed, False on any error.
    """
    return _remove_blob_file_with_root(fs_path, blobs_root=None)


def _remove_blob_file_with_root(
    fs_path: str, *, blobs_root: Path | None
) -> bool:
    """Attempt to unlink a blob file with optional root confinement."""
    log = get_logger(component="jobs.hard_delete")
    try:
        target = Path(os.path.abspath(fs_path))
        if blobs_root is not None:
            root = Path(os.path.abspath(blobs_root)).resolve(strict=False)
            # Resolve the target so the confinement check and the
            # unlink operate on the same final path, closing the
            # TOCTOU window for symlink swaps.
            target = target.resolve(strict=False)
            try:
                target.relative_to(root)
            except ValueError:
                return False
        target.unlink()
        return True
    except FileNotFoundError:
        return False
    except (RuntimeError, OSError) as exc:
        log.warning(
            "blob file removal failed",
            fs_path=fs_path,
            error=str(exc),
        )
        return False


def _resolve_payloads_root(payloads_root: Path | None) -> Path:
    """Resolve payload root used for payload file deletion."""
    if payloads_root is not None:
        return payloads_root.resolve(strict=False)
    data_dir = Path(
        os.environ.get("SIFT_GATEWAY_DATA_DIR", DEFAULT_DATA_DIR)
    ).expanduser()
    return (data_dir / BLOBS_PAYLOAD_SUBDIR).resolve(strict=False)


def run_hard_delete_batch(
    connection: Any,
    *,
    grace_period_timestamp: str,
    batch_size: int = 50,
    remove_fs_blobs: bool = True,
    blobs_root: Path | None = None,
    payloads_root: Path | None = None,
    metrics: Any | None = None,
    logger: Any | None = None,
) -> HardDeleteResult:
    """Run one hard-delete batch and clean up orphaned storage.

    SQL uses Postgres-style syntax (``%s``, ``= ANY()``,
    ``FOR UPDATE SKIP LOCKED``) which the SQLite connection
    proxy rewrites transparently.

    Args:
        connection: Database connection for the transaction.
        grace_period_timestamp: ISO timestamp cutoff; only
            artifacts soft-deleted before this are eligible.
        batch_size: Maximum artifacts to process per batch.
        remove_fs_blobs: If True, unlink orphaned blob files
            from the filesystem after commit.
        blobs_root: Optional root directory used to constrain
            filesystem blob deletion paths.
        payloads_root: Optional payload root directory used to
            constrain payload file deletion paths.
        metrics: Optional GatewayMetrics for counter updates.
        logger: Optional structured logger override.

    Returns:
        A HardDeleteResult summarizing deletions and reclaimed
        bytes.
    """
    log = logger or get_logger(component="jobs.hard_delete")
    resolved_payloads_root = _resolve_payloads_root(payloads_root)
    try:
        candidate_rows = connection.execute(
            FIND_HARD_DELETE_CANDIDATES_SQL,
            hard_delete_candidates_params(
                grace_period_timestamp=grace_period_timestamp,
                batch_size=batch_size,
            ),
        ).fetchall()

        artifact_ids = [
            row[0]
            for row in candidate_rows
            if len(row) >= 1 and isinstance(row[0], str)
        ]
        if artifact_ids:
            connection.execute(
                DELETE_ARTIFACTS_BATCH_SQL,
                (WORKSPACE_ID, artifact_ids),
            )
        artifacts_deleted = len(artifact_ids)

        payload_rows = connection.execute(
            FIND_UNREFERENCED_PAYLOADS_SQL,
            (WORKSPACE_ID,),
        ).fetchall()
        payload_hashes = []
        payload_paths_to_remove: list[str] = []
        payload_bytes_reclaimed = 0
        for row in payload_rows:
            if len(row) < 3:
                continue
            payload_hash_full = row[0]
            payload_total_bytes = row[1]
            payload_fs_path = row[2]
            if not isinstance(payload_hash_full, str):
                continue
            payload_hashes.append(payload_hash_full)
            if isinstance(payload_total_bytes, int) and payload_total_bytes > 0:
                payload_bytes_reclaimed += payload_total_bytes
            if remove_fs_blobs and isinstance(payload_fs_path, str):
                payload_path = Path(payload_fs_path)
                if not payload_path.is_absolute():
                    payload_path = (
                        resolved_payloads_root / payload_path
                    ).resolve(strict=False)
                payload_paths_to_remove.append(str(payload_path))
        if payload_hashes:
            connection.execute(
                DELETE_PAYLOADS_BATCH_SQL,
                (WORKSPACE_ID, payload_hashes),
            )
        payloads_deleted = len(payload_hashes)

        blob_rows = connection.execute(
            FIND_UNREFERENCED_BLOBS_SQL,
            (WORKSPACE_ID,),
        ).fetchall()
        blob_hashes = []
        fs_blobs_removed = 0
        blob_bytes_reclaimed = 0
        fs_paths_to_remove: list[str] = []
        for row in blob_rows:
            if len(row) < 4:
                continue
            binary_hash = row[0]
            fs_path = row[2]
            byte_count = row[3]
            if not isinstance(binary_hash, str):
                continue
            blob_hashes.append(binary_hash)
            if isinstance(byte_count, int) and byte_count > 0:
                blob_bytes_reclaimed += byte_count
            if remove_fs_blobs and isinstance(fs_path, str):
                fs_paths_to_remove.append(fs_path)
        if blob_hashes:
            connection.execute(
                DELETE_BLOBS_BATCH_SQL,
                (WORKSPACE_ID, blob_hashes),
            )
        binary_blobs_deleted = len(blob_hashes)

        total_reclaimed = payload_bytes_reclaimed + blob_bytes_reclaimed
        connection.commit()

        # Remove FS blobs AFTER commit so a rollback doesn't orphan files
        for fs_path in payload_paths_to_remove:
            if _remove_blob_file_with_root(
                fs_path,
                blobs_root=resolved_payloads_root,
            ):
                fs_blobs_removed += 1
        for fs_path in fs_paths_to_remove:
            if _remove_blob_file_with_root(fs_path, blobs_root=blobs_root):
                fs_blobs_removed += 1

        increment_metric(metrics, "prune_hard_deletes", artifacts_deleted)
        increment_metric(metrics, "prune_bytes_reclaimed", total_reclaimed)
        increment_metric(metrics, "prune_fs_orphans_removed", fs_blobs_removed)
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
        safe_rollback(connection)
        raise
