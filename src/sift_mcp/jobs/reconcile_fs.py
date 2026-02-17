"""Reconcile filesystem blob storage against database records.

Scans the ``blobs/bin`` directory tree and compares file names
against known binary hashes in the database.  Reports orphan
files (present on disk but absent from DB) and missing files
(referenced in DB but absent from disk).  Optionally removes
orphans and cleans up empty parent directories.
"""

from __future__ import annotations

import contextlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sift_mcp.constants import WORKSPACE_ID
from sift_mcp.obs.logging import LogEvents, get_logger


@dataclass(frozen=True)
class ReconcileResult:
    """Result of filesystem reconciliation.

    Attributes:
        orphan_files: File paths on disk not referenced in DB.
        missing_files: Binary hashes in DB with no file on disk.
        orphan_bytes: Total bytes of orphan files.
        removed_count: Number of orphan files actually removed
            (non-zero only when ``remove=True``).
    """

    orphan_files: list[str]  # paths not in DB
    missing_files: list[str]  # DB references without files
    orphan_bytes: int
    removed_count: int  # only if remove=True


# SQL to get all known blob paths
FETCH_ALL_BLOB_PATHS_SQL = """
SELECT binary_hash, fs_path, byte_count
FROM binary_blobs
WHERE workspace_id = %s
"""


def scan_blob_directory(blobs_bin_dir: Path) -> dict[str, Path]:
    """Scan the blobs/bin directory tree for blob files.

    Args:
        blobs_bin_dir: Root directory of the blob store.

    Returns:
        Dict mapping binary hash filenames to their paths.
    """
    found: dict[str, Path] = {}
    if not blobs_bin_dir.exists():
        return found

    for level1 in sorted(blobs_bin_dir.iterdir()):
        if not level1.is_dir() or len(level1.name) != 2:
            continue
        for level2 in sorted(level1.iterdir()):
            if not level2.is_dir() or len(level2.name) != 2:
                continue
            for blob_file in sorted(level2.iterdir()):
                if blob_file.is_file():
                    found[blob_file.name] = blob_file

    return found


def find_orphans(
    fs_blobs: dict[str, Path],
    db_hashes: set[str],
) -> list[Path]:
    """Find files on disk not referenced in the database.

    Args:
        fs_blobs: Dict of hash-name to path from disk scan.
        db_hashes: Set of binary hashes known to the database.

    Returns:
        List of filesystem paths for orphan blob files.
    """
    return [
        path
        for hash_name, path in fs_blobs.items()
        if hash_name not in db_hashes
    ]


def find_missing(
    db_paths: dict[str, str],
) -> list[str]:
    """Find database references with missing files on disk.

    Args:
        db_paths: Dict of binary_hash to fs_path from the
            database.

    Returns:
        List of binary hashes whose files are missing.
    """
    return [
        binary_hash
        for binary_hash, fs_path in db_paths.items()
        if not Path(fs_path).exists()
    ]


def remove_orphan_files(orphans: list[Path]) -> int:
    """Remove orphan files and clean up empty parent directories.

    Args:
        orphans: List of orphan file paths to unlink.

    Returns:
        Number of files successfully removed.
    """
    removed = 0
    for path in orphans:
        try:
            path.unlink()
            removed += 1
            # Clean up empty parent directories
            for parent in [path.parent, path.parent.parent]:
                try:
                    parent.rmdir()  # only removes if empty
                except OSError:
                    break
        except OSError:
            continue
    return removed


def _increment_metric(metrics: Any | None, attr: str, amount: int = 1) -> None:
    """Safely increment a Prometheus counter on a metrics object.

    Args:
        metrics: Optional GatewayMetrics instance.
        attr: Attribute name of the counter on metrics.
        amount: Value to increment the counter by.
    """
    if metrics is None:
        return
    counter = getattr(metrics, attr, None)
    increment = getattr(counter, "inc", None)
    if callable(increment):
        increment(amount)


def run_reconcile(
    connection: Any,
    *,
    blobs_bin_dir: Path,
    remove: bool = False,
    metrics: Any | None = None,
    logger: Any | None = None,
) -> ReconcileResult:
    """Run full filesystem reconciliation.

    Detects orphan files (on disk but not in DB) and missing
    files (in DB but not on disk), optionally removing orphans.

    Args:
        connection: Database connection for querying blob rows.
        blobs_bin_dir: Root directory of the blob store.
        remove: If True, unlink orphan files from disk.
        metrics: Optional GatewayMetrics for counter updates.
        logger: Optional structured logger override.

    Returns:
        A ReconcileResult with orphan and missing file details.
    """
    rows = connection.execute(
        FETCH_ALL_BLOB_PATHS_SQL,
        (WORKSPACE_ID,),
    ).fetchall()
    db_hashes: set[str] = set()
    db_paths: dict[str, str] = {}
    for row in rows:
        if len(row) < 3:
            continue
        binary_hash = row[0]
        fs_path = row[1]
        if isinstance(binary_hash, str) and isinstance(fs_path, str):
            db_hashes.add(binary_hash)
            db_paths[binary_hash] = fs_path
    fs_blobs = scan_blob_directory(blobs_bin_dir)
    orphan_paths = find_orphans(fs_blobs, db_hashes)
    missing_hashes = find_missing(db_paths)
    orphan_bytes = 0
    for path in orphan_paths:
        with contextlib.suppress(OSError):
            orphan_bytes += path.stat().st_size
    log = logger or get_logger(component="jobs.reconcile_fs")
    removed_count = 0
    if remove and orphan_paths:
        removed_count = remove_orphan_files(orphan_paths)
        _increment_metric(metrics, "prune_fs_orphans_removed", removed_count)
    log.info(
        LogEvents.PRUNE_FS_RECONCILE,
        orphan_count=len(orphan_paths),
        missing_count=len(missing_hashes),
        orphan_bytes=orphan_bytes,
        removed_count=removed_count,
        remove_mode=remove,
    )
    return ReconcileResult(
        orphan_files=[str(p) for p in orphan_paths],
        missing_files=missing_hashes,
        orphan_bytes=orphan_bytes,
        removed_count=removed_count,
    )
