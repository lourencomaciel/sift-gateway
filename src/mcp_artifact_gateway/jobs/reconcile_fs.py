"""Filesystem reconciliation: detect and optionally remove orphan files."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from mcp_artifact_gateway.constants import WORKSPACE_ID
from mcp_artifact_gateway.obs.logging import LogEvents, get_logger


@dataclass(frozen=True)
class ReconcileResult:
    """Result of filesystem reconciliation."""
    orphan_files: list[str]        # paths not in DB
    missing_files: list[str]       # DB references without files
    orphan_bytes: int
    removed_count: int             # only if remove=True


# SQL to get all known blob paths
FETCH_ALL_BLOB_PATHS_SQL = """
SELECT binary_hash, fs_path, byte_count
FROM binary_blobs
WHERE workspace_id = %s
"""


def scan_blob_directory(blobs_bin_dir: Path) -> dict[str, Path]:
    """Scan the blobs/bin directory tree and return {binary_hash: path}."""
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
    """Find files on disk not referenced in DB."""
    return [path for hash_name, path in fs_blobs.items() if hash_name not in db_hashes]


def find_missing(
    db_paths: dict[str, str],
) -> list[str]:
    """Find DB references with missing files."""
    return [
        binary_hash
        for binary_hash, fs_path in db_paths.items()
        if not Path(fs_path).exists()
    ]


def remove_orphan_files(orphans: list[Path]) -> int:
    """Remove orphan files and return count removed."""
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
    if metrics is None:
        return
    counter = getattr(metrics, attr, None)
    increment = getattr(counter, "increment", None)
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
    """Run full filesystem reconciliation: detect and optionally remove orphans.

    Steps:
    1. Query DB for all known binary blob paths.
    2. Scan the blobs/bin directory for files on disk.
    3. Compare to find orphan files (on disk but not in DB) and missing files
       (in DB but not on disk).
    4. Optionally remove orphan files.
    5. Return a ReconcileResult with findings.
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
        try:
            orphan_bytes += path.stat().st_size
        except OSError:
            pass
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
