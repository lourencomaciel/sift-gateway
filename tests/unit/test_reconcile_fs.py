from __future__ import annotations

from pathlib import Path

from mcp_artifact_gateway.jobs.reconcile_fs import (
    FETCH_ALL_BLOB_PATHS_SQL,
    find_missing,
    find_orphans,
    remove_orphan_files,
    run_reconcile,
    scan_blob_directory,
)
from mcp_artifact_gateway.obs.metrics import GatewayMetrics


def _create_blob_tree(base: Path, entries: dict[str, bytes]) -> None:
    """Create a blobs/bin directory tree with 2-level sharding.

    entries: mapping of binary_hash -> content.
    The hash name is used to derive the shard dirs (first 2 chars, next 2 chars).
    """
    for name, content in entries.items():
        shard1 = name[:2]
        shard2 = name[2:4]
        parent = base / shard1 / shard2
        parent.mkdir(parents=True, exist_ok=True)
        (parent / name).write_bytes(content)


def test_scan_blob_directory_finds_files(tmp_path: Path) -> None:
    blobs_dir = tmp_path / "blobs" / "bin"
    entries = {
        "aabbccdd1234": b"data1",
        "aabbee5678ff": b"data2",
        "11223344abcd": b"data3",
    }
    _create_blob_tree(blobs_dir, entries)

    found = scan_blob_directory(blobs_dir)
    assert set(found.keys()) == set(entries.keys())
    for name, path in found.items():
        assert path.exists()
        assert path.read_bytes() == entries[name]


def test_scan_blob_directory_empty(tmp_path: Path) -> None:
    blobs_dir = tmp_path / "blobs" / "bin"
    found = scan_blob_directory(blobs_dir)
    assert found == {}


def test_scan_blob_directory_skips_wrong_shard_names(tmp_path: Path) -> None:
    blobs_dir = tmp_path / "blobs" / "bin"
    blobs_dir.mkdir(parents=True)
    # Create a directory with wrong shard name length (3 chars instead of 2)
    wrong_dir = blobs_dir / "abc"
    wrong_dir.mkdir()
    (wrong_dir / "xx" / "somefile").parent.mkdir(parents=True)
    (wrong_dir / "xx" / "somefile").write_bytes(b"data")

    found = scan_blob_directory(blobs_dir)
    assert found == {}


def test_find_orphans_detects_files_not_in_db() -> None:
    fs_blobs = {
        "hash_a": Path("/fake/path/hash_a"),
        "hash_b": Path("/fake/path/hash_b"),
        "hash_c": Path("/fake/path/hash_c"),
    }
    db_hashes = {"hash_a", "hash_c"}

    orphans = find_orphans(fs_blobs, db_hashes)
    assert len(orphans) == 1
    assert orphans[0] == Path("/fake/path/hash_b")


def test_find_orphans_no_orphans() -> None:
    fs_blobs = {
        "hash_a": Path("/fake/path/hash_a"),
    }
    db_hashes = {"hash_a"}

    orphans = find_orphans(fs_blobs, db_hashes)
    assert orphans == []


def test_find_missing_detects_db_refs_without_files(tmp_path: Path) -> None:
    # Create one file that exists
    existing = tmp_path / "existing_blob"
    existing.write_bytes(b"data")

    db_paths = {
        "hash_exists": str(existing),
        "hash_missing": str(tmp_path / "nonexistent_blob"),
    }

    missing = find_missing(db_paths)
    assert missing == ["hash_missing"]


def test_find_missing_no_missing(tmp_path: Path) -> None:
    existing = tmp_path / "blob1"
    existing.write_bytes(b"data")

    db_paths = {"hash1": str(existing)}
    missing = find_missing(db_paths)
    assert missing == []


def test_remove_orphan_files_removes_and_counts(tmp_path: Path) -> None:
    blobs_dir = tmp_path / "blobs" / "bin"
    entries = {
        "aabb1111aaaa": b"orphan1",
        "aabb2222bbbb": b"orphan2",
    }
    _create_blob_tree(blobs_dir, entries)

    # Collect paths
    orphan_paths = []
    for name in entries:
        shard1 = name[:2]
        shard2 = name[2:4]
        orphan_paths.append(blobs_dir / shard1 / shard2 / name)

    # Verify files exist before removal
    for p in orphan_paths:
        assert p.exists()

    removed = remove_orphan_files(orphan_paths)
    assert removed == 2

    # Verify files are gone
    for p in orphan_paths:
        assert not p.exists()


def test_remove_orphan_files_cleans_empty_dirs(tmp_path: Path) -> None:
    blobs_dir = tmp_path / "blobs" / "bin"
    entries = {"aabb1111cccc": b"data"}
    _create_blob_tree(blobs_dir, entries)

    name = "aabb1111cccc"
    shard1 = name[:2]
    shard2 = name[2:4]
    orphan_path = blobs_dir / shard1 / shard2 / name

    removed = remove_orphan_files([orphan_path])
    assert removed == 1

    # Both shard directories should be cleaned up since they are empty
    assert not (blobs_dir / shard1 / shard2).exists()
    assert not (blobs_dir / shard1).exists()


def test_remove_orphan_files_preserves_nonempty_dirs(tmp_path: Path) -> None:
    blobs_dir = tmp_path / "blobs" / "bin"
    entries = {
        "aabb1111dddd": b"orphan",
        "aabb2222eeee": b"keep",
    }
    _create_blob_tree(blobs_dir, entries)

    # Only remove one file; the shard1 dir "aa" should remain since "aabb2222eeee" is still there
    orphan_path = blobs_dir / "aa" / "bb" / "aabb1111dddd"
    removed = remove_orphan_files([orphan_path])
    assert removed == 1

    # The shard1 dir should still exist since the other file keeps it
    assert (blobs_dir / "aa").exists()


# ---------------------------------------------------------------------------
# Tests for run_reconcile end-to-end orchestration
# ---------------------------------------------------------------------------


class _FakeCursor:
    def __init__(self, rows: list[tuple[object, ...]]) -> None:
        self._rows = rows

    def fetchall(self) -> list[tuple[object, ...]]:
        return list(self._rows)


class _FakeConnection:
    def __init__(self, rows: list[tuple[object, ...]] | None = None) -> None:
        self.rows = list(rows or [])

    def execute(self, _query: str, _params: tuple[object, ...] | None = None) -> _FakeCursor:
        return _FakeCursor(self.rows)


def test_run_reconcile_detects_orphans_report_only(tmp_path: Path) -> None:
    """run_reconcile with remove=False should detect orphans but not remove them."""
    blobs_dir = tmp_path / "blobs" / "bin"
    # Create files on disk: hash_a is in DB, hash_orphan is not
    _create_blob_tree(blobs_dir, {
        "aabb1111aaaa": b"known_data",
        "ccdd2222bbbb": b"orphan_data",
    })
    # DB only knows about hash_a
    db_rows = [
        ("aabb1111aaaa", str(blobs_dir / "aa" / "bb" / "aabb1111aaaa"), 10),
    ]
    connection = _FakeConnection(rows=db_rows)

    result = run_reconcile(connection, blobs_bin_dir=blobs_dir, remove=False)

    assert result.removed_count == 0
    assert len(result.orphan_files) == 1
    assert "ccdd2222bbbb" in result.orphan_files[0]
    assert result.missing_files == []
    assert result.orphan_bytes == len(b"orphan_data")
    # Orphan file should still exist (report only)
    assert (blobs_dir / "cc" / "dd" / "ccdd2222bbbb").exists()


def test_run_reconcile_removes_orphans(tmp_path: Path) -> None:
    """run_reconcile with remove=True should remove orphan files."""
    blobs_dir = tmp_path / "blobs" / "bin"
    _create_blob_tree(blobs_dir, {
        "aabb1111aaaa": b"known",
        "ccdd2222bbbb": b"orphan",
    })
    db_rows = [
        ("aabb1111aaaa", str(blobs_dir / "aa" / "bb" / "aabb1111aaaa"), 5),
    ]
    connection = _FakeConnection(rows=db_rows)

    result = run_reconcile(connection, blobs_bin_dir=blobs_dir, remove=True)

    assert result.removed_count == 1
    assert len(result.orphan_files) == 1
    assert not (blobs_dir / "cc" / "dd" / "ccdd2222bbbb").exists()


def test_run_reconcile_detects_missing_db_refs(tmp_path: Path) -> None:
    """run_reconcile should detect DB references to files that do not exist."""
    blobs_dir = tmp_path / "blobs" / "bin"
    blobs_dir.mkdir(parents=True)
    # DB references a file that does not exist on disk
    db_rows = [
        ("missing_hash", str(blobs_dir / "mi" / "ss" / "missing_hash"), 100),
    ]
    connection = _FakeConnection(rows=db_rows)

    result = run_reconcile(connection, blobs_bin_dir=blobs_dir, remove=False)

    assert result.orphan_files == []
    assert result.missing_files == ["missing_hash"]
    assert result.removed_count == 0


def test_run_reconcile_no_discrepancies(tmp_path: Path) -> None:
    """run_reconcile with everything in sync returns empty results."""
    blobs_dir = tmp_path / "blobs" / "bin"
    _create_blob_tree(blobs_dir, {"aabb1111cccc": b"data"})
    db_rows = [
        ("aabb1111cccc", str(blobs_dir / "aa" / "bb" / "aabb1111cccc"), 4),
    ]
    connection = _FakeConnection(rows=db_rows)

    result = run_reconcile(connection, blobs_bin_dir=blobs_dir, remove=False)

    assert result.orphan_files == []
    assert result.missing_files == []
    assert result.orphan_bytes == 0
    assert result.removed_count == 0


def test_run_reconcile_empty_db_and_empty_fs(tmp_path: Path) -> None:
    """run_reconcile with empty DB and empty FS returns empty results."""
    blobs_dir = tmp_path / "blobs" / "bin"
    connection = _FakeConnection(rows=[])

    result = run_reconcile(connection, blobs_bin_dir=blobs_dir, remove=False)

    assert result.orphan_files == []
    assert result.missing_files == []
    assert result.orphan_bytes == 0
    assert result.removed_count == 0


def test_run_reconcile_updates_metrics(tmp_path: Path) -> None:
    """run_reconcile with remove=True should update prune_fs_orphans_removed metric."""
    blobs_dir = tmp_path / "blobs" / "bin"
    _create_blob_tree(blobs_dir, {"ccdd3333eeee": b"orphan"})
    connection = _FakeConnection(rows=[])
    metrics = GatewayMetrics()

    result = run_reconcile(
        connection, blobs_bin_dir=blobs_dir, remove=True, metrics=metrics,
    )

    assert result.removed_count == 1
    assert metrics.prune_fs_orphans_removed.value == 1
