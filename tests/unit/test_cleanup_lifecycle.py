from __future__ import annotations

from pathlib import Path

from sift_gateway.constants import WORKSPACE_ID
from sift_gateway.db.backend import SqliteBackend
from sift_gateway.db.migrate import apply_migrations
from sift_gateway.jobs.hard_delete import run_hard_delete_batch
from sift_gateway.jobs.quota import enforce_quota
from sift_gateway.jobs.reconcile_fs import run_reconcile
from sift_gateway.jobs.soft_delete import run_soft_delete_expired

_CANONICALIZER_VERSION = "jcs_rfc8785_v1"
_MAPPER_VERSION = "mapper_v1"
_MIGRATIONS_DIR = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "sift_gateway"
    / "db"
    / "migrations_sqlite"
)


def _insert_session(connection, session_id: str) -> None:
    connection.execute(
        """
        INSERT INTO sessions (workspace_id, session_id)
        VALUES (?, ?)
        """,
        (WORKSPACE_ID, session_id),
    )


def _insert_binary_blob(
    connection,
    *,
    binary_hash: str,
    fs_path: str,
    byte_count: int,
) -> None:
    connection.execute(
        """
        INSERT INTO binary_blobs (
            workspace_id,
            binary_hash,
            blob_id,
            byte_count,
            mime,
            fs_path
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            WORKSPACE_ID,
            binary_hash,
            f"bin_{binary_hash[:12]}",
            byte_count,
            "application/octet-stream",
            fs_path,
        ),
    )


def _insert_payload_blob(
    connection,
    *,
    payload_hash_full: str,
    payload_fs_path: str,
    payload_json_bytes: int,
    payload_binary_bytes_total: int,
) -> None:
    payload_total_bytes = payload_json_bytes + payload_binary_bytes_total
    connection.execute(
        """
        INSERT INTO payload_blobs (
            workspace_id,
            payload_hash_full,
            envelope_canonical_encoding,
            payload_fs_path,
            canonicalizer_version,
            payload_json_bytes,
            payload_binary_bytes_total,
            payload_total_bytes,
            contains_binary_refs
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            WORKSPACE_ID,
            payload_hash_full,
            "zstd",
            payload_fs_path,
            _CANONICALIZER_VERSION,
            payload_json_bytes,
            payload_binary_bytes_total,
            payload_total_bytes,
            1 if payload_binary_bytes_total > 0 else 0,
        ),
    )


def _insert_payload_binary_ref(
    connection,
    *,
    payload_hash_full: str,
    binary_hash: str,
) -> None:
    connection.execute(
        """
        INSERT INTO payload_binary_refs (
            workspace_id,
            payload_hash_full,
            binary_hash
        ) VALUES (?, ?, ?)
        """,
        (WORKSPACE_ID, payload_hash_full, binary_hash),
    )


def _insert_artifact(
    connection,
    *,
    artifact_id: str,
    session_id: str,
    payload_hash_full: str,
    payload_json_bytes: int,
    payload_binary_bytes_total: int,
    request_key: str,
    expires_at: str | None,
    last_referenced_at: str,
    capture_key: str | None = None,
) -> None:
    payload_total_bytes = payload_json_bytes + payload_binary_bytes_total
    connection.execute(
        """
        INSERT INTO artifacts (
            workspace_id,
            artifact_id,
            session_id,
            source_tool,
            upstream_instance_id,
            request_key,
            payload_hash_full,
            canonicalizer_version,
            payload_json_bytes,
            payload_binary_bytes_total,
            payload_total_bytes,
            expires_at,
            last_referenced_at,
            mapper_version,
            capture_kind,
            capture_origin,
            capture_key
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            WORKSPACE_ID,
            artifact_id,
            session_id,
            "cli.run",
            "cli_local",
            request_key,
            payload_hash_full,
            _CANONICALIZER_VERSION,
            payload_json_bytes,
            payload_binary_bytes_total,
            payload_total_bytes,
            expires_at,
            last_referenced_at,
            _MAPPER_VERSION,
            "cli_command",
            {"command_argv": ["echo", artifact_id]},
            capture_key or request_key,
        ),
    )


def _payload_relpath(payload_hash_full: str) -> str:
    return f"{payload_hash_full[:2]}/{payload_hash_full[2:4]}/{payload_hash_full}.zst"


def _payload_path(payload_root: Path, payload_hash_full: str) -> Path:
    return payload_root / _payload_relpath(payload_hash_full)


def _binary_path(bin_root: Path, binary_hash: str) -> Path:
    return bin_root / binary_hash[:2] / binary_hash[2:4] / binary_hash


def _setup_backend(tmp_path: Path) -> SqliteBackend:
    backend = SqliteBackend(db_path=tmp_path / "gateway.db")
    with backend.connection() as connection:
        apply_migrations(connection, _MIGRATIONS_DIR)
    return backend


def test_cleanup_lifecycle_expired_then_hard_delete_then_reconcile(
    tmp_path: Path,
) -> None:
    backend = _setup_backend(tmp_path)
    blobs_bin_dir = tmp_path / "blobs" / "bin"
    blobs_payload_dir = tmp_path / "blobs" / "payload"

    try:
        payload_hash = (
            "a1b2c3d4e5f6deadbeef00112233445566778899aabbccddeeff001122334455"
        )
        binary_hash = (
            "ddee11223344556677889900aabbccddeeff00112233445566778899aabbccdd"
        )
        payload_path = _payload_path(blobs_payload_dir, payload_hash)
        binary_path = _binary_path(blobs_bin_dir, binary_hash)
        payload_path.parent.mkdir(parents=True, exist_ok=True)
        binary_path.parent.mkdir(parents=True, exist_ok=True)
        payload_path.write_bytes(b"payload-bytes")
        binary_path.write_bytes(b"binary-bytes")

        with backend.connection() as connection:
            _insert_session(connection, "sess_cleanup")
            _insert_binary_blob(
                connection,
                binary_hash=binary_hash,
                fs_path=str(binary_path),
                byte_count=12,
            )
            _insert_payload_blob(
                connection,
                payload_hash_full=payload_hash,
                payload_fs_path=_payload_relpath(payload_hash),
                payload_json_bytes=16,
                payload_binary_bytes_total=12,
            )
            _insert_payload_binary_ref(
                connection,
                payload_hash_full=payload_hash,
                binary_hash=binary_hash,
            )
            _insert_artifact(
                connection,
                artifact_id="art_cleanup_expired",
                session_id="sess_cleanup",
                payload_hash_full=payload_hash,
                payload_json_bytes=16,
                payload_binary_bytes_total=12,
                request_key="req_cleanup_expired",
                expires_at="2000-01-01 00:00:00",
                last_referenced_at="2000-01-01 00:00:00",
            )
            connection.commit()

        with backend.connection() as connection:
            soft_deleted = run_soft_delete_expired(connection, batch_size=10)
            assert soft_deleted.deleted_count == 1
            assert soft_deleted.artifact_ids == ["art_cleanup_expired"]

        with backend.connection() as connection:
            hard_deleted = run_hard_delete_batch(
                connection,
                grace_period_timestamp="9999-01-01T00:00:00Z",
                batch_size=10,
                remove_fs_blobs=True,
                blobs_root=blobs_bin_dir,
                payloads_root=blobs_payload_dir,
            )

        assert hard_deleted.artifacts_deleted == 1
        assert hard_deleted.payloads_deleted == 1
        assert hard_deleted.binary_blobs_deleted == 1
        assert hard_deleted.fs_blobs_removed == 2
        assert not payload_path.exists()
        assert not binary_path.exists()

        orphan_binary_hash = (
            "ff00112233445566778899aabbccddeeff00112233445566778899aabbccddee"
        )
        orphan_payload_hash = (
            "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"
        )
        orphan_binary_path = _binary_path(blobs_bin_dir, orphan_binary_hash)
        orphan_payload_path = _payload_path(
            blobs_payload_dir, orphan_payload_hash
        )
        orphan_binary_path.parent.mkdir(parents=True, exist_ok=True)
        orphan_payload_path.parent.mkdir(parents=True, exist_ok=True)
        orphan_binary_path.write_bytes(b"orphan-bin")
        orphan_payload_path.write_bytes(b"orphan-payload")

        with backend.connection() as connection:
            report_only = run_reconcile(
                connection,
                blobs_bin_dir=blobs_bin_dir,
                blobs_payload_dir=blobs_payload_dir,
                remove=False,
            )
            assert report_only.removed_count == 0
            assert len(report_only.orphan_files) == 2
            assert report_only.missing_files == []
            assert report_only.payload_missing_files == []

            removed = run_reconcile(
                connection,
                blobs_bin_dir=blobs_bin_dir,
                blobs_payload_dir=blobs_payload_dir,
                remove=True,
            )

        assert removed.removed_count == 2
        assert not orphan_binary_path.exists()
        assert not orphan_payload_path.exists()
    finally:
        backend.close()


def test_cleanup_lifecycle_quota_enforcement_clears_usage(
    tmp_path: Path,
) -> None:
    backend = _setup_backend(tmp_path)
    blobs_bin_dir = tmp_path / "blobs" / "bin"
    blobs_payload_dir = tmp_path / "blobs" / "payload"

    try:
        old_payload_hash = (
            "111122223333444455556666777788889999aaaabbbbccccddddeeeeffff0000"
        )
        new_payload_hash = (
            "aaaa22223333444455556666777788889999aaaabbbbccccddddeeeeffff1111"
        )
        old_payload_path = _payload_path(blobs_payload_dir, old_payload_hash)
        new_payload_path = _payload_path(blobs_payload_dir, new_payload_hash)
        old_payload_path.parent.mkdir(parents=True, exist_ok=True)
        new_payload_path.parent.mkdir(parents=True, exist_ok=True)
        old_payload_path.write_bytes(b"old-payload")
        new_payload_path.write_bytes(b"new-payload")

        with backend.connection() as connection:
            _insert_session(connection, "sess_quota")
            _insert_payload_blob(
                connection,
                payload_hash_full=old_payload_hash,
                payload_fs_path=_payload_relpath(old_payload_hash),
                payload_json_bytes=120,
                payload_binary_bytes_total=0,
            )
            _insert_payload_blob(
                connection,
                payload_hash_full=new_payload_hash,
                payload_fs_path=_payload_relpath(new_payload_hash),
                payload_json_bytes=120,
                payload_binary_bytes_total=0,
            )
            _insert_artifact(
                connection,
                artifact_id="art_quota_old",
                session_id="sess_quota",
                payload_hash_full=old_payload_hash,
                payload_json_bytes=120,
                payload_binary_bytes_total=0,
                request_key="req_quota_old",
                expires_at="2999-01-01 00:00:00",
                last_referenced_at="2000-01-01 00:00:00",
            )
            _insert_artifact(
                connection,
                artifact_id="art_quota_new",
                session_id="sess_quota",
                payload_hash_full=new_payload_hash,
                payload_json_bytes=120,
                payload_binary_bytes_total=0,
                request_key="req_quota_new",
                expires_at="2999-01-01 00:00:00",
                last_referenced_at="2100-01-01 00:00:00",
            )
            connection.commit()

        with backend.connection() as connection:
            quota_result = enforce_quota(
                connection,
                max_binary_blob_bytes=10_000,
                max_payload_total_bytes=150,
                max_total_storage_bytes=150,
                prune_batch_size=1,
                max_prune_rounds=5,
                hard_delete_grace_seconds=0,
                remove_fs_blobs=True,
                blobs_root=blobs_bin_dir,
                payloads_root=blobs_payload_dir,
            )
            assert quota_result.pruned is True
            assert quota_result.space_cleared is True
            assert quota_result.soft_deleted_count >= 1
            assert quota_result.hard_deleted_count >= 1
            assert quota_result.bytes_reclaimed >= 120

            remaining_artifacts = connection.execute(
                """
                SELECT artifact_id
                FROM artifacts
                WHERE workspace_id = ?
                ORDER BY artifact_id
                """,
                (WORKSPACE_ID,),
            ).fetchall()
            assert [row[0] for row in remaining_artifacts] == ["art_quota_new"]

            reconcile_result = run_reconcile(
                connection,
                blobs_bin_dir=blobs_bin_dir,
                blobs_payload_dir=blobs_payload_dir,
                remove=False,
            )
            assert reconcile_result.orphan_files == []
            assert reconcile_result.missing_files == []
            assert reconcile_result.payload_orphan_files == []
            assert reconcile_result.payload_missing_files == []

        assert not old_payload_path.exists()
        assert new_payload_path.exists()
    finally:
        backend.close()
