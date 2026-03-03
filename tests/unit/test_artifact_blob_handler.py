"""Tests for artifact blob_list and blob_materialize actions."""

from __future__ import annotations

import asyncio
import csv
import json
import os
from pathlib import Path
import time

import pytest

from sift_gateway.config.settings import GatewayConfig
from sift_gateway.constants import WORKSPACE_ID
from sift_gateway.db.backend import SqliteBackend
from sift_gateway.db.migrate import apply_migrations
from sift_gateway.fs.blob_store import BlobStore
from sift_gateway.mcp.server import GatewayServer


def _migrations_dir() -> Path:
    return (
        Path(__file__).resolve().parents[2]
        / "src"
        / "sift_gateway"
        / "db"
        / "migrations_sqlite"
    )


def _build_server(tmp_path: Path) -> tuple[GatewayServer, SqliteBackend]:
    config = GatewayConfig(data_dir=tmp_path)
    config.state_dir.mkdir(parents=True, exist_ok=True)
    backend = SqliteBackend(db_path=config.sqlite_path)
    with backend.connection() as connection:
        apply_migrations(connection, _migrations_dir())
    blob_store = BlobStore(config.blobs_bin_dir)
    server = GatewayServer(config=config, db_pool=backend, blob_store=blob_store)
    return server, backend


def _seed_blob_for_two_artifacts(
    server: GatewayServer,
    backend: SqliteBackend,
    *,
    payload_hash: str,
) -> dict[str, str]:
    assert server.blob_store is not None
    blob_ref = server.blob_store.put_bytes(b"video-bytes", mime="video/mp4")
    with backend.connection() as connection:
        connection.execute(
            """
            INSERT INTO sessions (workspace_id, session_id)
            VALUES (%s, %s)
            """,
            (WORKSPACE_ID, "sess_1"),
        )
        connection.execute(
            """
            INSERT INTO binary_blobs (
                workspace_id, binary_hash, blob_id, byte_count,
                mime, fs_path, probe_head_hash, probe_tail_hash, probe_bytes
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                WORKSPACE_ID,
                blob_ref.binary_hash,
                blob_ref.blob_id,
                blob_ref.byte_count,
                blob_ref.mime,
                blob_ref.fs_path,
                blob_ref.probe_head_hash,
                blob_ref.probe_tail_hash,
                blob_ref.probe_bytes,
            ),
        )
        connection.execute(
            """
            INSERT INTO payload_blobs (
                workspace_id, payload_hash_full, envelope,
                envelope_canonical_encoding, payload_fs_path,
                canonicalizer_version, payload_json_bytes,
                payload_binary_bytes_total, payload_total_bytes,
                contains_binary_refs
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                WORKSPACE_ID,
                payload_hash,
                None,
                "none",
                "aa/bb/payload.zst",
                "jcs_rfc8785_v1",
                2,
                blob_ref.byte_count,
                blob_ref.byte_count + 2,
                1,
            ),
        )
        connection.execute(
            """
            INSERT INTO payload_binary_refs (
                workspace_id, payload_hash_full, binary_hash
            ) VALUES (%s, %s, %s)
            """,
            (WORKSPACE_ID, payload_hash, blob_ref.binary_hash),
        )
        connection.execute(
            """
            INSERT INTO artifacts (
                workspace_id, artifact_id, session_id, source_tool,
                upstream_instance_id, request_key, payload_hash_full,
                canonicalizer_version, payload_json_bytes,
                payload_binary_bytes_total, payload_total_bytes,
                mapper_version, parent_artifact_id, chain_seq
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                WORKSPACE_ID,
                "art_1",
                "sess_1",
                "meta-ads.get_ads",
                "inst_meta",
                "req_1",
                payload_hash,
                "jcs_rfc8785_v1",
                2,
                blob_ref.byte_count,
                blob_ref.byte_count + 2,
                "mapper_v1",
                None,
                None,
            ),
        )
        connection.execute(
            """
            INSERT INTO artifacts (
                workspace_id, artifact_id, session_id, source_tool,
                upstream_instance_id, request_key, payload_hash_full,
                canonicalizer_version, payload_json_bytes,
                payload_binary_bytes_total, payload_total_bytes,
                mapper_version, parent_artifact_id, chain_seq
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                WORKSPACE_ID,
                "art_2",
                "sess_1",
                "meta-ads.get_ads",
                "inst_meta",
                "req_2",
                payload_hash,
                "jcs_rfc8785_v1",
                2,
                blob_ref.byte_count,
                blob_ref.byte_count + 2,
                "mapper_v1",
                "art_1",
                1,
            ),
        )
        connection.commit()
    return {"blob_id": blob_ref.blob_id, "binary_hash": blob_ref.binary_hash}


def test_blob_list_all_related_deduplicates_blob_rows(tmp_path: Path) -> None:
    server, backend = _build_server(tmp_path)
    try:
        blob_info = _seed_blob_for_two_artifacts(
            server,
            backend,
            payload_hash="ph_blob_list",
        )
        response = asyncio.run(
            server.handle_artifact(
                {
                    "action": "blob_list",
                    "_gateway_context": {"session_id": "sess_1"},
                    "artifact_id": "art_1",
                    "scope": "all_related",
                }
            )
        )
        assert response["action"] == "blob_list"
        assert response["blob_count"] == 1
        blob_row = response["blobs"][0]
        assert blob_row["blob_id"] == blob_info["blob_id"]
        assert blob_row["artifact_count"] == 2
        assert set(blob_row["artifact_ids"]) == {"art_1", "art_2"}
        assert blob_row["mime"] == "video/mp4"
    finally:
        backend.close()


def test_blob_materialize_uses_mime_when_magic_unavailable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    server, backend = _build_server(tmp_path)
    try:
        blob_info = _seed_blob_for_two_artifacts(
            server,
            backend,
            payload_hash="ph_materialize_mime",
        )
        monkeypatch.setattr(
            "sift_gateway.mcp.handlers.artifact_blob._detect_mime_with_python_magic",
            lambda _path: None,
        )
        response = asyncio.run(
            server.handle_artifact(
                {
                    "action": "blob_materialize",
                    "_gateway_context": {"session_id": "sess_1"},
                    "blob_id": blob_info["blob_id"],
                    "filename": "creative",
                    "destination_dir": str(server.config.tmp_dir),
                }
            )
        )
        assert response["action"] == "blob_materialize"
        assert response["resolved_extension"] == ".mp4"
        assert response["resolved_from"] == "mime"
        assert response["sha256"] == blob_info["binary_hash"]
        assert response["materialize_mode_used"] == "copy"
        output_path = Path(response["path"])
        assert output_path.exists()
        assert output_path.name == "creative.mp4"
        assert output_path.read_bytes() == b"video-bytes"
    finally:
        backend.close()


def test_blob_materialize_auto_falls_back_to_copy_when_hardlink_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    server, backend = _build_server(tmp_path)
    try:
        blob_info = _seed_blob_for_two_artifacts(
            server,
            backend,
            payload_hash="ph_materialize_auto",
        )
        monkeypatch.setattr(
            "os.link",
            lambda _src, _dst: (_ for _ in ()).throw(OSError("no link")),
        )
        response = asyncio.run(
            server.handle_artifact(
                {
                    "action": "blob_materialize",
                    "_gateway_context": {"session_id": "sess_1"},
                    "blob_id": blob_info["blob_id"],
                    "filename": "auto_target",
                    "materialize_mode": "auto",
                }
            )
        )
        assert response["action"] == "blob_materialize"
        assert response["materialize_mode"] == "auto"
        assert response["materialize_mode_used"] == "copy"
        output_path = Path(response["path"])
        assert output_path.exists()
    finally:
        backend.close()


def test_blob_materialize_rejects_destination_outside_staging_root(
    tmp_path: Path,
) -> None:
    server, backend = _build_server(tmp_path)
    try:
        blob_info = _seed_blob_for_two_artifacts(
            server,
            backend,
            payload_hash="ph_materialize_outside",
        )
        outside = tmp_path / "outside"
        response = asyncio.run(
            server.handle_artifact(
                {
                    "action": "blob_materialize",
                    "_gateway_context": {"session_id": "sess_1"},
                    "blob_id": blob_info["blob_id"],
                    "destination_dir": str(outside),
                }
            )
        )
        assert response["code"] == "INVALID_ARGUMENT"
        assert "allowed staging root" in response["message"]
    finally:
        backend.close()


def test_blob_materialize_honors_max_bytes_guardrail(
    tmp_path: Path,
) -> None:
    server, backend = _build_server(tmp_path)
    try:
        blob_info = _seed_blob_for_two_artifacts(
            server,
            backend,
            payload_hash="ph_materialize_guardrail",
        )
        response = asyncio.run(
            server.handle_artifact(
                {
                    "action": "blob_materialize",
                    "_gateway_context": {"session_id": "sess_1"},
                    "blob_id": blob_info["blob_id"],
                    "max_bytes": 4,
                }
            )
        )
        assert response["code"] == "RESOURCE_EXHAUSTED"
        assert "max_bytes" in response["message"]
    finally:
        backend.close()


def test_blob_cleanup_removes_explicit_staged_file(tmp_path: Path) -> None:
    server, backend = _build_server(tmp_path)
    try:
        blob_info = _seed_blob_for_two_artifacts(
            server,
            backend,
            payload_hash="ph_cleanup_explicit",
        )
        materialized = asyncio.run(
            server.handle_artifact(
                {
                    "action": "blob_materialize",
                    "_gateway_context": {"session_id": "sess_1"},
                    "blob_id": blob_info["blob_id"],
                    "filename": "cleanup_target",
                }
            )
        )
        target = Path(materialized["path"])
        assert target.exists()
        cleaned = asyncio.run(
            server.handle_artifact(
                {
                    "action": "blob_cleanup",
                    "_gateway_context": {"session_id": "sess_1"},
                    "path": str(target),
                }
            )
        )
        assert cleaned["action"] == "blob_cleanup"
        assert cleaned["deleted_count"] == 1
        assert not target.exists()
    finally:
        backend.close()


def test_blob_cleanup_dry_run_does_not_delete(tmp_path: Path) -> None:
    server, backend = _build_server(tmp_path)
    try:
        blob_info = _seed_blob_for_two_artifacts(
            server,
            backend,
            payload_hash="ph_cleanup_dry_run",
        )
        materialized = asyncio.run(
            server.handle_artifact(
                {
                    "action": "blob_materialize",
                    "_gateway_context": {"session_id": "sess_1"},
                    "blob_id": blob_info["blob_id"],
                    "filename": "dry_run_target",
                }
            )
        )
        target = Path(materialized["path"])
        assert target.exists()
        cleaned = asyncio.run(
            server.handle_artifact(
                {
                    "action": "blob_cleanup",
                    "_gateway_context": {"session_id": "sess_1"},
                    "path": str(target),
                    "dry_run": True,
                }
            )
        )
        assert cleaned["deleted_count"] == 0
        assert cleaned["would_delete_count"] == 1
        assert target.exists()
    finally:
        backend.close()


def test_blob_cleanup_sweep_respects_older_than_seconds(
    tmp_path: Path,
) -> None:
    server, backend = _build_server(tmp_path)
    try:
        staging = server.config.tmp_dir / "materialized_blobs"
        staging.mkdir(parents=True, exist_ok=True)
        old_file = staging / "old.bin"
        new_file = staging / "new.bin"
        old_file.write_bytes(b"old")
        new_file.write_bytes(b"new")
        old_mtime = time.time() - 7_200
        os.utime(old_file, (old_mtime, old_mtime))

        cleaned = asyncio.run(
            server.handle_artifact(
                {
                    "action": "blob_cleanup",
                    "_gateway_context": {"session_id": "sess_1"},
                    "destination_dir": str(staging),
                    "older_than_seconds": 3_600,
                }
            )
        )
        assert cleaned["deleted_count"] == 1
        assert not old_file.exists()
        assert new_file.exists()
    finally:
        backend.close()


def test_blob_cleanup_rejects_path_outside_allowed_roots(
    tmp_path: Path,
) -> None:
    server, backend = _build_server(tmp_path)
    try:
        outside = tmp_path / "outside" / "x.bin"
        outside.parent.mkdir(parents=True, exist_ok=True)
        outside.write_bytes(b"x")
        response = asyncio.run(
            server.handle_artifact(
                {
                    "action": "blob_cleanup",
                    "_gateway_context": {"session_id": "sess_1"},
                    "path": str(outside),
                }
            )
        )
        assert response["code"] == "INVALID_ARGUMENT"
        assert "allowed staging root" in response["message"]
    finally:
        backend.close()


def test_blob_manifest_writes_csv_file(tmp_path: Path) -> None:
    server, backend = _build_server(tmp_path)
    try:
        _seed_blob_for_two_artifacts(
            server,
            backend,
            payload_hash="ph_manifest_csv",
        )
        response = asyncio.run(
            server.handle_artifact(
                {
                    "action": "blob_manifest",
                    "_gateway_context": {"session_id": "sess_1"},
                    "artifact_id": "art_1",
                    "scope": "all_related",
                    "format": "csv",
                    "filename": "manifest_export",
                }
            )
        )
        assert response["action"] == "blob_manifest"
        assert response["format"] == "csv"
        manifest_path = Path(response["path"])
        assert manifest_path.exists()
        assert manifest_path.suffix == ".csv"
        with manifest_path.open("r", encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle))
        assert len(rows) == 1
        assert rows[0]["mime"] == "video/mp4"
    finally:
        backend.close()


def test_blob_manifest_writes_json_file(tmp_path: Path) -> None:
    server, backend = _build_server(tmp_path)
    try:
        _seed_blob_for_two_artifacts(
            server,
            backend,
            payload_hash="ph_manifest_json",
        )
        response = asyncio.run(
            server.handle_artifact(
                {
                    "action": "blob_manifest",
                    "_gateway_context": {"session_id": "sess_1"},
                    "artifact_ids": ["art_1", "art_2"],
                    "format": "json",
                    "filename": "manifest.json",
                }
            )
        )
        assert response["action"] == "blob_manifest"
        assert response["format"] == "json"
        manifest_path = Path(response["path"])
        assert manifest_path.exists()
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        assert payload["action"] == "blob_manifest"
        assert payload["blob_count"] == 1
        assert payload["blobs"][0]["mime"] == "video/mp4"
    finally:
        backend.close()
