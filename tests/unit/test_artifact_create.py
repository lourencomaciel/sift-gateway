"""Tests for the artifact creation pipeline."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from sidepouch_mcp.artifacts.create import (
    INSERT_ARTIFACT_SQL,
    INSERT_PAYLOAD_BINARY_REF_SQL,
    UPSERT_ARTIFACT_REF_SQL,
    CreateArtifactInput,
    build_artifact_row,
    compute_payload_sizes,
    generate_artifact_id,
    persist_artifact,
    prepare_envelope_storage,
)
from sidepouch_mcp.canon.rfc8785 import canonical_bytes
from sidepouch_mcp.config.settings import (
    EnvelopeJsonbMode,
    GatewayConfig,
)
from sidepouch_mcp.constants import (
    ARTIFACT_ID_PREFIX,
    CANONICALIZER_VERSION,
    MAPPER_VERSION,
    WORKSPACE_ID,
)
from sidepouch_mcp.envelope.model import (
    BinaryRefContentPart,
    Envelope,
    ErrorBlock,
    JsonContentPart,
    TextContentPart,
)
from sidepouch_mcp.obs.metrics import GatewayMetrics, counter_value
from sidepouch_mcp.util.hashing import sha256_hex


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _ok_envelope(**overrides: Any) -> Envelope:
    defaults: dict[str, Any] = {
        "upstream_instance_id": "up_1",
        "upstream_prefix": "github",
        "tool": "search_issues",
        "status": "ok",
        "content": [JsonContentPart(value={"key": "value"})],
        "meta": {"warnings": []},
    }
    defaults.update(overrides)
    return Envelope(**defaults)


def _error_envelope() -> Envelope:
    return Envelope(
        upstream_instance_id="up_1",
        upstream_prefix="github",
        tool="search_issues",
        status="error",
        content=[],
        error=ErrorBlock(code="UPSTREAM_ERROR", message="something broke"),
        meta={"warnings": []},
    )


def _binary_ref_part(byte_count: int = 1024) -> BinaryRefContentPart:
    return BinaryRefContentPart(
        blob_id="bin_abc123",
        binary_hash="deadbeef" * 8,
        mime="application/octet-stream",
        byte_count=byte_count,
    )


def _sample_input(envelope: Envelope | None = None) -> CreateArtifactInput:
    return CreateArtifactInput(
        session_id="sess_1",
        upstream_instance_id="up_1",
        prefix="github",
        tool_name="search_issues",
        request_key="rk_abc",
        request_args_hash="arghash_abc",
        request_args_prefix="prefix_abc",
        upstream_tool_schema_hash="schema_hash_abc",
        envelope=envelope or _ok_envelope(),
    )


def _config(tmp_path: Path, **overrides: Any) -> GatewayConfig:
    defaults: dict[str, Any] = {"data_dir": tmp_path}
    defaults.update(overrides)
    return GatewayConfig(**defaults)


# ---------------------------------------------------------------------------
# generate_artifact_id
# ---------------------------------------------------------------------------
def test_generate_artifact_id_starts_with_prefix() -> None:
    aid = generate_artifact_id()
    assert aid.startswith(ARTIFACT_ID_PREFIX)


def test_generate_artifact_id_is_unique() -> None:
    ids = {generate_artifact_id() for _ in range(100)}
    assert len(ids) == 100


# ---------------------------------------------------------------------------
# compute_payload_sizes
# ---------------------------------------------------------------------------
def test_compute_payload_sizes_json_only() -> None:
    value = {"key": "value"}
    envelope = _ok_envelope(content=[JsonContentPart(value=value)])
    json_bytes, binary_bytes, total_bytes = compute_payload_sizes(envelope)

    expected_json = len(json.dumps(value, ensure_ascii=False).encode("utf-8"))
    assert json_bytes == expected_json
    assert binary_bytes == 0
    assert total_bytes == expected_json


def test_compute_payload_sizes_binary_refs() -> None:
    envelope = _ok_envelope(content=[_binary_ref_part(byte_count=2048)])
    json_bytes, binary_bytes, total_bytes = compute_payload_sizes(envelope)

    assert json_bytes == 0
    assert binary_bytes == 2048
    assert total_bytes == 2048


def test_compute_payload_sizes_mixed_content() -> None:
    json_value = {"a": 1}
    binary_part = _binary_ref_part(byte_count=512)
    text_part = TextContentPart(text="hello")

    envelope = _ok_envelope(
        content=[JsonContentPart(value=json_value), binary_part, text_part]
    )
    json_bytes, binary_bytes, total_bytes = compute_payload_sizes(envelope)

    expected_json_from_value = len(
        json.dumps(json_value, ensure_ascii=False).encode("utf-8")
    )
    expected_json_from_text = len(
        json.dumps(text_part.to_dict(), ensure_ascii=False).encode("utf-8")
    )
    expected_json = expected_json_from_value + expected_json_from_text

    assert json_bytes == expected_json
    assert binary_bytes == 512
    assert total_bytes == expected_json + 512


# ---------------------------------------------------------------------------
# prepare_envelope_storage
# ---------------------------------------------------------------------------
def test_prepare_envelope_storage_full_jsonb_mode(tmp_path: Path) -> None:
    config = _config(tmp_path, envelope_jsonb_mode=EnvelopeJsonbMode.full)
    envelope = _ok_envelope()

    p_hash, uncompressed, compressed, jsonb = prepare_envelope_storage(
        envelope, config
    )

    assert isinstance(p_hash, str) and len(p_hash) == 64
    assert isinstance(uncompressed, bytes)
    assert jsonb is not None
    assert jsonb["type"] == "mcp_envelope"
    assert jsonb["content"][0]["type"] == "json"


def test_prepare_envelope_storage_minimal_for_large_small(
    tmp_path: Path,
) -> None:
    """Small envelope => full JSONB under minimal_for_large mode."""
    config = _config(
        tmp_path,
        envelope_jsonb_mode=EnvelopeJsonbMode.minimal_for_large,
        envelope_jsonb_minimize_threshold_bytes=10_000_000,
    )
    envelope = _ok_envelope()

    _, _, _, jsonb = prepare_envelope_storage(envelope, config)

    assert jsonb is not None
    # Full payload includes content array
    assert "content" in jsonb


def test_prepare_envelope_storage_minimal_for_large_large(
    tmp_path: Path,
) -> None:
    """Large envelope => minimal JSONB under minimal_for_large mode."""
    big_value = {"data": "x" * 500}
    config = _config(
        tmp_path,
        envelope_jsonb_mode=EnvelopeJsonbMode.minimal_for_large,
        envelope_jsonb_minimize_threshold_bytes=20,
    )
    envelope = _ok_envelope(content=[JsonContentPart(value=big_value)])

    _, _, _, jsonb = prepare_envelope_storage(envelope, config)

    assert jsonb is not None
    assert "content_summary" in jsonb
    assert jsonb["content_summary"]["part_count"] == 1


def test_prepare_envelope_storage_none_jsonb_mode(tmp_path: Path) -> None:
    config = _config(tmp_path, envelope_jsonb_mode=EnvelopeJsonbMode.none)
    envelope = _ok_envelope()

    _, _, _, jsonb = prepare_envelope_storage(envelope, config)

    assert jsonb is None


def test_prepare_envelope_storage_hash_integrity(tmp_path: Path) -> None:
    """Hash of uncompressed canonical bytes should match returned hash."""
    config = _config(tmp_path)
    envelope = _ok_envelope()

    p_hash, uncompressed, _, _ = prepare_envelope_storage(envelope, config)

    assert p_hash == sha256_hex(uncompressed)
    # Also verify uncompressed is deterministic canonical bytes of envelope dict
    assert uncompressed == canonical_bytes(envelope.to_dict())


# ---------------------------------------------------------------------------
# build_artifact_row
# ---------------------------------------------------------------------------
def test_build_artifact_row_contains_required_fields(tmp_path: Path) -> None:
    envelope = _ok_envelope()
    input_data = _sample_input(envelope)
    row = build_artifact_row(
        artifact_id="art_test123",
        input_data=input_data,
        payload_hash="hash_abc",
        payload_json_bytes=100,
        payload_binary_bytes_total=0,
        payload_total_bytes=100,
    )

    expected_keys = {
        "workspace_id",
        "artifact_id",
        "session_id",
        "source_tool",
        "upstream_instance_id",
        "upstream_tool_schema_hash",
        "request_key",
        "request_args_hash",
        "request_args_prefix",
        "payload_hash_full",
        "canonicalizer_version",
        "payload_json_bytes",
        "payload_binary_bytes_total",
        "payload_total_bytes",
        "last_referenced_at",
        "generation",
        "parent_artifact_id",
        "chain_seq",
        "map_kind",
        "map_status",
        "mapper_version",
        "index_status",
        "error_summary",
    }
    assert set(row.keys()) == expected_keys
    assert row["workspace_id"] == WORKSPACE_ID
    assert row["artifact_id"] == "art_test123"
    assert row["session_id"] == "sess_1"
    assert row["source_tool"] == "github.search_issues"
    assert row["canonicalizer_version"] == CANONICALIZER_VERSION
    assert row["mapper_version"] == MAPPER_VERSION
    assert row["generation"] == 1


def test_build_artifact_row_map_kind_and_status() -> None:
    row = build_artifact_row(
        artifact_id="art_x",
        input_data=_sample_input(),
        payload_hash="h",
        payload_json_bytes=0,
        payload_binary_bytes_total=0,
        payload_total_bytes=0,
    )
    assert row["map_kind"] == "none"
    assert row["map_status"] == "pending"


def test_build_artifact_row_error_summary_for_error_envelope() -> None:
    envelope = _error_envelope()
    input_data = _sample_input(envelope)
    row = build_artifact_row(
        artifact_id="art_err",
        input_data=input_data,
        payload_hash="h",
        payload_json_bytes=0,
        payload_binary_bytes_total=0,
        payload_total_bytes=0,
    )
    assert row["error_summary"] == "UPSTREAM_ERROR: something broke"


def test_build_artifact_row_no_error_summary_for_ok_envelope() -> None:
    envelope = _ok_envelope()
    input_data = _sample_input(envelope)
    row = build_artifact_row(
        artifact_id="art_ok",
        input_data=input_data,
        payload_hash="h",
        payload_json_bytes=0,
        payload_binary_bytes_total=0,
        payload_total_bytes=0,
    )
    assert row["error_summary"] is None


# ---------------------------------------------------------------------------
# SQL constants smoke check
# ---------------------------------------------------------------------------
def test_insert_artifact_sql_has_returning_clause() -> None:
    assert "RETURNING created_seq" in INSERT_ARTIFACT_SQL


def test_upsert_artifact_ref_sql_has_on_conflict() -> None:
    assert "ON CONFLICT" in UPSERT_ARTIFACT_REF_SQL


def test_insert_payload_binary_ref_sql_has_on_conflict() -> None:
    assert "ON CONFLICT" in INSERT_PAYLOAD_BINARY_REF_SQL


class _PersistCursor:
    def __init__(self, row: tuple[object, ...] | None = None) -> None:
        self._row = row

    def fetchone(self) -> tuple[object, ...] | None:
        return self._row


class _PersistConnection:
    def __init__(self, *, fail_on_call: int | None = None) -> None:
        self.calls: list[tuple[str, tuple[object, ...] | None]] = []
        self.fail_on_call = fail_on_call
        self.committed = False
        self.rolled_back = False

    def execute(
        self,
        query: str,
        params: tuple[object, ...] | None = None,
    ) -> _PersistCursor:
        self.calls.append((query, params))
        if (
            self.fail_on_call is not None
            and len(self.calls) == self.fail_on_call
        ):
            raise RuntimeError("simulated execute failure")
        if "RETURNING created_seq" in query:
            return _PersistCursor((42,))
        return _PersistCursor()

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        self.rolled_back = True


def test_persist_artifact_executes_core_writes(tmp_path: Path) -> None:
    conn = _PersistConnection()
    config = _config(tmp_path)

    handle = persist_artifact(
        connection=conn,
        config=config,
        input_data=_sample_input(),
    )

    assert handle.artifact_id.startswith(ARTIFACT_ID_PREFIX)
    assert handle.created_seq == 42
    assert handle.status == "ok"
    assert conn.committed is True
    assert len(conn.calls) == 4  # payload, session, artifact, artifact_refs


def test_persist_artifact_rolls_back_on_error(tmp_path: Path) -> None:
    conn = _PersistConnection(fail_on_call=3)
    config = _config(tmp_path)

    with pytest.raises(RuntimeError, match="simulated execute failure"):
        persist_artifact(
            connection=conn,
            config=config,
            input_data=_sample_input(),
        )

    assert conn.committed is False
    assert conn.rolled_back is True


# ---------------------------------------------------------------------------
# Metrics wiring tests
# ---------------------------------------------------------------------------
def test_persist_artifact_increments_binary_blob_writes(tmp_path: Path) -> None:
    """persist_artifact increments binary_blob_writes for each binary hash."""
    conn = _PersistConnection()
    config = _config(tmp_path)
    metrics = GatewayMetrics()

    persist_artifact(
        connection=conn,
        config=config,
        input_data=_sample_input(),
        binary_hashes=["hash_a", "hash_b"],
        metrics=metrics,
    )

    assert counter_value(metrics.binary_blob_writes) == 2
