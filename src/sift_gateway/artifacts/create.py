"""Persist artifact envelopes from mirrored tool calls.

Provide the end-to-end pipeline for creating artifacts:
ID generation, payload sizing, envelope serialization,
and transactional DB writes.  Key exports are
``persist_artifact``, ``ArtifactHandle``, and
``CreateArtifactInput``.
"""

from __future__ import annotations

from dataclasses import dataclass
import datetime as dt
import json
from pathlib import Path
import secrets
from typing import Any

from sift_gateway.canon.compress import CompressedBytes, compress_bytes
from sift_gateway.canon.rfc8785 import canonical_bytes, coerce_floats
from sift_gateway.config.settings import GatewayConfig
from sift_gateway.constants import (
    ARTIFACT_ID_PREFIX,
    CANONICALIZER_VERSION,
    KIND_DATA,
    MAPPER_VERSION,
    WORKSPACE_ID,
)
from sift_gateway.core.capture_identity import build_capture_identity
from sift_gateway.db.protocols import (
    ConnectionLike,
    increment_metric,
    safe_rollback,
)
from sift_gateway.db.repos.artifacts_repo import validate_artifact_row
from sift_gateway.db.repos.lineage_repo import (
    INSERT_LINEAGE_EDGE_SQL,
    lineage_edge_params,
)
from sift_gateway.db.repos.payloads_repo import (
    INSERT_PAYLOAD_BLOB_SQL,
    payload_blob_params,
)
from sift_gateway.db.repos.sessions_repo import (
    UPSERT_SESSION_SQL,
    upsert_session_params,
)
from sift_gateway.envelope.jsonb import envelope_to_jsonb
from sift_gateway.envelope.model import (
    BinaryRefContentPart,
    Envelope,
    JsonContentPart,
)
from sift_gateway.fs.blob_store import BinaryRef, _atomic_write_bytes
from sift_gateway.obs.logging import LogEvents, get_logger
from sift_gateway.util.hashing import sha256_hex


# ---------------------------------------------------------------------------
# DTOs
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class ArtifactHandle:
    """Immutable handle returned after successful artifact creation.

    Carries the identifiers and size metadata the caller needs
    to reference the newly persisted artifact without a DB
    round-trip.

    Attributes:
        artifact_id: Unique artifact identifier with prefix.
        created_seq: Auto-increment sequence from DB, or None.
        generation: Optimistic-concurrency generation counter.
        session_id: Owning session identifier.
        source_tool: Fully qualified tool name (prefix.tool).
        upstream_instance_id: Identity of the upstream server.
        request_key: Content-addressed request fingerprint.
        capture_kind: Protocol-neutral capture source kind.
        capture_origin: Protocol-neutral capture provenance object.
        capture_key: Protocol-neutral capture identity key.
        payload_hash_full: SHA-256 hex of canonical payload.
        payload_json_bytes: Total bytes of JSON content parts.
        payload_binary_bytes_total: Total bytes of binary refs.
        payload_total_bytes: Sum of JSON and binary bytes.
        contains_binary_refs: True if envelope has binary refs.
        map_kind: Mapping kind applied (e.g. "none", "full").
        map_status: Current mapping status (e.g. "pending").
        index_status: Current index status (e.g. "off").
        status: Envelope status, "ok" or "error".
        error_summary: Human-readable error, or None if ok.
    """

    artifact_id: str
    created_seq: int | None  # assigned by DB
    generation: int
    session_id: str
    source_tool: str
    upstream_instance_id: str
    request_key: str
    payload_hash_full: str
    payload_json_bytes: int
    payload_binary_bytes_total: int
    payload_total_bytes: int
    contains_binary_refs: bool
    map_kind: str
    map_status: str
    index_status: str
    status: str  # "ok" | "error"
    error_summary: str | None
    kind: str = KIND_DATA
    capture_kind: str | None = None
    capture_origin: dict[str, Any] | None = None
    capture_key: str | None = None


@dataclass(frozen=True)
class CreateArtifactInput:
    """Immutable input bundle for the artifact creation pipeline.

    Groups every value needed to persist an artifact so that
    callers can build the input once and pass it through.

    Attributes:
        session_id: Client session that triggered the call.
        upstream_instance_id: Identity of the upstream server.
        prefix: Namespace prefix for the tool.
        tool_name: Bare upstream tool name (without prefix).
        request_key: Content-addressed request fingerprint.
        request_args_hash: Hash of the stripped request args.
        request_args_prefix: Truncated args for display.
        upstream_tool_schema_hash: Schema hash, or None.
        envelope: Normalized envelope with tool results.
        parent_artifact_id: Parent artifact for chained calls.
        chain_seq: Position in a chain sequence, or None.
        capture_kind: Optional explicit protocol-neutral capture
            kind override.
        capture_origin: Optional explicit protocol-neutral capture
            provenance object override.
        capture_key: Optional explicit protocol-neutral capture
            identity key override.
        expires_at: Optional absolute expiration timestamp.
    """

    session_id: str
    upstream_instance_id: str
    prefix: str
    tool_name: str
    request_key: str
    request_args_hash: str
    request_args_prefix: str
    upstream_tool_schema_hash: str | None
    envelope: Envelope
    parent_artifact_id: str | None = None
    chain_seq: int | None = None
    kind: str = KIND_DATA
    derivation: str | None = None
    capture_kind: str | None = None
    capture_origin: dict[str, Any] | None = None
    capture_key: str | None = None
    expires_at: dt.datetime | None = None


# ---------------------------------------------------------------------------
# ID generation
# ---------------------------------------------------------------------------
def generate_artifact_id() -> str:
    """Generate a unique artifact ID with the standard prefix.

    Returns:
        A string of the form ``art_<32 hex chars>``.
    """
    return f"{ARTIFACT_ID_PREFIX}{secrets.token_hex(16)}"


# ---------------------------------------------------------------------------
# Payload sizing
# ---------------------------------------------------------------------------
def compute_payload_sizes(
    envelope: Envelope,
) -> tuple[int, int, int]:
    """Compute byte sizes for all content parts in an envelope.

    Args:
        envelope: Normalized envelope whose parts are measured.

    Returns:
        A tuple of (payload_json_bytes,
        payload_binary_bytes_total, payload_total_bytes).
    """
    json_bytes = 0
    binary_bytes = 0

    for part in envelope.content:
        if isinstance(part, JsonContentPart):
            # Approximate JSON size from UTF-8 encoded value
            if part.value is not None:
                json_bytes += len(
                    json.dumps(part.value, ensure_ascii=False).encode("utf-8")
                )
        elif isinstance(part, BinaryRefContentPart):
            binary_bytes += part.byte_count
        else:
            # Text and resource ref contribute to json bytes
            part_dict = part.to_dict()
            json_bytes += len(
                json.dumps(part_dict, ensure_ascii=False).encode("utf-8")
            )

    return json_bytes, binary_bytes, json_bytes + binary_bytes


# ---------------------------------------------------------------------------
# Envelope storage prep
# ---------------------------------------------------------------------------
def prepare_envelope_storage(
    envelope: Envelope,
    config: GatewayConfig,
) -> tuple[str, bytes, CompressedBytes, dict[str, Any] | None]:
    """Canonicalize, hash, compress, and optionally JSONB-encode an envelope.

    Args:
        envelope: Normalized envelope to prepare.
        config: Gateway configuration controlling encoding
            and JSONB mode.

    Returns:
        A tuple of (payload_hash_hex, uncompressed_canonical,
        compressed_bytes, jsonb_value_or_none).
    """
    envelope_dict = coerce_floats(envelope.to_dict())
    uncompressed = canonical_bytes(envelope_dict)
    p_hash = sha256_hex(uncompressed)
    compressed = compress_bytes(
        uncompressed, config.envelope_canonical_encoding.value
    )

    # JSONB storage mode
    jsonb_value: dict[str, Any] | None = envelope_to_jsonb(
        envelope,
        mode=config.envelope_jsonb_mode.value,
        minimize_threshold_bytes=config.envelope_jsonb_minimize_threshold_bytes,
    )

    return p_hash, uncompressed, compressed, jsonb_value


# ---------------------------------------------------------------------------
# Artifact row builder
# ---------------------------------------------------------------------------
def build_artifact_row(
    *,
    artifact_id: str,
    input_data: CreateArtifactInput,
    payload_hash: str,
    payload_json_bytes: int,
    payload_binary_bytes_total: int,
    payload_total_bytes: int,
) -> dict[str, Any]:
    """Build the artifact row dict for DB insertion.

    Args:
        artifact_id: Unique ID for the new artifact.
        input_data: Creation input with tool and session info.
        payload_hash: SHA-256 hex of canonical payload.
        payload_json_bytes: Total bytes of JSON content.
        payload_binary_bytes_total: Total bytes of binary refs.
        payload_total_bytes: Sum of JSON and binary bytes.

    Returns:
        A dict whose keys match the ``artifacts`` table columns.
    """
    error_summary = None
    if (
        input_data.envelope.status == "error"
        and input_data.envelope.error is not None
    ):
        err = input_data.envelope.error
        error_summary = f"{err.code}: {err.message}"
    capture_identity = build_capture_identity(
        artifact_kind=input_data.kind,
        request_key=input_data.request_key,
        prefix=input_data.prefix,
        tool_name=input_data.tool_name,
        upstream_instance_id=input_data.upstream_instance_id,
        capture_kind=input_data.capture_kind,
        capture_origin=input_data.capture_origin,
        capture_key=input_data.capture_key,
    )

    return {
        "workspace_id": WORKSPACE_ID,
        "artifact_id": artifact_id,
        "session_id": input_data.session_id,
        "source_tool": f"{input_data.prefix}.{input_data.tool_name}",
        "upstream_instance_id": input_data.upstream_instance_id,
        "upstream_tool_schema_hash": input_data.upstream_tool_schema_hash,
        "request_key": input_data.request_key,
        "capture_kind": capture_identity.capture_kind,
        "capture_origin": capture_identity.capture_origin,
        "capture_key": capture_identity.capture_key,
        "request_args_hash": input_data.request_args_hash,
        "request_args_prefix": input_data.request_args_prefix,
        "payload_hash_full": payload_hash,
        "canonicalizer_version": CANONICALIZER_VERSION,
        "payload_json_bytes": payload_json_bytes,
        "payload_binary_bytes_total": payload_binary_bytes_total,
        "payload_total_bytes": payload_total_bytes,
        "expires_at": input_data.expires_at,
        "last_referenced_at": dt.datetime.now(dt.UTC),
        "generation": 1,
        "parent_artifact_id": input_data.parent_artifact_id,
        "chain_seq": input_data.chain_seq,
        "map_kind": "none",
        "map_status": "pending",
        "mapper_version": MAPPER_VERSION,
        "kind": input_data.kind,
        "derivation": input_data.derivation,
        "index_status": "off",
        "error_summary": error_summary,
    }


# ---------------------------------------------------------------------------
# SQL helpers
# ---------------------------------------------------------------------------
INSERT_ARTIFACT_SQL = """
INSERT INTO artifacts (
    workspace_id, artifact_id, session_id, source_tool,
    upstream_instance_id, upstream_tool_schema_hash,
    request_key, capture_kind, capture_origin, capture_key,
    request_args_hash, request_args_prefix,
    payload_hash_full, canonicalizer_version,
    payload_json_bytes, payload_binary_bytes_total, payload_total_bytes,
    expires_at,
    last_referenced_at, generation,
    parent_artifact_id, chain_seq,
    map_kind, map_status, mapper_version,
    kind, derivation,
    index_status, error_summary
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s, %s, %s, %s
)
RETURNING created_seq
"""

INSERT_PAYLOAD_BINARY_REF_SQL = """
INSERT INTO payload_binary_refs (workspace_id, payload_hash_full, binary_hash)
VALUES (%s, %s, %s)
ON CONFLICT (workspace_id, payload_hash_full, binary_hash) DO NOTHING
"""

INSERT_BINARY_BLOB_SQL = """
INSERT INTO binary_blobs (
    workspace_id, binary_hash, blob_id, byte_count,
    mime, fs_path, probe_head_hash, probe_tail_hash, probe_bytes
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (workspace_id, binary_hash) DO NOTHING
"""


def _artifact_insert_params(
    row: dict[str, Any],
) -> tuple[object, ...]:
    """Extract positional SQL parameters from an artifact row dict.

    Args:
        row: Artifact row dict built by ``build_artifact_row``.

    Returns:
        Ordered tuple matching INSERT_ARTIFACT_SQL placeholders.
    """
    return (
        row["workspace_id"],
        row["artifact_id"],
        row["session_id"],
        row["source_tool"],
        row["upstream_instance_id"],
        row["upstream_tool_schema_hash"],
        row["request_key"],
        row["capture_kind"],
        row["capture_origin"],
        row["capture_key"],
        row["request_args_hash"],
        row["request_args_prefix"],
        row["payload_hash_full"],
        row["canonicalizer_version"],
        row["payload_json_bytes"],
        row["payload_binary_bytes_total"],
        row["payload_total_bytes"],
        row["expires_at"],
        row["last_referenced_at"],
        row["generation"],
        row["parent_artifact_id"],
        row["chain_seq"],
        row["map_kind"],
        row["map_status"],
        row["mapper_version"],
        row["kind"],
        row["derivation"],
        row["index_status"],
        row["error_summary"],
    )


def _created_seq_from_row(
    row: tuple[object, ...] | None,
) -> int | None:
    """Extract the created_seq integer from a RETURNING row.

    Args:
        row: Single-column row from INSERT ... RETURNING, or
            None if the database returned no row.

    Returns:
        The integer sequence value, or None if unavailable.
    """
    if row is None:
        return None
    raw = row[0]
    if isinstance(raw, int):
        return raw
    if isinstance(raw, str) and raw.isdigit():
        return int(raw)
    return None


def _insert_primary_lineage_edge(
    *,
    connection: ConnectionLike,
    artifact_id: str,
    parent_artifact_id: Any,
) -> None:
    """Insert a parent->child lineage edge when parent is present."""
    if not isinstance(parent_artifact_id, str) or not parent_artifact_id:
        return
    connection.execute(
        INSERT_LINEAGE_EDGE_SQL,
        lineage_edge_params(
            child_artifact_id=artifact_id,
            parent_artifact_id=parent_artifact_id,
            ord=0,
        ),
    )


def persist_artifact(
    *,
    connection: ConnectionLike,
    config: GatewayConfig,
    input_data: CreateArtifactInput,
    binary_hashes: list[str] | None = None,
    binary_refs: list[BinaryRef] | None = None,
    metrics: Any | None = None,
    logger: Any | None = None,
) -> ArtifactHandle:
    """Persist payload and artifact rows in a single transaction.

    Canonicalize the envelope, insert payload blob and artifact
    rows, upsert session and artifact-ref rows, and link any
    binary blob hashes.  When ``binary_refs`` are provided,
    the corresponding ``binary_blobs`` rows are inserted first
    to satisfy the FK constraint on ``payload_binary_refs``.
    Rolls back on failure.

    Args:
        connection: Active database connection.
        config: Gateway configuration for encoding settings.
        input_data: Creation input with envelope and metadata.
        binary_hashes: Optional binary blob hashes to link to
            the payload via ``payload_binary_refs``.
        binary_refs: Optional ``BinaryRef`` objects to insert
            into the ``binary_blobs`` table before linking.
        metrics: Optional metrics collector for counters.
        logger: Optional structured logger override.

    Returns:
        An ArtifactHandle with all assigned identifiers and
        size metadata.

    Raises:
        ValueError: If the built artifact row fails validation.
    """
    log = logger or get_logger(component="artifacts.create")

    payload_hash, _canonical_raw, compressed, jsonb_value = (
        prepare_envelope_storage(
            input_data.envelope,
            config,
        )
    )
    payload_json_bytes, payload_binary_bytes_total, payload_total_bytes = (
        compute_payload_sizes(input_data.envelope)
    )
    artifact_id = generate_artifact_id()
    row = build_artifact_row(
        artifact_id=artifact_id,
        input_data=input_data,
        payload_hash=payload_hash,
        payload_json_bytes=payload_json_bytes,
        payload_binary_bytes_total=payload_binary_bytes_total,
        payload_total_bytes=payload_total_bytes,
    )
    validate_artifact_row(row)
    payload_rel_path = (
        Path(payload_hash[:2]) / payload_hash[2:4] / f"{payload_hash}.zst"
    )
    payload_abs_path = config.blobs_payload_dir / payload_rel_path
    if not payload_abs_path.exists():
        _atomic_write_bytes(payload_abs_path, compressed.data)

    try:
        connection.execute(
            INSERT_PAYLOAD_BLOB_SQL,
            payload_blob_params(
                payload_hash_full=payload_hash,
                envelope=jsonb_value,
                encoding=compressed.encoding,
                payload_fs_path=payload_rel_path.as_posix(),
                canonicalizer_version=CANONICALIZER_VERSION,
                payload_json_bytes=payload_json_bytes,
                payload_binary_bytes_total=payload_binary_bytes_total,
                payload_total_bytes=payload_total_bytes,
                contains_binary_refs=input_data.envelope.contains_binary_refs,
            ),
        )
        connection.execute(
            UPSERT_SESSION_SQL, upsert_session_params(input_data.session_id)
        )
        created_row = connection.execute(
            INSERT_ARTIFACT_SQL,
            _artifact_insert_params(row),
        ).fetchone()
        _insert_primary_lineage_edge(
            connection=connection,
            artifact_id=artifact_id,
            parent_artifact_id=row["parent_artifact_id"],
        )

        for ref in binary_refs or []:
            connection.execute(
                INSERT_BINARY_BLOB_SQL,
                (
                    WORKSPACE_ID,
                    ref.binary_hash,
                    ref.blob_id,
                    ref.byte_count,
                    ref.mime,
                    ref.fs_path,
                    ref.probe_head_hash,
                    ref.probe_tail_hash,
                    ref.probe_bytes,
                ),
            )

        for binary_hash in binary_hashes or []:
            connection.execute(
                INSERT_PAYLOAD_BINARY_REF_SQL,
                (WORKSPACE_ID, payload_hash, binary_hash),
            )
            increment_metric(metrics, "binary_blob_writes")
            log.info(
                LogEvents.ARTIFACT_BINARY_BLOB_WRITE,
                artifact_id=artifact_id,
                binary_hash=binary_hash,
                payload_hash_full=payload_hash,
            )

        connection.commit()
    except Exception:
        safe_rollback(connection)
        raise

    log.info(
        LogEvents.ARTIFACT_CREATED,
        artifact_id=artifact_id,
        session_id=input_data.session_id,
        request_key=input_data.request_key,
        payload_hash_full=payload_hash,
        payload_json_bytes=payload_json_bytes,
        payload_binary_bytes_total=payload_binary_bytes_total,
        payload_total_bytes=payload_total_bytes,
        source_tool=f"{input_data.prefix}.{input_data.tool_name}",
    )

    log.info(
        LogEvents.ARTIFACT_ENVELOPE_SIZES,
        artifact_id=artifact_id,
        payload_json_bytes=payload_json_bytes,
        payload_binary_bytes_total=payload_binary_bytes_total,
        payload_total_bytes=payload_total_bytes,
    )

    return ArtifactHandle(
        artifact_id=artifact_id,
        created_seq=_created_seq_from_row(created_row),
        generation=int(row["generation"]),
        session_id=input_data.session_id,
        source_tool=f"{input_data.prefix}.{input_data.tool_name}",
        upstream_instance_id=input_data.upstream_instance_id,
        request_key=input_data.request_key,
        capture_kind=str(row["capture_kind"]),
        capture_origin=(
            row["capture_origin"]
            if isinstance(row.get("capture_origin"), dict)
            else None
        ),
        capture_key=str(row["capture_key"]),
        payload_hash_full=payload_hash,
        payload_json_bytes=payload_json_bytes,
        payload_binary_bytes_total=payload_binary_bytes_total,
        payload_total_bytes=payload_total_bytes,
        contains_binary_refs=input_data.envelope.contains_binary_refs,
        map_kind=row["map_kind"],
        map_status=row["map_status"],
        index_status=row["index_status"],
        status=input_data.envelope.status,
        error_summary=row["error_summary"],
        kind=str(row["kind"]),
    )
