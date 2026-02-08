"""Artifact creation pipeline for mirrored tool calls."""
from __future__ import annotations

import datetime as dt
import json
import secrets
from dataclasses import dataclass
from typing import Any, Protocol

from mcp_artifact_gateway.canon.compress import CompressedBytes, compress_bytes
from mcp_artifact_gateway.canon.rfc8785 import canonical_bytes
from mcp_artifact_gateway.config.settings import GatewayConfig
from mcp_artifact_gateway.constants import (
    ARTIFACT_ID_PREFIX,
    CANONICALIZER_VERSION,
    MAPPER_VERSION,
    WORKSPACE_ID,
)
from mcp_artifact_gateway.db.repos.artifacts_repo import validate_artifact_row
from mcp_artifact_gateway.db.repos.payloads_repo import (
    INSERT_PAYLOAD_BLOB_SQL,
    payload_blob_params,
)
from mcp_artifact_gateway.db.repos.sessions_repo import UPSERT_SESSION_SQL, upsert_session_params
from mcp_artifact_gateway.envelope.jsonb import envelope_to_jsonb
from mcp_artifact_gateway.envelope.model import (
    BinaryRefContentPart,
    Envelope,
    JsonContentPart,
)
from mcp_artifact_gateway.obs.logging import LogEvents, get_logger
from mcp_artifact_gateway.util.hashing import sha256_hex


# ---------------------------------------------------------------------------
# DTOs
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class ArtifactHandle:
    """Handle returned to the caller after artifact creation."""

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


@dataclass(frozen=True)
class CreateArtifactInput:
    """Input for creating an artifact."""

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
    cache_mode: str = "allow"  # "allow" | "fresh"


# ---------------------------------------------------------------------------
# ID generation
# ---------------------------------------------------------------------------
def generate_artifact_id() -> str:
    """Generate a unique artifact ID."""
    return f"{ARTIFACT_ID_PREFIX}{secrets.token_hex(16)}"


# ---------------------------------------------------------------------------
# Payload sizing
# ---------------------------------------------------------------------------
def compute_payload_sizes(envelope: Envelope) -> tuple[int, int, int]:
    """Compute (payload_json_bytes, payload_binary_bytes_total, payload_total_bytes).

    - payload_json_bytes: sum of JSON content part sizes
    - payload_binary_bytes_total: sum of binary_ref byte_counts
    - payload_total_bytes: json + binary
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
    """Prepare envelope for storage.

    Returns: (payload_hash, uncompressed_canonical, compressed, jsonb_or_none)
    """
    envelope_dict = envelope.to_dict()
    uncompressed = canonical_bytes(envelope_dict)
    p_hash = sha256_hex(uncompressed)
    compressed = compress_bytes(uncompressed, config.envelope_canonical_encoding.value)

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
    """Build the artifact row dict for DB insertion."""
    error_summary = None
    if input_data.envelope.status == "error" and input_data.envelope.error is not None:
        error_summary = (
            f"{input_data.envelope.error.code}: {input_data.envelope.error.message}"
        )

    return {
        "workspace_id": WORKSPACE_ID,
        "artifact_id": artifact_id,
        "session_id": input_data.session_id,
        "source_tool": f"{input_data.prefix}.{input_data.tool_name}",
        "upstream_instance_id": input_data.upstream_instance_id,
        "upstream_tool_schema_hash": input_data.upstream_tool_schema_hash,
        "request_key": input_data.request_key,
        "request_args_hash": input_data.request_args_hash,
        "request_args_prefix": input_data.request_args_prefix,
        "payload_hash_full": payload_hash,
        "canonicalizer_version": CANONICALIZER_VERSION,
        "payload_json_bytes": payload_json_bytes,
        "payload_binary_bytes_total": payload_binary_bytes_total,
        "payload_total_bytes": payload_total_bytes,
        "last_referenced_at": dt.datetime.now(dt.timezone.utc),
        "generation": 1,
        "parent_artifact_id": input_data.parent_artifact_id,
        "chain_seq": input_data.chain_seq,
        "map_kind": "none",
        "map_status": "pending",
        "mapper_version": MAPPER_VERSION,
        "index_status": "off",
        "error_summary": error_summary,
    }


# ---------------------------------------------------------------------------
# Inline decision
# ---------------------------------------------------------------------------
def should_inline_envelope(
    *,
    payload_json_bytes: int,
    payload_total_bytes: int,
    contains_binary_refs: bool,
    config: GatewayConfig,
    inline_allowed: bool = True,
) -> bool:
    """Check if envelope should be returned inline with the handle.

    Inline only when:
    - policy allows
    - payload_json_bytes <= inline_envelope_max_json_bytes
    - payload_total_bytes <= inline_envelope_max_total_bytes
    - no binary refs
    """
    if not inline_allowed:
        return False
    if contains_binary_refs:
        return False
    if payload_json_bytes > config.inline_envelope_max_json_bytes:
        return False
    if payload_total_bytes > config.inline_envelope_max_total_bytes:
        return False
    return True


# ---------------------------------------------------------------------------
# SQL helpers
# ---------------------------------------------------------------------------
INSERT_ARTIFACT_SQL = """
INSERT INTO artifacts (
    workspace_id, artifact_id, session_id, source_tool,
    upstream_instance_id, upstream_tool_schema_hash,
    request_key, request_args_hash, request_args_prefix,
    payload_hash_full, canonicalizer_version,
    payload_json_bytes, payload_binary_bytes_total, payload_total_bytes,
    last_referenced_at, generation,
    parent_artifact_id, chain_seq,
    map_kind, map_status, mapper_version, index_status, error_summary
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
)
RETURNING created_seq
"""

UPSERT_ARTIFACT_REF_SQL = """
INSERT INTO artifact_refs (workspace_id, session_id, artifact_id, first_seen_at, last_seen_at)
VALUES (%s, %s, %s, NOW(), NOW())
ON CONFLICT (workspace_id, session_id, artifact_id)
DO UPDATE SET last_seen_at = EXCLUDED.last_seen_at
"""

INSERT_PAYLOAD_BINARY_REF_SQL = """
INSERT INTO payload_binary_refs (workspace_id, payload_hash_full, binary_hash)
VALUES (%s, %s, %s)
ON CONFLICT (workspace_id, payload_hash_full, binary_hash) DO NOTHING
"""


def _increment_metric(metrics: Any | None, attr: str, amount: int = 1) -> None:
    if metrics is None:
        return
    counter = getattr(metrics, attr, None)
    increment = getattr(counter, "increment", None)
    if callable(increment):
        increment(amount)


class CursorLike(Protocol):
    def fetchone(self) -> tuple[object, ...] | None: ...


class ArtifactConnectionLike(Protocol):
    def execute(self, query: str, params: tuple[object, ...] | None = None) -> CursorLike: ...
    def commit(self) -> None: ...


def _artifact_insert_params(row: dict[str, Any]) -> tuple[object, ...]:
    return (
        row["workspace_id"],
        row["artifact_id"],
        row["session_id"],
        row["source_tool"],
        row["upstream_instance_id"],
        row["upstream_tool_schema_hash"],
        row["request_key"],
        row["request_args_hash"],
        row["request_args_prefix"],
        row["payload_hash_full"],
        row["canonicalizer_version"],
        row["payload_json_bytes"],
        row["payload_binary_bytes_total"],
        row["payload_total_bytes"],
        row["last_referenced_at"],
        row["generation"],
        row["parent_artifact_id"],
        row["chain_seq"],
        row["map_kind"],
        row["map_status"],
        row["mapper_version"],
        row["index_status"],
        row["error_summary"],
    )


def _created_seq_from_row(row: tuple[object, ...] | None) -> int | None:
    if row is None:
        return None
    raw = row[0]
    if isinstance(raw, int):
        return raw
    if isinstance(raw, str) and raw.isdigit():
        return int(raw)
    return None


def persist_artifact(
    *,
    connection: ArtifactConnectionLike,
    config: GatewayConfig,
    input_data: CreateArtifactInput,
    binary_hashes: list[str] | None = None,
    metrics: Any | None = None,
    logger: Any | None = None,
) -> ArtifactHandle:
    """Persist payload + artifact rows and return a stable artifact handle."""
    log = logger or get_logger(component="artifacts.create")

    payload_hash, _canonical_raw, compressed, jsonb_value = prepare_envelope_storage(
        input_data.envelope,
        config,
    )
    payload_json_bytes, payload_binary_bytes_total, payload_total_bytes = compute_payload_sizes(
        input_data.envelope
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

    # Check for oversize JSON offload
    is_oversize = payload_json_bytes > config.inline_envelope_max_json_bytes
    if is_oversize:
        _increment_metric(metrics, "oversize_json_count")
        log.info(
            LogEvents.ARTIFACT_OVERSIZE_JSON,
            artifact_id=artifact_id,
            payload_json_bytes=payload_json_bytes,
            payload_hash_full=payload_hash,
        )

    try:
        connection.execute(
            INSERT_PAYLOAD_BLOB_SQL,
            payload_blob_params(
                payload_hash_full=payload_hash,
                envelope=jsonb_value,
                encoding=compressed.encoding,
                canonical_bytes=compressed.data,
                canonical_len=compressed.uncompressed_len,
                canonicalizer_version=CANONICALIZER_VERSION,
                payload_json_bytes=payload_json_bytes,
                payload_binary_bytes_total=payload_binary_bytes_total,
                payload_total_bytes=payload_total_bytes,
                contains_binary_refs=input_data.envelope.contains_binary_refs,
            ),
        )
        connection.execute(UPSERT_SESSION_SQL, upsert_session_params(input_data.session_id))
        created_row = connection.execute(
            INSERT_ARTIFACT_SQL,
            _artifact_insert_params(row),
        ).fetchone()
        connection.execute(
            UPSERT_ARTIFACT_REF_SQL,
            (WORKSPACE_ID, input_data.session_id, artifact_id),
        )

        for binary_hash in binary_hashes or []:
            connection.execute(
                INSERT_PAYLOAD_BINARY_REF_SQL,
                (WORKSPACE_ID, payload_hash, binary_hash),
            )
            _increment_metric(metrics, "binary_blob_writes")
            log.info(
                LogEvents.ARTIFACT_BINARY_BLOB_WRITE,
                artifact_id=artifact_id,
                binary_hash=binary_hash,
                payload_hash_full=payload_hash,
            )

        connection.commit()
    except Exception:
        rollback = getattr(connection, "rollback", None)
        if callable(rollback):
            rollback()
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
    )
