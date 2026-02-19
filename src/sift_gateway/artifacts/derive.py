"""Create derived artifacts from query/code handler outputs."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import json
from typing import Any

from sift_gateway.artifacts.create import (
    ArtifactHandle,
    CreateArtifactInput,
    persist_artifact,
)
from sift_gateway.canon.rfc8785 import canonical_bytes
from sift_gateway.config.settings import GatewayConfig
from sift_gateway.constants import WORKSPACE_ID
from sift_gateway.db.protocols import ConnectionLike
from sift_gateway.db.repos.lineage_repo import (
    INSERT_LINEAGE_EDGE_SQL,
    lineage_edge_params,
)
from sift_gateway.envelope.model import Envelope, JsonContentPart
from sift_gateway.util.hashing import sha256_hex

FETCH_PARENT_METADATA_SQL = """
SELECT session_id, upstream_instance_id, source_tool, upstream_tool_schema_hash
FROM artifacts
WHERE workspace_id = %s AND artifact_id = %s
"""


@dataclass(frozen=True)
class DerivedArtifactResult:
    """Return value for derived artifact creation."""

    handle: ArtifactHandle
    envelope: Envelope


def _normalize_parent_artifact_ids(parent_artifact_ids: list[str]) -> list[str]:
    """Deduplicate parent ids while preserving order."""
    normalized: list[str] = []
    seen: set[str] = set()
    for artifact_id in parent_artifact_ids:
        if not isinstance(artifact_id, str) or not artifact_id:
            continue
        if artifact_id in seen:
            continue
        seen.add(artifact_id)
        normalized.append(artifact_id)
    return normalized


def _normalize_derivation_expression(
    derivation_expression: Mapping[str, Any] | str,
) -> dict[str, Any] | list[Any] | str:
    """Normalize expression payload into JSON-typed structure when possible."""
    if isinstance(derivation_expression, Mapping):
        return dict(derivation_expression)
    stripped = derivation_expression.strip()
    if stripped and stripped[0] in "{[":
        try:
            parsed = json.loads(stripped)
        except (json.JSONDecodeError, ValueError):
            return derivation_expression
        if isinstance(parsed, (dict, list)):
            return parsed
    return derivation_expression


def create_derived_artifact(
    *,
    connection: ConnectionLike,
    config: GatewayConfig,
    parent_artifact_ids: list[str],
    result_data: dict[str, Any] | list[Any],
    derivation_expression: Mapping[str, Any] | str,
    kind: str,
    query_kind: str,
) -> DerivedArtifactResult:
    """Persist a derived artifact and return its handle/envelope."""
    normalized_parent_ids = _normalize_parent_artifact_ids(parent_artifact_ids)
    if not normalized_parent_ids:
        msg = "parent_artifact_ids must not be empty"
        raise ValueError(msg)

    parent_row = connection.execute(
        FETCH_PARENT_METADATA_SQL,
        (WORKSPACE_ID, normalized_parent_ids[0]),
    ).fetchone()
    if parent_row is None:
        msg = "parent artifact not found"
        raise ValueError(msg)

    session_id = str(parent_row[0])
    upstream_instance_id = str(parent_row[1])
    source_tool = str(parent_row[2])
    upstream_tool_schema_hash = parent_row[3]
    if "." in source_tool:
        prefix, tool_name = source_tool.split(".", 1)
    else:
        prefix = source_tool
        tool_name = source_tool

    expression_payload = _normalize_derivation_expression(derivation_expression)
    derivation_payload: dict[str, Any] = {
        "query_kind": query_kind,
        "artifact_ids": normalized_parent_ids,
        "expression": expression_payload,
    }
    derivation_json = json.dumps(
        derivation_payload, sort_keys=True, separators=(",", ":")
    )

    request_args_hash = sha256_hex(derivation_json.encode("utf-8"))
    if isinstance(expression_payload, str):
        request_args_prefix = expression_payload[:200]
    else:
        request_args_prefix = json.dumps(
            expression_payload, sort_keys=True, separators=(",", ":")
        )[:200]
    request_key = sha256_hex(
        canonical_bytes(
            {
                "artifact_ids": sorted(normalized_parent_ids),
                "kind": kind,
                "expression": expression_payload,
                "query_kind": query_kind,
            }
        )
    )

    envelope = Envelope(
        upstream_instance_id=upstream_instance_id,
        upstream_prefix=prefix,
        tool=tool_name,
        status="ok",
        content=[JsonContentPart(value=result_data)],
        meta={},
    )
    input_data = CreateArtifactInput(
        session_id=session_id,
        upstream_instance_id=upstream_instance_id,
        prefix=prefix,
        tool_name=tool_name,
        request_key=request_key,
        request_args_hash=request_args_hash,
        request_args_prefix=request_args_prefix,
        upstream_tool_schema_hash=(
            str(upstream_tool_schema_hash)
            if isinstance(upstream_tool_schema_hash, str)
            else None
        ),
        envelope=envelope,
        parent_artifact_id=normalized_parent_ids[0],
        chain_seq=None,
        kind=kind,
        derivation=derivation_json,
    )
    handle = persist_artifact(
        connection=connection,
        config=config,
        input_data=input_data,
    )
    for ord_value, parent_artifact_id in enumerate(normalized_parent_ids):
        connection.execute(
            INSERT_LINEAGE_EDGE_SQL,
            lineage_edge_params(
                child_artifact_id=handle.artifact_id,
                parent_artifact_id=parent_artifact_id,
                ord=ord_value,
            ),
        )
    connection.commit()
    return DerivedArtifactResult(handle=handle, envelope=envelope)
