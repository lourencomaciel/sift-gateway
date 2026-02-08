"""Cursor payload construction and binding verification per Section 14.2.

Provides :func:`build_cursor_payload` to create the full payload dictionary
before signing, and :func:`verify_cursor_bindings` to check that a decoded
cursor's binding fields still match the current server state.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from mcp_artifact_gateway.constants import (
    CURSOR_VERSION,
    TRAVERSAL_CONTRACT_VERSION,
    WORKSPACE_ID,
)
from mcp_artifact_gateway.cursor.hmac import CursorStaleError


# ---------------------------------------------------------------------------
# Payload construction
# ---------------------------------------------------------------------------

def build_cursor_payload(
    *,
    tool: str,
    binding: dict,
    position_state: dict,
    artifact_id: str,
    artifact_generation: int,
    map_kind: str,
    mapper_version: str,
    cursor_secret_version: str,
    cursor_ttl_minutes: int,
    where_canonicalization_mode: str,
    map_budget_fingerprint: str | None = None,
    sample_set_hash: str | None = None,
) -> dict:
    """Construct the full cursor payload dictionary.

    All timestamps are UTC ISO 8601 strings.

    Parameters:
        tool: The tool name (artifact.get, artifact.select,
            artifact.find, artifact.search).
        binding: Tool-specific binding dict (e.g. target + normalized_jsonpath
            for get; root_path + select_paths_hash + where_hash for select).
        position_state: Opaque position state for cursor resumption.
        artifact_id: The artifact identifier.
        artifact_generation: Current artifact generation number.
        map_kind: Mapping kind (e.g. "full", "partial").
        mapper_version: Version string for the mapper.
        cursor_secret_version: Version string of the signing secret.
        cursor_ttl_minutes: Cursor time-to-live in minutes.
        where_canonicalization_mode: How WHERE clauses are canonicalized.
        map_budget_fingerprint: Required for partial map_kind.
        sample_set_hash: Required for partial map_kind.

    Returns:
        The cursor payload dictionary ready for signing.
    """
    now = datetime.now(timezone.utc)
    expires_at = now + timedelta(minutes=cursor_ttl_minutes)

    payload: dict = {
        "cursor_version": CURSOR_VERSION,
        "cursor_secret_version": cursor_secret_version,
        "traversal_contract_version": TRAVERSAL_CONTRACT_VERSION,
        "workspace_id": WORKSPACE_ID,
        "artifact_id": artifact_id,
        "tool": tool,
        "binding": binding,
        "where_canonicalization_mode": where_canonicalization_mode,
        "mapper_version": mapper_version,
        "artifact_generation": artifact_generation,
        "map_kind": map_kind,
        "position_state": position_state,
        "issued_at": now.isoformat(),
        "expires_at": expires_at.isoformat(),
    }

    if map_budget_fingerprint is not None:
        payload["map_budget_fingerprint"] = map_budget_fingerprint

    if sample_set_hash is not None:
        payload["sample_set_hash"] = sample_set_hash

    return payload


# ---------------------------------------------------------------------------
# Binding verification
# ---------------------------------------------------------------------------

def verify_cursor_bindings(
    payload: dict,
    *,
    artifact_generation: int,
    map_kind: str,
    workspace_id: str | None = None,
    tool: str | None = None,
    artifact_id: str | None = None,
    binding: dict | None = None,
    mapper_version: str | None = None,
    where_canonicalization_mode: str,
    traversal_contract_version: str,
    map_budget_fingerprint: str | None = None,
    sample_set_hash: str | None = None,
) -> None:
    """Verify that a decoded cursor's binding fields match current server state.

    Any mismatch raises :class:`CursorStaleError` with a descriptive message.

    Parameters:
        payload: The decoded cursor payload dictionary.
        artifact_generation: The current artifact generation on the server.
        map_kind: The current map kind.
        workspace_id: Expected workspace id. Defaults to ``WORKSPACE_ID``.
        tool: Expected tool name. If provided, must match payload.
        artifact_id: Expected artifact id. If provided, must match payload.
        binding: Expected binding dict. If provided, must match payload.
        mapper_version: Expected mapper version. If provided, must match payload.
        where_canonicalization_mode: The current WHERE canonicalization mode.
        traversal_contract_version: The current traversal contract version.
        map_budget_fingerprint: The current map budget fingerprint (for partial).
        sample_set_hash: The current sample set hash (for partial).

    Raises:
        CursorStaleError: If any binding field does not match.
    """
    # cursor_version mismatch
    cursor_version_in_payload = payload.get("cursor_version")
    if cursor_version_in_payload != CURSOR_VERSION:
        raise CursorStaleError(
            f"cursor_version mismatch: cursor has {cursor_version_in_payload!r}, "
            f"server expects {CURSOR_VERSION!r}"
        )

    # traversal_contract_version mismatch
    tcv_in_payload = payload.get("traversal_contract_version")
    if tcv_in_payload != traversal_contract_version:
        raise CursorStaleError(
            f"traversal_contract_version mismatch: cursor has {tcv_in_payload!r}, "
            f"server expects {traversal_contract_version!r}"
        )

    # where_canonicalization_mode mismatch
    wcm_in_payload = payload.get("where_canonicalization_mode")
    if wcm_in_payload != where_canonicalization_mode:
        raise CursorStaleError(
            f"where_canonicalization_mode mismatch: cursor has {wcm_in_payload!r}, "
            f"server expects {where_canonicalization_mode!r}"
        )

    # workspace_id mismatch
    expected_workspace_id = WORKSPACE_ID if workspace_id is None else workspace_id
    workspace_in_payload = payload.get("workspace_id")
    if workspace_in_payload != expected_workspace_id:
        raise CursorStaleError(
            f"workspace_id mismatch: cursor has {workspace_in_payload!r}, "
            f"server expects {expected_workspace_id!r}"
        )

    # tool mismatch
    if tool is not None:
        tool_in_payload = payload.get("tool")
        if tool_in_payload != tool:
            raise CursorStaleError(
                f"tool mismatch: cursor has {tool_in_payload!r}, "
                f"server expects {tool!r}"
            )

    # artifact_id mismatch
    if artifact_id is not None:
        artifact_id_in_payload = payload.get("artifact_id")
        if artifact_id_in_payload != artifact_id:
            raise CursorStaleError(
                f"artifact_id mismatch: cursor has {artifact_id_in_payload!r}, "
                f"server expects {artifact_id!r}"
            )

    # binding mismatch
    if binding is not None:
        binding_in_payload = payload.get("binding")
        if binding_in_payload != binding:
            raise CursorStaleError(
                "binding mismatch: cursor binding does not match current request"
            )

    # mapper_version mismatch
    if mapper_version is not None:
        mapper_in_payload = payload.get("mapper_version")
        if mapper_in_payload != mapper_version:
            raise CursorStaleError(
                f"mapper_version mismatch: cursor has {mapper_in_payload!r}, "
                f"server expects {mapper_version!r}"
            )

    # artifact_generation mismatch
    gen_in_payload = payload.get("artifact_generation")
    if gen_in_payload != artifact_generation:
        raise CursorStaleError(
            f"artifact_generation mismatch: cursor has {gen_in_payload!r}, "
            f"server expects {artifact_generation!r}"
        )

    # map_kind mismatch
    payload_map_kind = payload.get("map_kind")
    if payload_map_kind != map_kind:
        raise CursorStaleError(
            f"map_kind mismatch: cursor has {payload_map_kind!r}, "
            f"server expects {map_kind!r}"
        )

    # map_budget_fingerprint mismatch (relevant for partial mode)
    mbf_in_payload = payload.get("map_budget_fingerprint")
    if mbf_in_payload != map_budget_fingerprint:
        raise CursorStaleError(
            f"map_budget_fingerprint mismatch: cursor has {mbf_in_payload!r}, "
            f"server expects {map_budget_fingerprint!r}"
        )

    # sample_set_hash mismatch (relevant for partial mode)
    ssh_in_payload = payload.get("sample_set_hash")
    if ssh_in_payload != sample_set_hash:
        raise CursorStaleError(
            f"sample_set_hash mismatch: cursor has {ssh_in_payload!r}, "
            f"server expects {sample_set_hash!r}"
        )
