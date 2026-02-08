from datetime import timezone

import pytest

from mcp_artifact_gateway.constants import (
    CURSOR_VERSION,
    TRAVERSAL_CONTRACT_VERSION,
    WORKSPACE_ID,
)
from mcp_artifact_gateway.cursor.hmac import CursorStaleError
from mcp_artifact_gateway.cursor.payload import build_cursor_payload, verify_cursor_bindings


def _payload() -> dict:
    return build_cursor_payload(
        tool="artifact.get",
        binding={"target": "envelope", "path": "$.a"},
        position_state={"offset": 0},
        artifact_id="art_123",
        artifact_generation=1,
        map_kind="full",
        mapper_version="mapper_v1",
        cursor_secret_version="v1",
        cursor_ttl_minutes=5,
        where_canonicalization_mode="raw_string",
    )


def test_payload_contains_required_fields() -> None:
    payload = _payload()
    assert payload["cursor_version"] == CURSOR_VERSION
    assert payload["traversal_contract_version"] == TRAVERSAL_CONTRACT_VERSION
    assert payload["workspace_id"] == WORKSPACE_ID
    assert payload["tool"] == "artifact.get"
    assert payload["artifact_id"] == "art_123"
    assert payload["binding"]["target"] == "envelope"
    assert payload["expires_at"].endswith("+00:00") or payload["expires_at"].endswith("Z")


def test_verify_bindings_success() -> None:
    payload = _payload()
    verify_cursor_bindings(
        payload,
        artifact_generation=1,
        map_kind="full",
        workspace_id=WORKSPACE_ID,
        tool="artifact.get",
        artifact_id="art_123",
        binding={"target": "envelope", "path": "$.a"},
        mapper_version="mapper_v1",
        where_canonicalization_mode="raw_string",
        traversal_contract_version=TRAVERSAL_CONTRACT_VERSION,
    )


def test_verify_bindings_mismatch() -> None:
    payload = _payload()
    with pytest.raises(CursorStaleError):
        verify_cursor_bindings(
            payload,
            artifact_generation=2,
            map_kind="full",
            where_canonicalization_mode="raw_string",
            traversal_contract_version=TRAVERSAL_CONTRACT_VERSION,
        )

    with pytest.raises(CursorStaleError):
        verify_cursor_bindings(
            payload,
            artifact_generation=1,
            map_kind="full",
            where_canonicalization_mode="canonical_ast",
            traversal_contract_version=TRAVERSAL_CONTRACT_VERSION,
        )

    with pytest.raises(CursorStaleError):
        verify_cursor_bindings(
            payload,
            artifact_generation=1,
            map_kind="full",
            workspace_id="other",
            where_canonicalization_mode="raw_string",
            traversal_contract_version=TRAVERSAL_CONTRACT_VERSION,
        )

    with pytest.raises(CursorStaleError):
        verify_cursor_bindings(
            payload,
            artifact_generation=1,
            map_kind="full",
            tool="artifact.select",
            where_canonicalization_mode="raw_string",
            traversal_contract_version=TRAVERSAL_CONTRACT_VERSION,
        )

    with pytest.raises(CursorStaleError):
        verify_cursor_bindings(
            payload,
            artifact_generation=1,
            map_kind="full",
            binding={"target": "envelope", "path": "$.b"},
            where_canonicalization_mode="raw_string",
            traversal_contract_version=TRAVERSAL_CONTRACT_VERSION,
        )

    with pytest.raises(CursorStaleError):
        verify_cursor_bindings(
            payload,
            artifact_generation=1,
            map_kind="full",
            mapper_version="mapper_v2",
            where_canonicalization_mode="raw_string",
            traversal_contract_version=TRAVERSAL_CONTRACT_VERSION,
        )
