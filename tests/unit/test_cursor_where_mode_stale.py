import pytest

from mcp_artifact_gateway.constants import TRAVERSAL_CONTRACT_VERSION
from mcp_artifact_gateway.cursor.hmac import CursorStaleError
from mcp_artifact_gateway.cursor.payload import build_cursor_payload, verify_cursor_bindings


def test_cursor_where_mode_stale() -> None:
    payload = build_cursor_payload(
        tool="artifact.search",
        binding={"order_by": "created_seq_desc"},
        position_state={"cursor": None},
        artifact_id="art_1",
        artifact_generation=1,
        map_kind="full",
        mapper_version="mapper_v1",
        cursor_secret_version="v1",
        cursor_ttl_minutes=5,
        where_canonicalization_mode="raw_string",
    )

    with pytest.raises(CursorStaleError):
        verify_cursor_bindings(
            payload,
            artifact_generation=1,
            map_kind="full",
            where_canonicalization_mode="canonical_ast",
            traversal_contract_version=TRAVERSAL_CONTRACT_VERSION,
        )
