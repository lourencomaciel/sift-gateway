from __future__ import annotations

from sift_mcp.db.repos.mapping_repo import update_map_status_params
from sift_mcp.db.repos.payloads_repo import payload_blob_params
from sift_mcp.db.repos.prune_repo import soft_delete_expired_params
from sift_mcp.db.repos.sessions_repo import upsert_session_params


def test_repo_param_helpers_include_workspace() -> None:
    assert upsert_session_params("sess-1") == ("local", "sess-1")
    assert soft_delete_expired_params() == ("local",)

    payload_params = payload_blob_params(
        payload_hash_full="h",
        envelope=None,
        encoding="none",
        canonical_bytes=b"{}",
        canonical_len=2,
        canonicalizer_version="v1",
        payload_json_bytes=2,
        payload_binary_bytes_total=0,
        payload_total_bytes=2,
        contains_binary_refs=False,
    )
    assert payload_params[0] == "local"
    assert payload_params[1] == "h"

    mapping_params = update_map_status_params(
        artifact_id="art_1",
        map_kind="none",
        map_status="pending",
        mapper_version="v1",
        map_budget_fingerprint=None,
        map_backend_id=None,
        prng_version=None,
        map_error=None,
    )
    assert mapping_params[-2] == "local"
    assert mapping_params[-1] == "art_1"
