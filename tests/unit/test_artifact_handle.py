"""Tests for ArtifactHandle and CreateArtifactInput dataclasses."""

from __future__ import annotations

import dataclasses

from mcp_artifact_gateway.artifacts.create import ArtifactHandle, CreateArtifactInput
from mcp_artifact_gateway.envelope.model import Envelope, JsonContentPart


# ---------------------------------------------------------------------------
# ArtifactHandle
# ---------------------------------------------------------------------------
def test_artifact_handle_is_frozen_dataclass() -> None:
    handle = ArtifactHandle(
        artifact_id="art_abc",
        created_seq=1,
        generation=1,
        session_id="sess_1",
        source_tool="github.search_issues",
        upstream_instance_id="up_1",
        request_key="rk_1",
        payload_hash_full="hash_1",
        payload_json_bytes=100,
        payload_binary_bytes_total=0,
        payload_total_bytes=100,
        contains_binary_refs=False,
        map_kind="none",
        map_status="pending",
        index_status="off",
        status="ok",
        error_summary=None,
    )
    assert dataclasses.is_dataclass(handle)
    assert handle.artifact_id == "art_abc"

    # Frozen: cannot mutate
    try:
        handle.artifact_id = "art_xyz"  # type: ignore[misc]
        raise AssertionError("expected FrozenInstanceError")  # pragma: no cover
    except dataclasses.FrozenInstanceError:
        pass


# ---------------------------------------------------------------------------
# CreateArtifactInput
# ---------------------------------------------------------------------------
def _sample_envelope() -> Envelope:
    return Envelope(
        upstream_instance_id="up_1",
        upstream_prefix="github",
        tool="search_issues",
        status="ok",
        content=[JsonContentPart(value={"k": "v"})],
        meta={"warnings": []},
    )


def test_create_artifact_input_is_frozen_dataclass() -> None:
    envelope = _sample_envelope()
    input_data = CreateArtifactInput(
        session_id="sess_1",
        upstream_instance_id="up_1",
        prefix="github",
        tool_name="search_issues",
        request_key="rk_1",
        request_args_hash="arghash_1",
        request_args_prefix="prefix_1",
        upstream_tool_schema_hash="schema_1",
        envelope=envelope,
    )
    assert dataclasses.is_dataclass(input_data)

    # Frozen: cannot mutate
    try:
        input_data.session_id = "sess_2"  # type: ignore[misc]
        raise AssertionError("expected FrozenInstanceError")  # pragma: no cover
    except dataclasses.FrozenInstanceError:
        pass


def test_create_artifact_input_defaults() -> None:
    envelope = _sample_envelope()
    input_data = CreateArtifactInput(
        session_id="sess_1",
        upstream_instance_id="up_1",
        prefix="github",
        tool_name="search_issues",
        request_key="rk_1",
        request_args_hash="arghash_1",
        request_args_prefix="prefix_1",
        upstream_tool_schema_hash=None,
        envelope=envelope,
    )
    assert input_data.cache_mode == "allow"
    assert input_data.parent_artifact_id is None
    assert input_data.chain_seq is None
