from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from sift_mcp.artifacts import derive as derive_mod
from sift_mcp.artifacts.create import ArtifactHandle
from sift_mcp.canon.rfc8785 import canonical_bytes
from sift_mcp.config.settings import GatewayConfig
from sift_mcp.constants import KIND_DERIVED_QUERY
from sift_mcp.util.hashing import sha256_hex


class _Cursor:
    def __init__(self, row: tuple[object, ...] | None) -> None:
        self._row = row

    def fetchone(self) -> tuple[object, ...] | None:
        return self._row


class _Connection:
    def __init__(self, parent_row: tuple[object, ...] | None) -> None:
        self.parent_row = parent_row
        self.calls: list[tuple[str, tuple[object, ...] | None]] = []
        self.committed = False

    def execute(
        self,
        query: str,
        params: tuple[object, ...] | None = None,
    ) -> _Cursor:
        self.calls.append((query, params))
        return _Cursor(self.parent_row)

    def commit(self) -> None:
        self.committed = True


def _handle(kind: str) -> ArtifactHandle:
    return ArtifactHandle(
        artifact_id="art_derived_1",
        created_seq=11,
        generation=1,
        session_id="sess_parent",
        source_tool="github.list_prs",
        upstream_instance_id="inst_parent",
        request_key="rk_1",
        payload_hash_full="hash_1",
        payload_json_bytes=10,
        payload_binary_bytes_total=0,
        payload_total_bytes=10,
        contains_binary_refs=False,
        map_kind="none",
        map_status="pending",
        index_status="off",
        status="ok",
        error_summary=None,
        kind=kind,
    )


def test_create_derived_artifact_sources_parent_metadata_and_recipe(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = GatewayConfig(data_dir=tmp_path)
    connection = _Connection(
        (
            "sess_parent",
            "inst_parent",
            "github.list_prs",
            "schema_hash_parent",
        )
    )
    captured: dict[str, Any] = {}

    def _fake_persist_artifact(
        *,
        connection: Any,
        config: GatewayConfig,
        input_data: Any,
        binary_hashes: list[str] | None = None,
        binary_refs: Any = None,
    ) -> ArtifactHandle:
        del connection, config, binary_hashes, binary_refs
        captured["input_data"] = input_data
        return _handle(input_data.kind)

    monkeypatch.setattr(derive_mod, "persist_artifact", _fake_persist_artifact)

    parent_artifact_ids = ["art_parent_b", "art_parent_a"]
    expression = "$.items[?(@.state=='open')]"
    result = derive_mod.create_derived_artifact(
        connection=connection,
        config=config,
        parent_artifact_ids=parent_artifact_ids,
        result_data=[{"id": 1}],
        derivation_expression=expression,
        kind=KIND_DERIVED_QUERY,
        query_kind="select",
    )

    assert result.handle.artifact_id == "art_derived_1"
    assert connection.calls[0][1] == ("local", "art_parent_b")

    input_data = captured["input_data"]
    assert input_data.session_id == "sess_parent"
    assert input_data.upstream_instance_id == "inst_parent"
    assert input_data.prefix == "github"
    assert input_data.tool_name == "list_prs"
    assert input_data.parent_artifact_id == "art_parent_b"
    assert input_data.kind == KIND_DERIVED_QUERY

    derivation = json.loads(input_data.derivation)
    assert derivation["expression"] == expression
    assert derivation["artifact_ids"] == parent_artifact_ids
    assert derivation["query_kind"] == "select"
    assert connection.committed is True

    expected_request_key = sha256_hex(
        canonical_bytes(
            {
                "artifact_ids": sorted(parent_artifact_ids),
                "kind": KIND_DERIVED_QUERY,
                "expression": expression,
                "query_kind": "select",
            }
        )
    )
    assert input_data.request_key == expected_request_key
    assert input_data.upstream_tool_schema_hash == "schema_hash_parent"
    assert result.envelope.content[0].value == [{"id": 1}]
    lineage_inserts = [
        query for query, _params in connection.calls if "artifact_lineage_edges" in query
    ]
    assert len(lineage_inserts) == 2


def test_create_derived_artifact_stores_structured_expression_payload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = GatewayConfig(data_dir=tmp_path)
    connection = _Connection(
        (
            "sess_parent",
            "inst_parent",
            "github.list_prs",
            "schema_hash_parent",
        )
    )
    captured: dict[str, Any] = {}

    def _fake_persist_artifact(
        *,
        connection: Any,
        config: GatewayConfig,
        input_data: Any,
        binary_hashes: list[str] | None = None,
        binary_refs: Any = None,
    ) -> ArtifactHandle:
        del connection, config, binary_hashes, binary_refs
        captured["input_data"] = input_data
        return _handle(input_data.kind)

    monkeypatch.setattr(derive_mod, "persist_artifact", _fake_persist_artifact)

    expression_payload = {
        "root_paths": {"art_a": "$.items"},
        "code_hash": "sha256:abc",
    }
    derive_mod.create_derived_artifact(
        connection=connection,
        config=config,
        parent_artifact_ids=["art_a", "art_b"],
        result_data=[{"count": 2}],
        derivation_expression=expression_payload,
        kind=KIND_DERIVED_QUERY,
        query_kind="code",
    )

    derivation = json.loads(captured["input_data"].derivation)
    assert derivation["expression"] == expression_payload
    assert derivation["query_kind"] == "code"


def test_create_derived_artifact_requires_parent_row(
    tmp_path: Path,
) -> None:
    config = GatewayConfig(data_dir=tmp_path)
    connection = _Connection(None)

    with pytest.raises(ValueError, match="parent artifact not found"):
        derive_mod.create_derived_artifact(
            connection=connection,
            config=config,
            parent_artifact_ids=["art_missing"],
            result_data={"count": 1},
            derivation_expression="count_only",
            kind=KIND_DERIVED_QUERY,
            query_kind="select",
        )
