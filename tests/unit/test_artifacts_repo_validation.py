from __future__ import annotations

import pytest

from sift_mcp.db.repos.artifacts_repo import validate_artifact_row


def _valid_row() -> dict[str, object]:
    return {
        "workspace_id": "local",
        "artifact_id": "art_1234",
        "kind": "data",
        "map_kind": "none",
        "map_status": "pending",
        "index_status": "off",
        "payload_json_bytes": 1,
        "payload_binary_bytes_total": 0,
        "payload_total_bytes": 1,
        "capture_kind": "mcp_tool",
        "capture_origin": {
            "prefix": "demo",
            "tool": "echo",
            "upstream_instance_id": "inst_demo",
        },
        "capture_key": "rk_1",
    }


def test_artifacts_repo_validation_accepts_valid_row() -> None:
    validate_artifact_row(_valid_row())


def test_artifacts_repo_validation_rejects_workspace() -> None:
    row = _valid_row()
    row["workspace_id"] = "other"
    with pytest.raises(ValueError, match="workspace_id"):
        validate_artifact_row(row)


def test_artifacts_repo_validation_rejects_negative_size() -> None:
    row = _valid_row()
    row["payload_total_bytes"] = -1
    with pytest.raises(ValueError, match="non-negative"):
        validate_artifact_row(row)


def test_artifacts_repo_validation_accepts_valid_kind() -> None:
    row = _valid_row()
    row["kind"] = "derived_codegen"
    row["parent_artifact_id"] = "art_parent"
    row["derivation"] = '{"query_kind":"code","artifact_ids":["art_parent"],"expression":{"code_hash":"sha256:x"}}'
    validate_artifact_row(row)


def test_artifacts_repo_validation_rejects_invalid_kind() -> None:
    row = _valid_row()
    row["kind"] = "invalid"
    with pytest.raises(ValueError, match="kind"):
        validate_artifact_row(row)


def test_artifacts_repo_validation_rejects_invalid_capture_kind() -> None:
    row = _valid_row()
    row["capture_kind"] = "invalid"
    with pytest.raises(ValueError, match="capture_kind"):
        validate_artifact_row(row)


def test_artifacts_repo_validation_rejects_data_with_derivation() -> None:
    row = _valid_row()
    row["derivation"] = '{"query_kind":"select"}'
    with pytest.raises(ValueError, match="must not set derivation"):
        validate_artifact_row(row)


def test_artifacts_repo_validation_rejects_derived_without_parent() -> None:
    row = _valid_row()
    row["kind"] = "derived_query"
    row["derivation"] = '{"query_kind":"select","artifact_ids":["art_p"]}'
    with pytest.raises(ValueError, match="parent_artifact_id"):
        validate_artifact_row(row)


def test_artifacts_repo_validation_rejects_derived_bad_derivation_json() -> None:
    row = _valid_row()
    row["kind"] = "derived_query"
    row["parent_artifact_id"] = "art_parent"
    row["derivation"] = "not-json"
    with pytest.raises(ValueError, match="valid JSON"):
        validate_artifact_row(row)
