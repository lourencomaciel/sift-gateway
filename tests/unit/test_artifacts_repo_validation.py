from __future__ import annotations

import pytest

from sift_mcp.db.repos.artifacts_repo import validate_artifact_row


def _valid_row() -> dict[str, object]:
    return {
        "workspace_id": "local",
        "artifact_id": "art_1234",
        "map_kind": "none",
        "map_status": "pending",
        "index_status": "off",
        "payload_json_bytes": 1,
        "payload_binary_bytes_total": 0,
        "payload_total_bytes": 1,
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
