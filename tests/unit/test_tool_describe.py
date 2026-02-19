"""Tests for artifact.describe tool implementation."""

from __future__ import annotations

from sift_gateway.tools.artifact_describe import (
    build_describe_response,
    validate_describe_args,
)

# ---- validate_describe_args ----


def test_validate_describe_args_requires_session_id() -> None:
    result = validate_describe_args({})
    assert result is not None
    assert result["code"] == "INVALID_ARGUMENT"
    assert "session_id" in result["message"]


def test_validate_describe_args_requires_artifact_id() -> None:
    result = validate_describe_args(
        {"_gateway_context": {"session_id": "sess_1"}}
    )
    assert result is not None
    assert result["code"] == "INVALID_ARGUMENT"
    assert "artifact_id" in result["message"]


def test_validate_describe_args_accepts_valid_arguments() -> None:
    result = validate_describe_args(
        {
            "_gateway_context": {"session_id": "sess_1"},
            "artifact_id": "art_1",
        }
    )
    assert result is None


# ---- build_describe_response ----


def test_build_describe_response_includes_mapping_info() -> None:
    artifact_row = {
        "artifact_id": "art_1",
        "map_kind": "full",
        "map_status": "ready",
        "mapper_version": "mapper_v1",
        "map_budget_fingerprint": "fp_abc",
        "map_backend_id": "backend_1",
        "prng_version": "prng_xoshiro256ss_v1",
    }
    result = build_describe_response(artifact_row, [])

    assert result["artifact_id"] == "art_1"
    mapping = result["mapping"]
    assert mapping["map_kind"] == "full"
    assert mapping["map_status"] == "ready"
    assert mapping["mapper_version"] == "mapper_v1"
    assert mapping["map_budget_fingerprint"] == "fp_abc"
    assert mapping["map_backend_id"] == "backend_1"
    assert mapping["prng_version"] == "prng_xoshiro256ss_v1"


def test_build_describe_response_includes_roots() -> None:
    artifact_row = {
        "artifact_id": "art_1",
        "map_kind": "full",
        "map_status": "ready",
    }
    roots = [
        {
            "root_key": "rk_1",
            "root_path": "$.data",
            "root_shape": "array",
            "count_estimate": 42,
            "fields_top": ["id", "name"],
        },
    ]
    result = build_describe_response(artifact_row, roots)

    assert len(result["roots"]) == 1
    root = result["roots"][0]
    assert root["root_key"] == "rk_1"
    assert root["root_path"] == "$.data"
    assert root["root_shape"] == "array"
    assert root["count_estimate"] == 42
    assert root["fields_top"] == ["id", "name"]


def test_build_describe_response_marks_sampled_roots() -> None:
    artifact_row = {
        "artifact_id": "art_1",
        "map_kind": "partial",
        "map_status": "ready",
    }
    roots = [
        {
            "root_key": "rk_1",
            "root_path": "$.items",
            "root_shape": "array",
            "count_estimate": 10000,
            "fields_top": ["id"],
            "sample_indices": [0, 50, 99],
        },
    ]
    result = build_describe_response(artifact_row, roots)

    root = result["roots"][0]
    assert root["sampled_only"] is True
    assert root["sample_indices"] == [0, 50, 99]
    assert root["sampled_record_count"] == 3


def test_build_describe_response_no_sample_indices_means_not_sampled() -> None:
    artifact_row = {
        "artifact_id": "art_1",
        "map_kind": "full",
        "map_status": "ready",
    }
    roots = [
        {
            "root_key": "rk_1",
            "root_path": "$.data",
            "root_shape": "array",
            "count_estimate": 5,
            "fields_top": ["id"],
        },
    ]
    result = build_describe_response(artifact_row, roots)

    root = result["roots"][0]
    assert "sampled_only" not in root
    assert "sample_indices" not in root


def test_build_describe_response_defaults_for_missing_mapping_fields() -> None:
    artifact_row = {"artifact_id": "art_2"}
    result = build_describe_response(artifact_row, [])

    mapping = result["mapping"]
    assert mapping["map_kind"] == "none"
    assert mapping["map_status"] == "pending"
    assert mapping["mapper_version"] is None


def test_build_describe_response_includes_schema_without_root_duplication() -> (
    None
):
    artifact_row = {"artifact_id": "art_schema", "map_kind": "full"}
    roots = [
        {
            "root_key": "rk_1",
            "root_path": "$.data",
            "root_shape": "array",
            "count_estimate": 2,
            "fields_top": {"id": {"number": 2}},
        }
    ]
    schemas = [
        {
            "version": "schema_v1",
            "schema_hash": "sha256:abc",
            "root_path": "$.data",
            "mode": "exact",
            "coverage": {
                "completeness": "complete",
                "observed_records": 2,
            },
            "fields": [
                {
                    "path": "$.id",
                    "types": ["number"],
                    "nullable": False,
                    "required": True,
                    "observed_count": 2,
                    "example_value": "1",
                }
            ],
            "determinism": {
                "dataset_hash": "sha256:def",
                "traversal_contract_version": "traversal_v1",
                "map_budget_fingerprint": None,
            },
        }
    ]
    result = build_describe_response(artifact_row, roots, schemas=schemas)
    assert len(result["schemas"]) == 1
    assert result["schemas"][0]["root_path"] == "$.data"
    assert "schema" not in result["roots"][0]
    assert result["schemas"][0]["fields"][0]["example_value"] == "1"
