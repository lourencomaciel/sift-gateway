"""Unit tests for benchmark sift_runtime pure-logic helpers."""

from __future__ import annotations

from benchmarks.common.sift_runtime import (
    extract_root_paths,
    is_error_response,
)


class TestIsErrorResponse:
    def test_gateway_error_type(self) -> None:
        payload = {
            "type": "gateway_error",
            "code": "NOT_FOUND",
            "message": "not found",
        }
        assert is_error_response(payload) is True

    def test_capture_error_without_artifact_id(self) -> None:
        payload = {"code": "INVALID", "message": "bad request"}
        assert is_error_response(payload) is True

    def test_success_with_artifact_id(self) -> None:
        payload = {
            "code": "OK",
            "message": "captured",
            "artifact_id": "art_123",
        }
        assert is_error_response(payload) is False

    def test_normal_describe_result(self) -> None:
        payload = {
            "artifact_id": "art_123",
            "roots": [],
            "schemas": [],
        }
        assert is_error_response(payload) is False

    def test_empty_payload(self) -> None:
        assert is_error_response({}) is False

    def test_code_not_string(self) -> None:
        payload = {"code": 200, "message": "ok"}
        assert is_error_response(payload) is False


class TestExtractRootPaths:
    def test_extracts_from_roots(self) -> None:
        result = {
            "roots": [
                {"root_path": "$.features"},
                {"root_path": "$.metadata"},
            ],
        }
        assert extract_root_paths(result) == [
            "$.features",
            "$.metadata",
        ]

    def test_falls_back_to_schemas(self) -> None:
        result = {
            "schemas": [
                {"root_path": "$.items"},
            ],
        }
        assert extract_root_paths(result) == ["$.items"]

    def test_falls_back_to_dollar(self) -> None:
        assert extract_root_paths({}) == ["$"]

    def test_skips_non_dict_roots(self) -> None:
        result = {
            "roots": [
                "not a dict",
                {"root_path": "$.valid"},
            ],
        }
        assert extract_root_paths(result) == ["$.valid"]

    def test_skips_empty_root_path(self) -> None:
        result = {
            "roots": [
                {"root_path": ""},
                {"root_path": "$.data"},
            ],
        }
        assert extract_root_paths(result) == ["$.data"]

    def test_skips_non_string_root_path(self) -> None:
        result = {
            "roots": [
                {"root_path": 123},
                {"root_path": "$.ok"},
            ],
        }
        assert extract_root_paths(result) == ["$.ok"]

    def test_empty_roots_falls_to_schemas(self) -> None:
        result = {
            "roots": [],
            "schemas": [{"root_path": "$.from_schema"}],
        }
        assert extract_root_paths(result) == ["$.from_schema"]

    def test_roots_none_falls_to_schemas(self) -> None:
        result = {
            "roots": None,
            "schemas": [{"root_path": "$.fallback"}],
        }
        assert extract_root_paths(result) == ["$.fallback"]
