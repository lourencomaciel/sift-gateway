"""Unit tests for benchmarks.tier1.code_result."""

from __future__ import annotations

from benchmarks.tier1.code_result import unwrap_code_result


class TestUnwrapCodeResult:
    def test_single_item_unwrapped(self) -> None:
        assert unwrap_code_result({"items": [42]}) == 42

    def test_multi_item_list(self) -> None:
        assert unwrap_code_result({"items": [1, 2, 3]}) == [1, 2, 3]

    def test_payload_fallback(self) -> None:
        assert unwrap_code_result({"payload": {"key": "val"}}) == {
            "key": "val",
        }

    def test_error_response_passthrough(self) -> None:
        resp = {"error": "CODE_TIMEOUT", "message": "timed out"}
        assert unwrap_code_result(resp) == resp

    def test_empty_items_returns_response(self) -> None:
        resp = {"items": []}
        assert unwrap_code_result(resp) == []

    def test_items_preferred_over_payload(self) -> None:
        resp = {"items": [99], "payload": "ignored"}
        assert unwrap_code_result(resp) == 99

    def test_unknown_shape_passthrough(self) -> None:
        resp = {"something": "else"}
        assert unwrap_code_result(resp) == resp

    def test_payload_none_returned(self) -> None:
        assert unwrap_code_result({"payload": None}) is None

    def test_error_with_items_returns_error(self) -> None:
        resp = {"error": "CODE_TIMEOUT", "items": [1, 2]}
        assert unwrap_code_result(resp) is resp
