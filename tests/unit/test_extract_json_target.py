"""Tests for extract_json_target and _parse_text_as_json."""

from __future__ import annotations

import json

from sift_gateway.core.retrieval_helpers import (
    _parse_text_as_json,
    extract_json_target,
)


class TestParseTextAsJson:
    """Unit tests for _parse_text_as_json."""

    def test_valid_dict(self) -> None:
        result = _parse_text_as_json('{"key": "value"}')
        assert result == {"key": "value"}

    def test_valid_list(self) -> None:
        result = _parse_text_as_json("[1, 2, 3]")
        assert result == [1, 2, 3]

    def test_whitespace_padded(self) -> None:
        result = _parse_text_as_json('  {"a": 1}  ')
        assert result == {"a": 1}

    def test_double_encoded_dict(self) -> None:
        inner = json.dumps({"nested": True})
        double = json.dumps(inner)
        result = _parse_text_as_json(double)
        assert result == {"nested": True}

    def test_double_encoded_list(self) -> None:
        inner = json.dumps([1, 2])
        double = json.dumps(inner)
        result = _parse_text_as_json(double)
        assert result == [1, 2]

    def test_plain_text_returns_none(self) -> None:
        assert _parse_text_as_json("hello world") is None

    def test_empty_string_returns_none(self) -> None:
        assert _parse_text_as_json("") is None

    def test_whitespace_only_returns_none(self) -> None:
        assert _parse_text_as_json("   ") is None

    def test_json_number_returns_none(self) -> None:
        assert _parse_text_as_json("42") is None

    def test_json_boolean_returns_none(self) -> None:
        assert _parse_text_as_json("true") is None

    def test_json_string_scalar_returns_none(self) -> None:
        assert _parse_text_as_json('"just a string"') is None

    def test_invalid_json_returns_none(self) -> None:
        assert _parse_text_as_json("{broken json") is None


class TestExtractJsonTarget:
    """Unit tests for extract_json_target."""

    def test_json_part(self) -> None:
        envelope = {
            "content": [
                {"type": "json", "value": [{"id": 1}]},
            ],
        }
        result = extract_json_target(envelope, 0)
        assert result == [{"id": 1}]

    def test_text_part_with_json_dict(self) -> None:
        envelope = {
            "content": [
                {"type": "text", "text": '{"key": "val"}'},
            ],
        }
        result = extract_json_target(envelope, 0)
        assert result == {"key": "val"}

    def test_text_part_with_json_list(self) -> None:
        data = [{"id": i} for i in range(3)]
        envelope = {
            "content": [
                {"type": "text", "text": json.dumps(data)},
            ],
        }
        result = extract_json_target(envelope, 0)
        assert result == data

    def test_text_part_non_json_returns_envelope(self) -> None:
        envelope = {
            "content": [
                {"type": "text", "text": "not json"},
            ],
        }
        result = extract_json_target(envelope, 0)
        assert result is envelope

    def test_mapped_part_index_none_returns_envelope(self) -> None:
        envelope = {
            "content": [
                {"type": "json", "value": [1, 2]},
            ],
        }
        result = extract_json_target(envelope, None)
        assert result is envelope

    def test_mapped_part_index_out_of_range_returns_envelope(self) -> None:
        envelope = {"content": []}
        result = extract_json_target(envelope, 5)
        assert result is envelope

    def test_non_dict_part_returns_envelope(self) -> None:
        envelope = {"content": ["not a dict"]}
        result = extract_json_target(envelope, 0)
        assert result is envelope

    def test_selects_correct_part_index(self) -> None:
        envelope = {
            "content": [
                {"type": "text", "text": "ignored"},
                {"type": "json", "value": {"target": True}},
            ],
        }
        result = extract_json_target(envelope, 1)
        assert result == {"target": True}

    def test_text_part_double_encoded(self) -> None:
        inner = json.dumps({"deep": 1})
        double = json.dumps(inner)
        envelope = {
            "content": [
                {"type": "text", "text": double},
            ],
        }
        result = extract_json_target(envelope, 0)
        assert result == {"deep": 1}
