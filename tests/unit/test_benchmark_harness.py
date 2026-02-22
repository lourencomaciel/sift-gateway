"""Unit tests for benchmark harness helpers."""

from __future__ import annotations

import json

from benchmarks.tier1.harness import (
    _effective_max_bytes,
    _fits,
    _format_schema_for_prompt,
    _make_result,
    _truncate_dict,
    _truncate_for_baseline,
    _truncate_list,
)
from benchmarks.tier1.questions import Question


def _stub_question(**overrides: object) -> Question:
    """Build a minimal Question for _make_result tests."""
    defaults = {
        "dataset_name": "ds1",
        "question_id": "q1",
        "question_text": "What is 1+1?",
        "question_type": "number",
        "answer_type": "number",
        "gold_answer_fn": lambda _d: "2",
        "tolerance": 0.0,
    }
    defaults.update(overrides)
    return Question(**defaults)  # type: ignore[arg-type]


# -- _effective_max_bytes --


class TestEffectiveMaxBytes:
    def test_byte_cap_wins_when_smaller(self) -> None:
        # 100_000 bytes < 200_000 * 2 = 400_000
        assert _effective_max_bytes(100_000, 200_000) == 100_000

    def test_token_derived_wins_when_smaller(self) -> None:
        # 500_000 bytes > 100_000 * 2 = 200_000
        assert _effective_max_bytes(500_000, 100_000) == 200_000

    def test_equal_caps(self) -> None:
        assert _effective_max_bytes(200_000, 100_000) == 200_000


# -- _fits --


class TestFits:
    def test_fits_within_limit(self) -> None:
        assert _fits("hello", 10)

    def test_exceeds_limit(self) -> None:
        assert not _fits("hello world", 5)

    def test_multibyte_chars(self) -> None:
        # "é" is 2 bytes in UTF-8
        text = "é" * 5  # 10 bytes
        assert _fits(text, 10)
        assert not _fits(text, 9)


# -- _truncate_list --


class TestTruncateList:
    def test_returns_prefix_that_fits(self) -> None:
        data = [{"id": i, "value": "x" * 100} for i in range(100)]
        limit = 500
        result = _truncate_list(data, limit)
        assert len(result.encode("utf-8")) <= limit
        parsed = json.loads(result)
        assert isinstance(parsed, list)
        assert len(parsed) >= 1

    def test_single_item_always_returned(self) -> None:
        data = [{"id": 1}]
        result = _truncate_list(data, 100_000)
        assert json.loads(result) == [{"id": 1}]


# -- _truncate_dict --


class TestTruncateDict:
    def test_shrinks_top_level_arrays(self) -> None:
        data = {
            "meta": "info",
            "values": list(range(1000)),
        }
        limit = 200
        result = _truncate_dict(data, limit)
        assert result is not None
        assert len(result.encode("utf-8")) <= limit
        parsed = json.loads(result)
        assert parsed["meta"] == "info"
        assert len(parsed["values"]) < 1000

    def test_shrinks_nested_arrays(self) -> None:
        data = {
            "hourly": {
                "time": [f"2025-01-{i:02d}" for i in range(1, 100)],
                "temp": [float(i) for i in range(99)],
            },
        }
        limit = 500
        result = _truncate_dict(data, limit)
        assert result is not None
        assert len(result.encode("utf-8")) <= limit
        parsed = json.loads(result)
        assert len(parsed["hourly"]["time"]) < 99

    def test_returns_none_when_no_arrays(self) -> None:
        data = {"a": 1, "b": "hello"}
        result = _truncate_dict(data, 5)
        assert result is None


# -- _truncate_for_baseline --


class TestTruncateForBaseline:
    def test_no_truncation_when_small(self) -> None:
        data = [1, 2, 3]
        text, truncated = _truncate_for_baseline(
            data, max_bytes=1_000_000, max_tokens=500_000
        )
        assert not truncated
        assert json.loads(text) == [1, 2, 3]

    def test_truncates_large_list(self) -> None:
        data = list(range(100_000))
        text, truncated = _truncate_for_baseline(
            data, max_bytes=1_000, max_tokens=500_000
        )
        assert truncated
        parsed = json.loads(text)
        assert isinstance(parsed, list)
        assert len(parsed) < 100_000

    def test_token_limit_caps_before_byte_limit(self) -> None:
        # With max_tokens=100 and 2 bytes/token, effective limit = 200
        data = list(range(10_000))
        text, truncated = _truncate_for_baseline(
            data, max_bytes=1_000_000, max_tokens=100
        )
        assert truncated
        assert len(text.encode("utf-8")) <= 200

    def test_truncates_dict_with_arrays(self) -> None:
        data = {"vals": list(range(10_000))}
        text, truncated = _truncate_for_baseline(
            data, max_bytes=500, max_tokens=500_000
        )
        assert truncated
        parsed = json.loads(text)
        assert isinstance(parsed, dict)
        assert "vals" in parsed
        assert len(parsed["vals"]) < 10_000

    def test_fallback_note_for_dict_without_arrays(self) -> None:
        data = {"a": "x" * 10_000}
        text, truncated = _truncate_for_baseline(
            data, max_bytes=100, max_tokens=500_000
        )
        assert truncated
        parsed = json.loads(text)
        assert parsed["_truncated"] is True


# -- _format_schema_for_prompt: columnar hint --


class TestFormatSchemaForPrompt:
    def test_columnar_hint_when_object_with_arrays(self) -> None:
        describe = {
            "roots": [
                {
                    "root_path": "$",
                    "count_estimate": 100,
                    "root_shape": "object",
                },
            ],
            "schemas": [
                {
                    "root_path": "$",
                    "fields": [
                        {"field_path": "$.temp", "types": "array<number>"},
                        {
                            "field_path": "$.humidity",
                            "types": "array<number>",
                        },
                        {"field_path": "$.city", "types": "array<string>"},
                    ],
                },
            ],
        }
        result = _format_schema_for_prompt(describe)
        assert "IMPORTANT" in result
        assert "columnar" in result
        assert "dict of parallel arrays" in result
        # Should include a concrete code example with actual field name.
        assert 'data["temp"]' in result
        assert "sum(" in result

    def test_no_columnar_hint_for_array_root(self) -> None:
        describe = {
            "roots": [
                {
                    "root_path": "$",
                    "count_estimate": 50,
                    "root_shape": "array",
                },
            ],
            "schemas": [
                {
                    "root_path": "$",
                    "fields": [
                        {"field_path": "$.name", "types": "string"},
                        {"field_path": "$.age", "types": "number"},
                    ],
                },
            ],
        }
        result = _format_schema_for_prompt(describe)
        assert "columnar" not in result

    def test_no_columnar_hint_when_minority_arrays(self) -> None:
        describe = {
            "roots": [
                {
                    "root_path": "$",
                    "count_estimate": 1,
                    "root_shape": "object",
                },
            ],
            "schemas": [
                {
                    "root_path": "$",
                    "fields": [
                        {"field_path": "$.name", "types": "string"},
                        {"field_path": "$.age", "types": "number"},
                        {"field_path": "$.tags", "types": "array<string>"},
                    ],
                },
            ],
        }
        result = _format_schema_for_prompt(describe)
        assert "columnar" not in result


# -- _make_result --


class TestMakeResult:
    def test_default_attempted_true(self) -> None:
        q = _stub_question()
        r = _make_result(q, condition="baseline", gold="2")
        assert r["attempted"] is True

    def test_attempted_false_explicit(self) -> None:
        q = _stub_question()
        r = _make_result(q, condition="sift", gold="2", attempted=False)
        assert r["attempted"] is False
