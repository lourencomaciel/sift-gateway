"""Unit tests for benchmark harness helpers."""

from __future__ import annotations

import json
from unittest.mock import patch

from benchmarks.tier1.harness import (
    _build_nesting_hint,
    _effective_max_bytes,
    _extract_code_from_response,
    _extract_root_path_from_response,
    _field_has_type,
    _field_path,
    _field_type_names,
    _fits,
    _format_schema_for_prompt,
    _is_direct_child,
    _make_result,
    _run_sift,
    _truncate_dict,
    _truncate_for_baseline,
    _truncate_list,
)
from benchmarks.tier1.llm_client import LLMAPIError, LLMResponse
from benchmarks.tier1.questions import Question
from benchmarks.tier1.sift_runtime import CodeExecutionError
import pytest


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
        # 100_000 bytes < 200_000 * 3 = 600_000
        assert _effective_max_bytes(100_000, 200_000) == 100_000

    def test_token_derived_wins_when_smaller(self) -> None:
        # 500_000 bytes > 100_000 * 3 = 300_000
        assert _effective_max_bytes(500_000, 100_000) == 300_000

    def test_equal_caps(self) -> None:
        assert _effective_max_bytes(300_000, 100_000) == 300_000


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

    def test_single_item_exceeds_limit(self) -> None:
        data = [{"big": "x" * 500}]
        result = _truncate_list(data, 10)
        # Best-effort: returns valid JSON even though it exceeds limit.
        parsed = json.loads(result)
        assert isinstance(parsed, list)
        assert len(parsed) == 1


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

    def test_duplicate_keys_across_nesting_levels(self) -> None:
        # "vals" appears under two different parent dicts.
        # Both should be truncated independently.
        data = {
            "group_a": {"vals": list(range(500))},
            "group_b": {"vals": list(range(500))},
        }
        limit = 300
        result = _truncate_dict(data, limit)
        assert result is not None
        assert len(result.encode("utf-8")) <= limit
        parsed = json.loads(result)
        assert len(parsed["group_a"]["vals"]) < 500
        assert len(parsed["group_b"]["vals"]) < 500


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
        # With max_tokens=100 and 3 bytes/token, effective limit = 300
        data = list(range(10_000))
        text, truncated = _truncate_for_baseline(
            data, max_bytes=1_000_000, max_tokens=100
        )
        assert truncated
        assert len(text.encode("utf-8")) <= 300

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


# -- _field_path / _field_type_names / _field_has_type --


class TestFieldHelpers:
    def test_field_path_prefers_path_key(self) -> None:
        assert _field_path({"path": "$.a", "field_path": "$.b"}) == "$.a"

    def test_field_path_falls_back_to_field_path(self) -> None:
        assert _field_path({"field_path": "$.b"}) == "$.b"

    def test_field_path_default(self) -> None:
        assert _field_path({}) == "?"

    def test_field_type_names_list(self) -> None:
        assert _field_type_names({"types": ["string"]}) == ["string"]

    def test_field_type_names_string(self) -> None:
        assert _field_type_names({"types": "number"}) == ["number"]

    def test_field_type_names_empty(self) -> None:
        assert _field_type_names({}) == []

    def test_field_has_type_exact_match(self) -> None:
        assert _field_has_type({"types": ["array"]}, "array")

    def test_field_has_type_parametric(self) -> None:
        assert _field_has_type({"types": "array<number>"}, "array")

    def test_field_has_type_no_match(self) -> None:
        assert not _field_has_type({"types": ["string"]}, "array")


# -- _is_direct_child --


class TestIsDirectChild:
    def test_dot_notation_child(self) -> None:
        assert _is_direct_child("$[*].country", "$[*].country.en") == "en"

    def test_bracket_notation_child(self) -> None:
        assert (
            _is_direct_child("$[*].data", "$[*].data['special.key']")
            == "special.key"
        )

    def test_not_a_child(self) -> None:
        assert _is_direct_child("$[*].country", "$[*].name") is None

    def test_grandchild_dot(self) -> None:
        assert (
            _is_direct_child("$[*].birth", "$[*].birth.place.country") is None
        )

    def test_grandchild_bracket(self) -> None:
        assert _is_direct_child("$[*].d", "$[*].d['a']['b']") is None

    def test_array_index_not_a_child(self) -> None:
        assert _is_direct_child("$[*].tags", "$[*].tags[*]") is None

    def test_same_path_not_a_child(self) -> None:
        assert _is_direct_child("$[*].x", "$[*].x") is None

    def test_prefix_mismatch(self) -> None:
        assert _is_direct_child("$.alpha", "$.alphabet") is None


# -- _build_nesting_hint --


class TestBuildNestingHint:
    def test_simple_nesting(self) -> None:
        fields = [
            {"path": "$[*].country", "types": ["object"]},
            {"path": "$[*].country.en", "types": ["string"]},
            {"path": "$[*].country.no", "types": ["string"]},
        ]
        hint = _build_nesting_hint("$[*].country", fields)
        assert hint == '{"en": string, "no": string}'

    def test_no_children(self) -> None:
        fields = [
            {"path": "$[*].name", "types": ["string"]},
        ]
        assert _build_nesting_hint("$[*].name", fields) is None

    def test_skips_nested_children(self) -> None:
        fields = [
            {"path": "$[*].birth", "types": ["object"]},
            {"path": "$[*].birth.date", "types": ["string"]},
            {"path": "$[*].birth.place", "types": ["object"]},
            {"path": "$[*].birth.place.country", "types": ["object"]},
        ]
        hint = _build_nesting_hint("$[*].birth", fields)
        # Should only show direct children, not deeper nesting.
        assert hint == '{"date": string, "place": object}'

    def test_truncates_many_keys(self) -> None:
        fields = [{"path": "$[*].obj", "types": ["object"]}]
        fields.extend(
            {"path": f"$[*].obj.k{i}", "types": ["string"]} for i in range(6)
        )
        hint = _build_nesting_hint("$[*].obj", fields)
        assert hint is not None
        assert hint.endswith(", ...}")
        # Should show at most 4 keys.
        assert hint.count(":") == 4

    def test_mixed_child_types(self) -> None:
        fields = [
            {"path": "$[*].meta", "types": ["object"]},
            {"path": "$[*].meta.id", "types": ["number"]},
            {"path": "$[*].meta.tags", "types": ["array"]},
            {"path": "$[*].meta.active", "types": ["boolean", "null"]},
        ]
        hint = _build_nesting_hint("$[*].meta", fields)
        assert hint == '{"id": number, "tags": array, "active": boolean/null}'

    def test_field_path_key_fallback(self) -> None:
        """Supports legacy field_path key for backward compat."""
        fields = [
            {"field_path": "$.x", "types": ["object"]},
            {"field_path": "$.x.a", "types": ["string"]},
        ]
        hint = _build_nesting_hint("$.x", fields)
        assert hint == '{"a": string}'

    def test_bracket_notation_children(self) -> None:
        """Bracket-notation child paths are detected."""
        fields = [
            {"path": "$[*].data", "types": ["object"]},
            {"path": "$[*].data['key.one']", "types": ["string"]},
            {"path": "$[*].data.simple", "types": ["number"]},
        ]
        hint = _build_nesting_hint("$[*].data", fields)
        assert hint == '{"key.one": string, "simple": number}'

    def test_fallback_field_path_returns_none(self) -> None:
        """Field with unresolvable path (?) is never a child."""
        fields = [
            {"path": "$[*].obj", "types": ["object"]},
            {"types": ["string"]},
        ]
        assert _build_nesting_hint("$[*].obj", fields) is None


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
                        {"path": "$.temp", "types": ["array"]},
                        {"path": "$.humidity", "types": ["array"]},
                        {"path": "$.city", "types": ["array"]},
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

    def test_columnar_hint_with_string_types(self) -> None:
        """Columnar detection works with legacy string-format types."""
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
                        {"path": "$.temp", "types": "array<number>"},
                        {"path": "$.humidity", "types": "array<number>"},
                        {"path": "$.city", "types": "array<string>"},
                    ],
                },
            ],
        }
        result = _format_schema_for_prompt(describe)
        assert "columnar" in result

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
                        {"path": "$.name", "types": ["string"]},
                        {"path": "$.age", "types": ["number"]},
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
                        {"path": "$.name", "types": ["string"]},
                        {"path": "$.age", "types": ["number"]},
                        {"path": "$.tags", "types": ["array"]},
                    ],
                },
            ],
        }
        result = _format_schema_for_prompt(describe)
        assert "columnar" not in result

    def test_nesting_hint_for_object_fields(self) -> None:
        describe = {
            "roots": [
                {
                    "root_path": "$",
                    "count_estimate": 100,
                    "root_shape": "array",
                },
            ],
            "schemas": [
                {
                    "root_path": "$",
                    "fields": [
                        {"path": "$[*].name", "types": ["string"]},
                        {"path": "$[*].country", "types": ["object"]},
                        {
                            "path": "$[*].country.en",
                            "types": ["string"],
                        },
                        {
                            "path": "$[*].country.no",
                            "types": ["string"],
                        },
                    ],
                },
            ],
        }
        result = _format_schema_for_prompt(describe)
        assert 'object {"en": string, "no": string}' in result
        # Child fields should still appear in the flat listing.
        assert "$[*].country.en" in result

    def test_field_paths_displayed_from_path_key(self) -> None:
        """Describe result uses 'path' key, not 'field_path'."""
        describe = {
            "roots": [],
            "schemas": [
                {
                    "root_path": "$",
                    "fields": [
                        {"path": "$[*].id", "types": ["number"]},
                    ],
                },
            ],
        }
        result = _format_schema_for_prompt(describe)
        assert "$[*].id" in result
        assert "?" not in result


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


# -- _run_sift retry loop --


def _llm_resp(
    text: str = "def run(data, schema, params):\n  return 42",
) -> LLMResponse:
    return LLMResponse(
        text=text,
        input_tokens=10,
        output_tokens=10,
        model="test",
        latency_ms=1.0,
    )


class TestRunSiftRetryLoop:
    """Verify that CodeExecutionError triggers retries while LLMAPIError propagates."""

    def test_code_execution_error_retried(self) -> None:
        q = _stub_question()
        with (
            patch(
                "benchmarks.tier1.harness.call_llm",
                return_value=_llm_resp(),
            ),
            patch(
                "benchmarks.tier1.harness.execute_code",
                side_effect=CodeExecutionError("bad code"),
            ),
        ):
            result = _run_sift(
                q,
                [1, 2],
                runtime=None,
                artifact_id="art_test",
                root_paths=["$"],
                schema_text="test schema",
                model="test",
                api_key="k",
                temperature=0.0,
                max_retries=1,
            )
        # Should exhaust retries and mark as not attempted.
        assert result["attempted"] is False
        assert result["retries"] == 1

    def test_llm_api_error_propagates(self) -> None:
        q = _stub_question()
        with (
            patch(
                "benchmarks.tier1.harness.call_llm",
                side_effect=LLMAPIError("rate limited"),
            ),
            pytest.raises(LLMAPIError, match="rate limited"),
        ):
            _run_sift(
                q,
                [1, 2],
                runtime=None,
                artifact_id="art_test",
                root_paths=["$"],
                schema_text="test schema",
                model="test",
                api_key="k",
                temperature=0.0,
                max_retries=1,
            )

    def test_success_after_retry(self) -> None:
        q = _stub_question()
        call_count = 0

        def exec_side_effect(*_a: object, **_kw: object) -> dict:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise CodeExecutionError("first fail")
            return {"items": [42]}

        with (
            patch(
                "benchmarks.tier1.harness.call_llm",
                return_value=_llm_resp(
                    "def run(data, schema, params):\n  return 42"
                ),
            ),
            patch(
                "benchmarks.tier1.harness.execute_code",
                side_effect=exec_side_effect,
            ),
        ):
            result = _run_sift(
                q,
                [1, 2],
                runtime=None,
                artifact_id="art_test",
                root_paths=["$"],
                schema_text="test schema",
                model="test",
                api_key="k",
                temperature=0.0,
                max_retries=2,
            )
        assert result["attempted"] is True
        assert result["retries"] == 1

    def test_max_retries_zero_no_retry(self) -> None:
        q = _stub_question()
        with (
            patch(
                "benchmarks.tier1.harness.call_llm",
                return_value=_llm_resp(),
            ),
            patch(
                "benchmarks.tier1.harness.execute_code",
                side_effect=CodeExecutionError("fail"),
            ),
        ):
            result = _run_sift(
                q,
                [1, 2],
                runtime=None,
                artifact_id="art_test",
                root_paths=["$"],
                schema_text="test schema",
                model="test",
                api_key="k",
                temperature=0.0,
                max_retries=0,
            )
        assert result["attempted"] is False
        assert result["retries"] == 0


# -- _extract_code_from_response --


class TestExtractCodeFromResponse:
    def test_python_fence(self) -> None:
        text = "Here is the code:\n```python\ndef run(data, schema, params):\n  return 42\n```"
        assert "def run" in _extract_code_from_response(text)
        assert "return 42" in _extract_code_from_response(text)

    def test_generic_fence_fallback(self) -> None:
        text = "```\ndef run(data, schema, params):\n  return 99\n```"
        result = _extract_code_from_response(text)
        assert "def run" in result
        assert "return 99" in result

    def test_raw_text_fallback(self) -> None:
        text = "def run(data, schema, params):\n  return 1"
        assert "def run" in _extract_code_from_response(text)

    def test_prefers_candidate_with_def_run(self) -> None:
        text = (
            "```python\nprint('hello')\n```\n\n"
            "def run(data, schema, params):\n  return 5"
        )
        result = _extract_code_from_response(text)
        assert "def run" in result

    def test_no_def_run_uses_first_candidate(self) -> None:
        text = "```python\nresult = 42\n```"
        result = _extract_code_from_response(text)
        assert "result = 42" in result

    def test_multiple_python_fences_uses_first(self) -> None:
        text = (
            "```python\ndef run(data, schema, params):\n  return 1\n```\n"
            "```python\ndef run(data, schema, params):\n  return 2\n```"
        )
        result = _extract_code_from_response(text)
        assert "return 1" in result

    def test_empty_text(self) -> None:
        assert _extract_code_from_response("") == ""


# -- _extract_root_path_from_response --


class TestExtractRootPathFromResponse:
    def test_parses_root_path_comment(self) -> None:
        text = "# root_path: $.features\ndef run(data, schema, params):\n  return 1"
        result = _extract_root_path_from_response(
            text, ["$.features", "$.metadata"]
        )
        assert result == "$.features"

    def test_returns_none_when_not_in_available(self) -> None:
        text = (
            "# root_path: $.unknown\ndef run(data, schema, params):\n  return 1"
        )
        result = _extract_root_path_from_response(text, ["$.features"])
        assert result is None

    def test_returns_none_when_no_comment(self) -> None:
        text = "def run(data, schema, params):\n  return 1"
        result = _extract_root_path_from_response(text, ["$.features"])
        assert result is None

    def test_picks_first_valid_line(self) -> None:
        text = (
            "# root_path: $.invalid\n"
            "# root_path: $.valid\n"
            "def run(data, schema, params):\n  return 1"
        )
        result = _extract_root_path_from_response(text, ["$.valid"])
        assert result == "$.valid"

    def test_dollar_root(self) -> None:
        text = "# root_path: $\ndef run(data, schema, params):\n  return 1"
        result = _extract_root_path_from_response(text, ["$"])
        assert result == "$"
