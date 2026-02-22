"""Unit tests for benchmark harness helpers."""

from __future__ import annotations

from benchmarks.tier1.harness import _format_schema_for_prompt, _make_result
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
                        {"field_path": "$.humidity", "types": "array<number>"},
                        {"field_path": "$.city", "types": "array<string>"},
                    ],
                },
            ],
        }
        result = _format_schema_for_prompt(describe)
        assert "columnar" in result
        assert 'data["field"][i]' in result

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
