"""Unit tests for benchmark evaluation functions."""

from __future__ import annotations

from benchmarks.tier1.evaluate import (
    build_report,
    evaluate_answer,
    latency_percentiles,
    match_boolean,
    match_list,
    match_number,
    match_string,
)
from benchmarks.tier1.questions import question_set_hash
import pytest

# -- match_number --


class TestMatchNumber:
    def test_exact_integer(self) -> None:
        assert match_number("500", "500")

    def test_exact_float(self) -> None:
        assert match_number("3.14", "3.14")

    def test_rejects_off_by_one_default_tolerance(self) -> None:
        # Default tolerance=0.0 requires exact match.
        assert not match_number("501", "500")
        assert not match_number("499", "500")

    def test_absolute_tolerance_accepts_within_range(self) -> None:
        # tolerance=0.01 means +-0.01 absolute.
        assert match_number("200.005", "200", tolerance=0.01)

    def test_absolute_tolerance_rejects_outside_range(self) -> None:
        assert not match_number("200.02", "200", tolerance=0.01)
        assert not match_number("201", "200", tolerance=0.01)

    def test_zero_gold(self) -> None:
        assert match_number("0", "0")
        assert match_number("0.005", "0", tolerance=0.01)
        assert not match_number("1", "0")

    def test_commas_stripped(self) -> None:
        assert match_number("1,500", "1500")
        assert match_number("1500", "1,500")

    def test_extracts_number_from_text(self) -> None:
        assert match_number("The answer is 42.", "42")

    def test_non_numeric_returns_false(self) -> None:
        assert not match_number("no number here", "42")

    def test_empty_answer_returns_false(self) -> None:
        assert not match_number("", "42")

    def test_large_number_exact(self) -> None:
        assert match_number("750000000", "750000000")
        assert not match_number("745000000", "750000000")

    def test_negative_number(self) -> None:
        assert match_number("-5", "-5")
        assert not match_number("5", "-5")

    def test_close_floats_without_tolerance(self) -> None:
        # Different floats must not match at default tolerance=0.0.
        assert not match_number("3.14159", "3.14")

    def test_first_number_wins(self) -> None:
        # Extracts first numeric token — may mis-score if LLM
        # elaborates before answering.
        assert match_number("There are 42 items", "42")
        assert not match_number("There are 10 items costing 42", "42")


# -- match_string --


class TestMatchString:
    def test_exact_match(self) -> None:
        assert match_string("Paris", "Paris")

    def test_case_insensitive(self) -> None:
        assert match_string("PARIS", "paris")

    def test_gold_in_llm_elaborated(self) -> None:
        assert match_string("The capital is Paris", "Paris")

    def test_rejects_empty(self) -> None:
        assert not match_string("", "Paris")

    def test_rejects_wrong_answer(self) -> None:
        assert not match_string("London", "Paris")

    def test_short_gold_word_boundary(self) -> None:
        # "ak" should not match inside "make"
        assert not match_string("make", "ak")
        assert not match_string("because", "us")

    def test_short_gold_standalone(self) -> None:
        # "ak" as a standalone word should match
        assert match_string("the network is ak", "ak")
        assert match_string("ak", "ak")

    def test_multiword_gold(self) -> None:
        assert match_string(
            "The answer is Sub-Saharan Africa",
            "Sub-Saharan Africa",
        )

    def test_rejects_partial_match(self) -> None:
        # LLM-in-gold direction is not accepted
        assert not match_string("pari", "paris")

    def test_empty_gold_rejects(self) -> None:
        assert not match_string("anything", "")

    def test_both_empty(self) -> None:
        assert match_string("", "")

    def test_gold_with_regex_metacharacters(self) -> None:
        # re.escape should handle metacharacters in gold.
        assert match_string("I use C++", "C++")
        assert match_string("file.txt", "file.txt")
        assert not match_string("filetxt", "file.txt")


# -- match_boolean --


class TestMatchBoolean:
    def test_yes_matches_yes(self) -> None:
        assert match_boolean("Yes", "Yes")

    def test_true_matches_yes(self) -> None:
        assert match_boolean("true", "Yes")

    def test_one_matches_yes(self) -> None:
        assert match_boolean("1", "Yes")

    def test_no_matches_no(self) -> None:
        assert match_boolean("No", "No")

    def test_false_matches_no(self) -> None:
        assert match_boolean("false", "No")

    def test_zero_matches_no(self) -> None:
        assert match_boolean("0", "No")

    def test_yes_rejects_no(self) -> None:
        assert not match_boolean("Yes", "No")

    def test_no_rejects_yes(self) -> None:
        assert not match_boolean("No", "Yes")

    def test_case_insensitive(self) -> None:
        assert match_boolean("YES", "yes")
        assert match_boolean("no", "NO")

    def test_whitespace_stripped(self) -> None:
        assert match_boolean("  yes  ", "  Yes  ")
        assert match_boolean("  no  ", "No")

    def test_unrecognized_gold_returns_false(self) -> None:
        assert not match_boolean("yes", "maybe")
        assert not match_boolean("no", "unknown")

    def test_verbose_llm_rejected(self) -> None:
        # match_boolean is strict — no word-boundary extraction.
        assert not match_boolean("Yes, there is", "Yes")
        assert not match_boolean("No, there is not", "No")

    def test_cross_variant_yes_matches_true_gold(self) -> None:
        assert match_boolean("yes", "true")
        assert match_boolean("YES", "True")

    def test_cross_variant_no_matches_false_gold(self) -> None:
        assert match_boolean("no", "false")
        assert match_boolean("NO", "False")

    def test_zero_matches_false_gold(self) -> None:
        assert match_boolean("0", "false")

    def test_one_matches_true_gold(self) -> None:
        assert match_boolean("1", "true")

    def test_empty_llm_rejected(self) -> None:
        assert not match_boolean("", "Yes")
        assert not match_boolean("", "No")

    def test_empty_gold_rejected(self) -> None:
        assert not match_boolean("Yes", "")
        assert not match_boolean("No", "")

    def test_numeric_non_boolean_rejected(self) -> None:
        # Only "1" and "0" are valid; other numerics are not.
        assert not match_boolean("2", "yes")
        assert not match_boolean("-1", "no")


# -- match_list --


class TestMatchList:
    def test_exact_set(self) -> None:
        assert match_list('["a", "b"]', '["b", "a"]')

    def test_rejects_missing_element(self) -> None:
        assert not match_list('["a"]', '["a", "b"]')

    def test_rejects_extra_element(self) -> None:
        assert not match_list('["a", "b", "c"]', '["a", "b"]')

    def test_case_insensitive(self) -> None:
        assert match_list('["A", "B"]', '["a", "b"]')

    def test_invalid_json(self) -> None:
        assert not match_list("not json", '["a"]')
        assert not match_list('["a"]', "not json")

    def test_non_list_json(self) -> None:
        assert not match_list('{"a": 1}', '["a"]')

    def test_duplicates_preserved(self) -> None:
        assert not match_list('["a", "a"]', '["a"]')
        assert not match_list('["a"]', '["a", "a"]')
        assert match_list('["a", "a"]', '["a", "a"]')


# -- evaluate_answer --


class TestEvaluateAnswer:
    def test_routes_number(self) -> None:
        assert evaluate_answer("42", "42", answer_type="number")
        assert not evaluate_answer("43", "42", answer_type="number")

    def test_routes_string(self) -> None:
        assert evaluate_answer("Paris", "Paris", answer_type="string")
        assert not evaluate_answer("London", "Paris", answer_type="string")

    def test_routes_boolean(self) -> None:
        assert evaluate_answer("Yes", "Yes", answer_type="boolean")
        assert evaluate_answer("true", "Yes", answer_type="boolean")
        assert not evaluate_answer("No", "Yes", answer_type="boolean")

    def test_routes_list(self) -> None:
        assert evaluate_answer('["a", "b"]', '["b", "a"]', answer_type="list")

    def test_tolerance_forwarded(self) -> None:
        assert evaluate_answer(
            "200.005", "200", answer_type="number", tolerance=0.01
        )
        assert not evaluate_answer(
            "201", "200", answer_type="number", tolerance=0.0
        )

    def test_unknown_answer_type_falls_through_to_string(self) -> None:
        assert evaluate_answer("Paris", "Paris", answer_type="unknown")
        assert not evaluate_answer("London", "Paris", answer_type="unknown")


# -- latency_percentiles --


class TestLatencyPercentiles:
    def test_empty_list(self) -> None:
        assert latency_percentiles([]) == {}

    def test_single_value(self) -> None:
        result = latency_percentiles([100.0])
        assert result["p50_ms"] == 100.0
        assert result["p90_ms"] == 100.0
        assert result["mean_ms"] == 100.0
        assert result["count"] == 1

    def test_two_values(self) -> None:
        result = latency_percentiles([100.0, 200.0])
        assert result["count"] == 2
        assert result["mean_ms"] == 150.0
        # statistics.median interpolates midpoint for even n
        assert result["p50_ms"] == 150.0
        # nearest-rank: p90 idx=ceil(2*0.9)-1=1 → 200
        assert result["p90_ms"] == 200.0

    def test_multiple_values(self) -> None:
        latencies = [
            10.0,
            20.0,
            30.0,
            40.0,
            50.0,
            60.0,
            70.0,
            80.0,
            90.0,
            100.0,
        ]
        result = latency_percentiles(latencies)
        assert result["count"] == 10
        assert result["mean_ms"] == 55.0
        # statistics.median: (50+60)/2 = 55.0 for even n
        assert result["p50_ms"] == 55.0
        # nearest-rank: p90 idx=ceil(10*0.9)-1=8 → 90.0
        assert result["p90_ms"] == 90.0


# -- build_report --


def _stub_result(
    condition: str,
    dataset: str,
    *,
    correct: bool = False,
    attempted: bool = True,
    difficulty: int = 1,
) -> dict:
    """Minimal result dict for report tests."""
    return {
        "condition": condition,
        "dataset": dataset,
        "question_id": "q1",
        "question_type": "number",
        "difficulty": difficulty,
        "question_text": "stub",
        "gold_answer": "42",
        "llm_answer": "42" if correct else "0",
        "correct": correct,
        "input_tokens": 100,
        "output_tokens": 10,
        "latency_ms": 50.0,
        "attempted": attempted,
    }


class TestBuildReport:
    def test_error_counts_tracked(self) -> None:
        results = [
            _stub_result("baseline", "ds1", correct=True),
            _stub_result("baseline", "ds1", attempted=False),
            _stub_result("sift", "ds1", correct=True),
            _stub_result("sift", "ds1", attempted=False),
            _stub_result("sift", "ds1", attempted=False),
        ]
        report = build_report(results, model="test")
        s = report["summary"]
        assert s["baseline_errors"] == 1
        assert s["baseline_attempted"] == 1
        assert s["sift_errors"] == 2
        assert s["sift_attempted"] == 1

        ds = report["per_dataset"]["ds1"]
        assert ds["baseline_errors"] == 1
        assert ds["sift_errors"] == 2

    def test_backward_compat_no_attempted(self) -> None:
        results = [
            {
                "condition": "baseline",
                "dataset": "ds1",
                "question_id": "q1",
                "question_type": "number",
                "question_text": "stub",
                "gold_answer": "42",
                "llm_answer": "42",
                "correct": True,
                "input_tokens": 100,
                "output_tokens": 10,
                "latency_ms": 50.0,
                # No "attempted" key — should default to True.
            },
        ]
        report = build_report(results, model="test")
        assert report["summary"]["baseline_errors"] == 0
        assert report["per_dataset"]["ds1"]["baseline_errors"] == 0

    def test_per_question_type_breakdown(self) -> None:
        results = [
            {
                **_stub_result("baseline", "ds1", correct=True),
                "question_type": "count",
            },
            {
                **_stub_result("baseline", "ds1", correct=False),
                "question_type": "aggregation",
            },
            {
                **_stub_result("sift", "ds1", correct=True),
                "question_type": "count",
            },
            {
                **_stub_result("sift", "ds1", correct=True),
                "question_type": "aggregation",
            },
        ]
        report = build_report(results, model="test")
        qt = report["per_question_type"]
        assert qt["count"]["baseline_correct"] == 1
        assert qt["count"]["baseline_total"] == 1
        assert qt["count"]["sift_correct"] == 1
        assert qt["count"]["sift_total"] == 1
        assert qt["aggregation"]["baseline_correct"] == 0
        assert qt["aggregation"]["baseline_total"] == 1
        assert qt["aggregation"]["sift_correct"] == 1

    def test_token_stats(self) -> None:
        results = [
            _stub_result("baseline", "ds1", correct=True),
            _stub_result("sift", "ds1", correct=True),
        ]
        report = build_report(results, model="test")
        s = report["summary"]
        assert s["baseline_input_tokens"] == 100
        assert s["baseline_output_tokens"] == 10
        assert s["sift_input_tokens"] == 100
        assert s["sift_output_tokens"] == 10

    def test_token_reduction_clamped_to_zero(self) -> None:
        results = [
            {**_stub_result("baseline", "ds1"), "input_tokens": 10},
            {**_stub_result("sift", "ds1"), "input_tokens": 100},
        ]
        report = build_report(results, model="test")
        assert report["summary"]["token_reduction_pct"] == 0

    def test_question_hash_included(self) -> None:
        results = [_stub_result("baseline", "ds1")]
        report = build_report(results, model="test", question_hash="abc123")
        assert report["question_set_hash"] == "abc123"

    def test_question_hash_empty_default(self) -> None:
        results = [_stub_result("baseline", "ds1")]
        report = build_report(results, model="test")
        assert report["question_set_hash"] == ""

    def test_empty_results(self) -> None:
        report = build_report([], model="test")
        s = report["summary"]
        assert s["baseline_accuracy_pct"] == 0
        assert s["sift_accuracy_pct"] == 0
        assert s["token_reduction_pct"] == 0

    def test_per_difficulty_breakdown(self) -> None:
        results = [
            _stub_result("baseline", "ds1", correct=True, difficulty=1),
            _stub_result("baseline", "ds1", correct=False, difficulty=2),
            _stub_result("sift", "ds1", correct=True, difficulty=1),
            _stub_result("sift", "ds1", correct=True, difficulty=2),
        ]
        report = build_report(results, model="test")
        pd = report["per_difficulty"]
        assert pd["1"]["baseline_correct"] == 1
        assert pd["1"]["baseline_total"] == 1
        assert pd["1"]["sift_correct"] == 1
        assert pd["1"]["sift_total"] == 1
        assert pd["2"]["baseline_correct"] == 0
        assert pd["2"]["baseline_total"] == 1
        assert pd["2"]["sift_correct"] == 1
        assert pd["2"]["sift_total"] == 1

    def test_per_difficulty_latency_included(self) -> None:
        results = [
            {
                **_stub_result("baseline", "ds1", difficulty=1),
                "latency_ms": 100.0,
            },
            {
                **_stub_result("sift", "ds1", difficulty=1),
                "latency_ms": 200.0,
            },
        ]
        report = build_report(results, model="test")
        pd = report["per_difficulty"]["1"]
        assert pd["baseline_latency"]["count"] == 1
        assert pd["baseline_latency"]["mean_ms"] == 100.0
        assert pd["sift_latency"]["count"] == 1
        assert pd["sift_latency"]["mean_ms"] == 200.0

    def test_latency_percentiles_in_report(self) -> None:
        results = [
            {**_stub_result("baseline", "ds1"), "latency_ms": 100.0},
            {**_stub_result("baseline", "ds1"), "latency_ms": 200.0},
            {**_stub_result("sift", "ds1"), "latency_ms": 50.0},
            {**_stub_result("sift", "ds1"), "latency_ms": 150.0},
        ]
        report = build_report(results, model="test")
        lat = report["latency"]
        assert lat["baseline"]["count"] == 2
        assert lat["baseline"]["mean_ms"] == 150.0
        assert lat["sift"]["count"] == 2
        assert lat["sift"]["mean_ms"] == 100.0

    def test_difficulty_retries_tracked(self) -> None:
        results = [
            {
                **_stub_result("sift", "ds1", difficulty=2),
                "retries": 1,
            },
            {
                **_stub_result("sift", "ds1", difficulty=2),
                "retries": 2,
            },
            {
                **_stub_result("sift", "ds1", difficulty=1),
                "retries": 0,
            },
        ]
        report = build_report(results, model="test")
        pd = report["per_difficulty"]
        assert pd["2"]["sift_retries"] == 3
        assert pd["1"]["sift_retries"] == 0

    def test_latency_empty_results(self) -> None:
        report = build_report([], model="test")
        lat = report["latency"]
        assert lat["baseline"] == {}
        assert lat["sift"] == {}
        assert report["per_difficulty"] == {}


# -- question_set_hash --


class TestQuestionSetHash:
    def test_deterministic(self) -> None:
        h1 = question_set_hash()
        h2 = question_set_hash()
        assert h1 == h2

    def test_is_12_char_hex(self) -> None:
        h = question_set_hash()
        assert len(h) == 12
        int(h, 16)  # raises ValueError if not hex


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
