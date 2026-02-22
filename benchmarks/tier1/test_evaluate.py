"""Unit tests for benchmark evaluation functions."""

from __future__ import annotations

import pytest

from benchmarks.tier1.evaluate import (
    evaluate_answer,
    match_list,
    match_number,
    match_string,
)

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

    def test_relative_tolerance_accepts_within_range(self) -> None:
        # 1% of 200 = 2, so 201 is within range.
        assert match_number("201", "200", tolerance=0.01)

    def test_relative_tolerance_rejects_outside_range(self) -> None:
        # 1% of 200 = 2, so 203 is outside.
        assert not match_number("203", "200", tolerance=0.01)

    def test_zero_gold_uses_absolute(self) -> None:
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


# -- evaluate_answer --


class TestEvaluateAnswer:
    def test_routes_number(self) -> None:
        assert evaluate_answer("42", "42", answer_type="number")
        assert not evaluate_answer("43", "42", answer_type="number")

    def test_routes_string(self) -> None:
        assert evaluate_answer("Paris", "Paris", answer_type="string")
        assert not evaluate_answer("London", "Paris", answer_type="string")

    def test_routes_list(self) -> None:
        assert evaluate_answer('["a", "b"]', '["b", "a"]', answer_type="list")

    def test_tolerance_forwarded(self) -> None:
        assert evaluate_answer(
            "201", "200", answer_type="number", tolerance=0.01
        )
        assert not evaluate_answer(
            "201", "200", answer_type="number", tolerance=0.0
        )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
