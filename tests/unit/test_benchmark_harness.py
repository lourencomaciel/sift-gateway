"""Unit tests for benchmark harness helpers."""

from __future__ import annotations

import json
from unittest.mock import patch

from benchmarks.common.baseline import (
    _effective_max_bytes,
    _fits,
    _truncate_dict,
    _truncate_list,
    truncate_for_baseline,
)
from benchmarks.common.llm_client import LLMAPIError, LLMResponse
from benchmarks.common.questions import Question
from benchmarks.common.sift_runtime import CodeExecutionError
from benchmarks.tier1.harness import (
    _make_result,
    _run_baseline,
    _run_sift,
)
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
        # 100_000 bytes < 200_000 * 2 = 400_000
        assert _effective_max_bytes(100_000, 200_000) == 100_000

    def test_token_derived_wins_when_smaller(self) -> None:
        # 500_000 bytes > 100_000 * 2 = 200_000
        assert _effective_max_bytes(500_000, 100_000) == 200_000

    def test_equal_caps(self) -> None:
        assert _effective_max_bytes(200_000, 100_000) == 200_000

    def test_default_caps_token_derived_wins(self) -> None:
        # With defaults (400K bytes, 180K tokens), the token-
        # derived cap (360K) is smaller and should be effective.
        assert _effective_max_bytes(400_000, 180_000) == 360_000


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


# -- truncate_for_baseline --


class TestTruncateForBaseline:
    def test_no_truncation_when_small(self) -> None:
        data = [1, 2, 3]
        text, truncated = truncate_for_baseline(
            data, max_bytes=1_000_000, max_tokens=500_000
        )
        assert not truncated
        assert json.loads(text) == [1, 2, 3]

    def test_truncates_large_list(self) -> None:
        data = list(range(100_000))
        text, truncated = truncate_for_baseline(
            data, max_bytes=1_000, max_tokens=500_000
        )
        assert truncated
        parsed = json.loads(text)
        assert isinstance(parsed, list)
        assert len(parsed) < 100_000

    def test_token_limit_caps_before_byte_limit(self) -> None:
        # With max_tokens=100 and 2 bytes/token, effective limit = 200
        data = list(range(10_000))
        text, truncated = truncate_for_baseline(
            data, max_bytes=1_000_000, max_tokens=100
        )
        assert truncated
        assert len(text.encode("utf-8")) <= 200

    def test_truncates_dict_with_arrays(self) -> None:
        data = {"vals": list(range(10_000))}
        text, truncated = truncate_for_baseline(
            data, max_bytes=500, max_tokens=500_000
        )
        assert truncated
        parsed = json.loads(text)
        assert isinstance(parsed, dict)
        assert "vals" in parsed
        assert len(parsed["vals"]) < 10_000

    def test_fallback_note_for_dict_without_arrays(self) -> None:
        data = {"a": "x" * 10_000}
        text, truncated = truncate_for_baseline(
            data, max_bytes=100, max_tokens=500_000
        )
        assert truncated
        parsed = json.loads(text)
        assert parsed["_truncated"] is True


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


# -- _run_baseline error handling --


class TestRunBaselineError:
    """Verify _run_baseline error handling with narrowed exceptions."""

    def test_llm_api_error_caught_as_failed_result(self) -> None:
        q = _stub_question()
        with patch(
            "benchmarks.tier1.harness.call_llm",
            side_effect=LLMAPIError("rate limited"),
        ):
            result = _run_baseline(
                q,
                [1, 2, 3],
                model="test",
                api_key="k",
                temperature=0.0,
                max_baseline_bytes=1_000_000,
                max_baseline_tokens=500_000,
            )
        assert result["attempted"] is False
        assert "rate limited" in result["error"]
        assert result["condition"] == "baseline"

    def test_non_llm_exception_propagates(self) -> None:
        q = _stub_question()
        with (
            patch(
                "benchmarks.tier1.harness.call_llm",
                side_effect=ValueError("bad config"),
            ),
            pytest.raises(ValueError, match="bad config"),
        ):
            _run_baseline(
                q,
                [1, 2, 3],
                model="test",
                api_key="k",
                temperature=0.0,
                max_baseline_bytes=1_000_000,
                max_baseline_tokens=500_000,
            )


# -- _run_sift answer extraction error --


class TestRunSiftAnswerError:
    """Verify LLMAPIError during answer extraction is recorded."""

    def test_answer_llm_error_recorded(self) -> None:
        q = _stub_question()
        call_count = 0

        def llm_side_effect(**_kw: object) -> LLMResponse:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                # Codegen call succeeds.
                return _llm_resp()
            # Answer extraction call fails.
            raise LLMAPIError("answer rate limited")

        with (
            patch(
                "benchmarks.tier1.harness.call_llm",
                side_effect=llm_side_effect,
            ),
            patch(
                "benchmarks.tier1.harness.execute_code",
                return_value={"items": [42]},
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
        assert "answer extraction failed" in result["error"]
        assert result["condition"] == "sift"
