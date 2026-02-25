"""Unit tests for Tier 2 benchmark harness."""

from __future__ import annotations

import argparse
from unittest.mock import patch

from benchmarks.common.llm_client import LLMAPIError, LLMResponse
from benchmarks.tier2.harness import (
    _build_parser,
    _run_baseline_across_datasets,
    _run_benchmark,
)
import pytest


class TestBuildParser:
    def test_default_model(self) -> None:
        args = _build_parser().parse_args([])
        assert args.model == "claude-sonnet-4-6"

    def test_default_max_turns(self) -> None:
        args = _build_parser().parse_args([])
        assert args.max_turns == 15

    def test_default_max_pages(self) -> None:
        args = _build_parser().parse_args([])
        assert args.max_pages == 10

    def test_default_max_input_tokens(self) -> None:
        args = _build_parser().parse_args([])
        assert args.max_input_tokens == 200_000

    def test_default_temperature(self) -> None:
        args = _build_parser().parse_args([])
        assert args.temperature == 0.0

    def test_custom_model(self) -> None:
        args = _build_parser().parse_args(["--model", "claude-opus-4-6"])
        assert args.model == "claude-opus-4-6"

    def test_custom_max_turns(self) -> None:
        args = _build_parser().parse_args(["--max-turns", "20"])
        assert args.max_turns == 20

    def test_datasets_filter(self) -> None:
        args = _build_parser().parse_args(
            ["--datasets", "earthquakes", "products"]
        )
        assert args.datasets == ["earthquakes", "products"]

    def test_questions_filter(self) -> None:
        args = _build_parser().parse_args(["--questions", "eq_count_total"])
        assert args.questions == ["eq_count_total"]

    def test_json_flag(self) -> None:
        args = _build_parser().parse_args(["--json"])
        assert args.json is True

    def test_save_conversations_flag(self) -> None:
        args = _build_parser().parse_args(["--save-conversations"])
        assert args.save_conversations is True

    def test_continue_on_error_flag(self) -> None:
        args = _build_parser().parse_args(["--continue-on-error"])
        assert args.continue_on_error is True

    def test_api_key(self) -> None:
        args = _build_parser().parse_args(["--api-key", "test-key"])
        assert args.api_key == "test-key"

    def test_default_api_key_none(self) -> None:
        args = _build_parser().parse_args([])
        assert args.api_key is None

    def test_skip_baseline_flag(self) -> None:
        args = _build_parser().parse_args(["--skip-baseline"])
        assert args.skip_baseline is True

    def test_skip_baseline_default_false(self) -> None:
        args = _build_parser().parse_args([])
        assert args.skip_baseline is False

    def test_skip_sift_flag(self) -> None:
        args = _build_parser().parse_args(["--skip-sift"])
        assert args.skip_sift is True

    def test_skip_sift_default_false(self) -> None:
        args = _build_parser().parse_args([])
        assert args.skip_sift is False

    def test_max_baseline_payload_bytes_default(self) -> None:
        args = _build_parser().parse_args([])
        assert args.max_baseline_payload_bytes == 400_000

    def test_max_baseline_payload_bytes_custom(self) -> None:
        args = _build_parser().parse_args(
            ["--max-baseline-payload-bytes", "200000"]
        )
        assert args.max_baseline_payload_bytes == 200_000

    def test_max_baseline_tokens_default(self) -> None:
        args = _build_parser().parse_args([])
        assert args.max_baseline_tokens == 180_000

    def test_max_baseline_tokens_custom(self) -> None:
        args = _build_parser().parse_args(["--max-baseline-tokens", "100000"])
        assert args.max_baseline_tokens == 100_000


class TestSkipBothGuard:
    def test_skip_both_exits(self) -> None:
        args = _build_parser().parse_args(["--skip-baseline", "--skip-sift"])
        with pytest.raises(SystemExit, match="1"):
            _run_benchmark(args)


class TestRunBaselineAcrossDatasets:
    """Tests for _run_baseline_across_datasets with mocked LLM."""

    @staticmethod
    def _make_args(
        *,
        model: str = "test-model",
        api_key: str = "k",
        temperature: float = 0.0,
        max_baseline_payload_bytes: int = 400_000,
        max_baseline_tokens: int = 180_000,
        continue_on_error: bool = False,
    ) -> argparse.Namespace:
        return argparse.Namespace(
            model=model,
            api_key=api_key,
            temperature=temperature,
            max_baseline_payload_bytes=max_baseline_payload_bytes,
            max_baseline_tokens=max_baseline_tokens,
            continue_on_error=continue_on_error,
        )

    @patch("benchmarks.tier2.harness.call_llm")
    @patch("benchmarks.tier2.harness.get_questions_for_dataset")
    def test_correct_answer_recorded(
        self,
        mock_get_q,
        mock_llm,
    ) -> None:
        from benchmarks.common.questions import Question

        mock_get_q.return_value = [
            Question(
                dataset_name="earthquakes",
                question_id="eq_count",
                question_text="How many earthquakes?",
                question_type="count",
                gold_answer_fn=lambda d: str(len(d)),
                answer_type="number",
                difficulty=1,
            ),
        ]
        mock_llm.return_value = LLMResponse(
            text="3",
            input_tokens=500,
            output_tokens=10,
            model="test-model",
            latency_ms=100.0,
        )

        results: list[dict] = []
        _run_baseline_across_datasets(
            dataset_names=["earthquakes"],
            loaded={"earthquakes": [1, 2, 3]},
            results=results,
            question_filter=None,
            args=self._make_args(),
        )

        assert len(results) == 1
        assert results[0]["condition"] == "baseline"
        assert results[0]["correct"] is True
        assert results[0]["input_tokens"] == 500
        assert results[0]["truncated"] is False

    @patch("benchmarks.tier2.harness.call_llm")
    @patch("benchmarks.tier2.harness.get_questions_for_dataset")
    def test_wrong_answer_recorded(
        self,
        mock_get_q,
        mock_llm,
    ) -> None:
        from benchmarks.common.questions import Question

        mock_get_q.return_value = [
            Question(
                dataset_name="earthquakes",
                question_id="eq_count",
                question_text="How many earthquakes?",
                question_type="count",
                gold_answer_fn=lambda d: str(len(d)),
                answer_type="number",
                difficulty=1,
            ),
        ]
        mock_llm.return_value = LLMResponse(
            text="999",
            input_tokens=500,
            output_tokens=10,
            model="test-model",
            latency_ms=100.0,
        )

        results: list[dict] = []
        _run_baseline_across_datasets(
            dataset_names=["earthquakes"],
            loaded={"earthquakes": [1, 2, 3]},
            results=results,
            question_filter=None,
            args=self._make_args(),
        )

        assert len(results) == 1
        assert results[0]["correct"] is False

    @patch("benchmarks.tier2.harness.call_llm")
    @patch("benchmarks.tier2.harness.get_questions_for_dataset")
    def test_llm_error_continue_on_error(
        self,
        mock_get_q,
        mock_llm,
    ) -> None:
        from benchmarks.common.questions import Question

        mock_get_q.return_value = [
            Question(
                dataset_name="earthquakes",
                question_id="eq_count",
                question_text="How many earthquakes?",
                question_type="count",
                gold_answer_fn=lambda d: str(len(d)),
                answer_type="number",
                difficulty=1,
            ),
        ]
        mock_llm.side_effect = LLMAPIError("rate limit")

        results: list[dict] = []
        _run_baseline_across_datasets(
            dataset_names=["earthquakes"],
            loaded={"earthquakes": [1, 2, 3]},
            results=results,
            question_filter=None,
            args=self._make_args(continue_on_error=True),
        )

        assert len(results) == 1
        assert results[0]["correct"] is False
        assert "error" in results[0]
        assert results[0]["input_tokens"] == 0

    @patch("benchmarks.tier2.harness.call_llm")
    @patch("benchmarks.tier2.harness.get_questions_for_dataset")
    def test_llm_error_raises_without_continue(
        self,
        mock_get_q,
        mock_llm,
    ) -> None:
        from benchmarks.common.questions import Question

        mock_get_q.return_value = [
            Question(
                dataset_name="earthquakes",
                question_id="eq_count",
                question_text="How many earthquakes?",
                question_type="count",
                gold_answer_fn=lambda d: str(len(d)),
                answer_type="number",
                difficulty=1,
            ),
        ]
        mock_llm.side_effect = LLMAPIError("rate limit")

        results: list[dict] = []
        with pytest.raises(LLMAPIError, match="rate limit"):
            _run_baseline_across_datasets(
                dataset_names=["earthquakes"],
                loaded={"earthquakes": [1, 2, 3]},
                results=results,
                question_filter=None,
                args=self._make_args(continue_on_error=False),
            )

    @patch("benchmarks.tier2.harness.call_llm")
    @patch("benchmarks.tier2.harness.get_questions_for_dataset")
    def test_question_filter_respected(
        self,
        mock_get_q,
        mock_llm,
    ) -> None:
        from benchmarks.common.questions import Question

        mock_get_q.return_value = [
            Question(
                dataset_name="earthquakes",
                question_id="eq_count",
                question_text="How many earthquakes?",
                question_type="count",
                gold_answer_fn=lambda d: "3",
                answer_type="number",
                difficulty=1,
            ),
            Question(
                dataset_name="earthquakes",
                question_id="eq_max",
                question_text="Max magnitude?",
                question_type="aggregation",
                gold_answer_fn=lambda d: "5.0",
                answer_type="number",
                difficulty=2,
            ),
        ]
        mock_llm.return_value = LLMResponse(
            text="5.0",
            input_tokens=500,
            output_tokens=10,
            model="test-model",
            latency_ms=100.0,
        )

        results: list[dict] = []
        _run_baseline_across_datasets(
            dataset_names=["earthquakes"],
            loaded={"earthquakes": [1, 2, 3]},
            results=results,
            question_filter={"eq_max"},
            args=self._make_args(),
        )

        assert len(results) == 1
        assert results[0]["question_id"] == "eq_max"
