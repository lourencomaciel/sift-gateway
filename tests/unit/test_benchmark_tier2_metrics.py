"""Unit tests for Tier 2 metrics and reporting."""

from __future__ import annotations

from benchmarks.tier2.agent_loop import AgentResult, TurnMetrics
from benchmarks.tier2.metrics import (
    _latency_percentiles,
    build_question_metrics,
    build_report,
)


class TestLatencyPercentiles:
    def test_empty_list(self) -> None:
        assert _latency_percentiles([]) == {}

    def test_single_value(self) -> None:
        result = _latency_percentiles([100.0])
        assert result["p50_ms"] == 100.0
        assert result["p90_ms"] == 100.0
        assert result["mean_ms"] == 100.0
        assert result["count"] == 1

    def test_multiple_values(self) -> None:
        result = _latency_percentiles([100.0, 200.0, 300.0, 400.0, 500.0])
        assert result["p50_ms"] == 300.0
        assert result["count"] == 5


class TestBuildQuestionMetrics:
    def test_basic_metrics(self) -> None:
        agent_result = AgentResult(
            answer="42",
            turns=3,
            max_turns_reached=False,
            token_budget_reached=False,
            total_input_tokens=1000,
            total_output_tokens=200,
            total_latency_ms=5000.0,
            tool_call_counts={
                "mirrored": 1,
                "code_query": 2,
            },
            code_query_attempts=2,
            code_query_errors=1,
            pages_fetched=0,
            turn_metrics=[
                TurnMetrics(
                    input_tokens=300,
                    output_tokens=60,
                    latency_ms=1500.0,
                ),
                TurnMetrics(
                    input_tokens=400,
                    output_tokens=80,
                    latency_ms=2000.0,
                ),
                TurnMetrics(
                    input_tokens=300,
                    output_tokens=60,
                    latency_ms=1500.0,
                ),
            ],
        )

        metrics = build_question_metrics(
            agent_result=agent_result,
            question_id="eq_count",
            dataset_name="earthquakes",
            question_type="count",
            difficulty=1,
            gold_answer="42",
            llm_answer="42",
            correct=True,
        )

        assert metrics["question_id"] == "eq_count"
        assert metrics["correct"] is True
        assert metrics["turns"] == 3
        assert metrics["input_tokens"] == 1000
        assert metrics["code_query_errors"] == 1
        assert metrics["total_tool_calls"] == 3
        assert len(metrics["per_turn"]) == 3


class TestBuildReport:
    def _make_result(
        self,
        *,
        correct: bool = True,
        dataset: str = "earthquakes",
        question_type: str = "count",
        difficulty: int = 1,
        turns: int = 3,
        input_tokens: int = 1000,
        output_tokens: int = 200,
        latency_ms: float = 5000.0,
        code_query_errors: int = 0,
        code_query_attempts: int = 1,
        pages_fetched: int = 0,
    ) -> dict:
        return {
            "question_id": "q1",
            "dataset": dataset,
            "question_type": question_type,
            "difficulty": difficulty,
            "gold_answer": "42",
            "llm_answer": "42" if correct else "wrong",
            "correct": correct,
            "turns": turns,
            "max_turns_reached": False,
            "token_budget_reached": False,
            "tool_calls": {"mirrored": 1, "code_query": 1},
            "total_tool_calls": 2,
            "code_query_attempts": code_query_attempts,
            "code_query_errors": code_query_errors,
            "pages_fetched": pages_fetched,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "latency_ms": latency_ms,
            "per_turn": [],
        }

    def test_summary_accuracy(self) -> None:
        results = [
            self._make_result(correct=True),
            self._make_result(correct=True),
            self._make_result(correct=False),
        ]
        report = build_report(results, model="test")
        assert report["summary"]["accuracy"] == "2/3"
        assert report["summary"]["accuracy_pct"] == 66.7

    def test_per_dataset(self) -> None:
        results = [
            self._make_result(dataset="earthquakes", correct=True),
            self._make_result(dataset="earthquakes", correct=False),
            self._make_result(dataset="products", correct=True),
        ]
        report = build_report(results, model="test")
        assert report["per_dataset"]["earthquakes"]["correct"] == 1
        assert report["per_dataset"]["earthquakes"]["total"] == 2
        assert report["per_dataset"]["products"]["correct"] == 1

    def test_per_question_type(self) -> None:
        results = [
            self._make_result(question_type="count"),
            self._make_result(question_type="filter"),
        ]
        report = build_report(results, model="test")
        assert report["per_question_type"]["count"]["total"] == 1
        assert report["per_question_type"]["filter"]["total"] == 1

    def test_per_difficulty(self) -> None:
        results = [
            self._make_result(difficulty=1),
            self._make_result(difficulty=3, code_query_errors=2),
        ]
        report = build_report(results, model="test")
        assert report["per_difficulty"]["1"]["total"] == 1
        assert report["per_difficulty"]["3"]["code_errors"] == 2

    def test_tool_call_distribution(self) -> None:
        results = [self._make_result()]
        report = build_report(results, model="test")
        dist = report["summary"]["tool_call_distribution"]
        assert dist["mirrored"] == 1
        assert dist["code_query"] == 1

    def test_code_retry_rate(self) -> None:
        results = [
            self._make_result(
                code_query_attempts=4,
                code_query_errors=2,
            ),
        ]
        report = build_report(results, model="test")
        assert report["summary"]["code_retry_rate"] == 0.5

    def test_pagination_count(self) -> None:
        results = [
            self._make_result(pages_fetched=0),
            self._make_result(pages_fetched=3),
            self._make_result(pages_fetched=1),
        ]
        report = build_report(results, model="test")
        assert report["summary"]["pagination_questions"] == 2

    def test_empty_results(self) -> None:
        report = build_report([], model="test")
        assert report["summary"]["accuracy"] == "0/0"
        assert report["summary"]["accuracy_pct"] == 0

    def test_latency_in_report(self) -> None:
        results = [
            self._make_result(latency_ms=1000.0),
            self._make_result(latency_ms=2000.0),
        ]
        report = build_report(results, model="test")
        assert report["latency"]["count"] == 2
