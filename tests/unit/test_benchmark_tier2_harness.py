"""Unit tests for Tier 2 benchmark harness."""

from __future__ import annotations

from benchmarks.tier2.harness import _build_parser


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
