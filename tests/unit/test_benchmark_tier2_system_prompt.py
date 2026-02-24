"""Unit tests for Tier 2 system prompt."""

from __future__ import annotations

from benchmarks.tier2.system_prompt import SYSTEM_PROMPT, get_system_prompt


class TestSystemPrompt:
    def test_prompt_is_nonempty_string(self) -> None:
        assert isinstance(SYSTEM_PROMPT, str)
        assert len(SYSTEM_PROMPT) > 100

    def test_get_system_prompt_returns_constant(self) -> None:
        assert get_system_prompt() is SYSTEM_PROMPT

    def test_mentions_artifact_tool(self) -> None:
        assert "artifact" in SYSTEM_PROMPT

    def test_mentions_code_query(self) -> None:
        assert "query_kind" in SYSTEM_PROMPT
        assert "code" in SYSTEM_PROMPT

    def test_mentions_def_run_signature(self) -> None:
        assert "def run(data, schema, params)" in SYSTEM_PROMPT

    def test_mentions_next_page(self) -> None:
        assert "next_page" in SYSTEM_PROMPT

    def test_mentions_schema_ref(self) -> None:
        assert "schema" in SYSTEM_PROMPT.lower()

    def test_mentions_answer_format(self) -> None:
        assert "ONLY the final answer" in SYSTEM_PROMPT
