"""Unit tests for benchmark LLM client helpers."""

from __future__ import annotations

from benchmarks.common.llm_client import (
    detect_provider,
    resolve_api_key,
)
import pytest


class TestDetectProvider:
    def test_claude_model(self) -> None:
        assert detect_provider("claude-sonnet-4-6") == "anthropic"

    def test_claude_haiku(self) -> None:
        assert detect_provider("claude-haiku-4-5-20251001") == "anthropic"

    def test_gpt_model(self) -> None:
        assert detect_provider("gpt-4o") == "openai"

    def test_o1_model(self) -> None:
        assert detect_provider("o1-preview") == "openai"

    def test_unknown_defaults_to_openai(self) -> None:
        assert detect_provider("some-other-model") == "openai"


class TestResolveApiKey:
    def test_explicit_key_takes_priority(self) -> None:
        key = resolve_api_key(provider="anthropic", api_key="explicit-key")
        assert key == "explicit-key"

    def test_anthropic_env_var(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ANTHROPIC_API_KEY", "env-anth-key")
        key = resolve_api_key(provider="anthropic", api_key=None)
        assert key == "env-anth-key"

    def test_openai_env_var(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("OPENAI_API_KEY", "env-oai-key")
        key = resolve_api_key(provider="openai", api_key=None)
        assert key == "env-oai-key"

    def test_missing_key_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        monkeypatch.delenv("OPENAI_API_KEY", raising=False)
        with pytest.raises(ValueError, match="No API key"):
            resolve_api_key(provider="anthropic", api_key=None)

    def test_empty_string_key_raises(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("ANTHROPIC_API_KEY", "")
        with pytest.raises(ValueError, match="No API key"):
            resolve_api_key(provider="anthropic", api_key=None)

    def test_explicit_key_overrides_env(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("OPENAI_API_KEY", "env-key")
        key = resolve_api_key(provider="openai", api_key="override")
        assert key == "override"
