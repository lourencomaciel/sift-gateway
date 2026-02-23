"""Unit tests for benchmarks.tier1.code_extract."""

from __future__ import annotations

from benchmarks.tier1.code_extract import (
    extract_code,
    extract_root_path_comment,
)

# -- extract_code --


class TestExtractCode:
    def test_python_fence(self) -> None:
        text = (
            "Here is the code:\n"
            "```python\n"
            "def run(data, schema, params):\n"
            "  return 42\n"
            "```"
        )
        result = extract_code(text)
        assert "def run" in result.code
        assert "return 42" in result.code
        assert result.had_fences is True
        assert result.has_entrypoint is True

    def test_generic_fence_fallback(self) -> None:
        text = "```\ndef run(data, schema, params):\n  return 99\n```"
        result = extract_code(text)
        assert "def run" in result.code
        assert "return 99" in result.code
        assert result.had_fences is True

    def test_raw_text_fallback(self) -> None:
        text = "def run(data, schema, params):\n  return 1"
        result = extract_code(text)
        assert "def run" in result.code
        assert result.had_fences is False
        assert result.has_entrypoint is True

    def test_prefers_candidate_with_def_run(self) -> None:
        text = (
            "```python\nprint('hello')\n```\n\n"
            "def run(data, schema, params):\n  return 5"
        )
        result = extract_code(text)
        assert "def run" in result.code

    def test_no_def_run_uses_first_candidate(self) -> None:
        text = "```python\nresult = 42\n```"
        result = extract_code(text)
        assert "result = 42" in result.code
        assert result.has_entrypoint is False

    def test_multiple_python_fences_uses_first(self) -> None:
        text = (
            "```python\n"
            "def run(data, schema, params):\n  return 1\n"
            "```\n"
            "```python\n"
            "def run(data, schema, params):\n  return 2\n"
            "```"
        )
        result = extract_code(text)
        assert "return 1" in result.code

    def test_empty_text(self) -> None:
        result = extract_code("")
        assert result.code == ""
        assert result.has_entrypoint is False


# -- extract_root_path_comment --


class TestExtractRootPathComment:
    def test_parses_root_path_comment(self) -> None:
        text = (
            "# root_path: $.features\n"
            "def run(data, schema, params):\n  return 1"
        )
        result = extract_root_path_comment(text, ["$.features", "$.metadata"])
        assert result == "$.features"

    def test_returns_none_when_not_in_available(self) -> None:
        text = (
            "# root_path: $.unknown\ndef run(data, schema, params):\n  return 1"
        )
        result = extract_root_path_comment(text, ["$.features"])
        assert result is None

    def test_returns_none_when_no_comment(self) -> None:
        text = "def run(data, schema, params):\n  return 1"
        result = extract_root_path_comment(text, ["$.features"])
        assert result is None

    def test_picks_first_valid_line(self) -> None:
        text = (
            "# root_path: $.invalid\n"
            "# root_path: $.valid\n"
            "def run(data, schema, params):\n  return 1"
        )
        result = extract_root_path_comment(text, ["$.valid"])
        assert result == "$.valid"

    def test_dollar_root(self) -> None:
        text = "# root_path: $\ndef run(data, schema, params):\n  return 1"
        result = extract_root_path_comment(text, ["$"])
        assert result == "$"
