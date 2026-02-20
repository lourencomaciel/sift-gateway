from __future__ import annotations

import json
import shlex

from sift_gateway.tools.usage_hint import (
    PAGINATION_COMPLETENESS_RULE,
    available_code_query_packages,
    build_code_query_usage,
    compact_schema_primary_root_path,
    render_code_query_usage_hint,
    summarize_code_query_packages,
    with_pagination_completeness_rule,
)


def test_rule_constant_is_stable() -> None:
    assert (
        PAGINATION_COMPLETENESS_RULE
        == "Do not claim completeness until pagination.retrieval_status == COMPLETE."
    )


def test_with_rule_returns_rule_for_blank_text() -> None:
    assert with_pagination_completeness_rule("") == PAGINATION_COMPLETENESS_RULE
    assert (
        with_pagination_completeness_rule("   ")
        == PAGINATION_COMPLETENESS_RULE
    )


def test_with_rule_appends_rule_once() -> None:
    base = "Continue with next_page when has_next_page is true."
    with_rule = with_pagination_completeness_rule(base)
    assert with_rule.endswith(PAGINATION_COMPLETENESS_RULE)
    assert with_rule.count(PAGINATION_COMPLETENESS_RULE) == 1


def test_with_rule_is_idempotent() -> None:
    once = with_pagination_completeness_rule("Hint text.")
    twice = with_pagination_completeness_rule(once)
    assert twice == once


def test_available_code_query_packages_filters_unimportable_roots(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "sift_gateway.tools.usage_hint._is_importable_root",
        lambda root: root in {"jmespath", "numpy"},
    )
    packages = available_code_query_packages(
        configured_roots=["json", "jmespath", "numpy", "pandas"],
    )
    assert packages == ["jmespath", "numpy"]


def test_summarize_code_query_packages_compacts_output(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "sift_gateway.tools.usage_hint._is_importable_root",
        lambda root: root in {"jmespath", "numpy", "pandas", "scipy", "matplotlib"},
    )
    summary = summarize_code_query_packages(
        configured_roots=[
            "json",
            "jmespath",
            "numpy",
            "pandas",
            "scipy",
            "matplotlib",
        ],
        max_items=3,
    )
    assert summary == "jmespath,matplotlib,numpy,+2"


def test_summarize_code_query_packages_reports_none_when_no_imports_allowed() -> (
    None
):
    summary = summarize_code_query_packages(configured_roots=[])
    assert summary == "none"


def test_summarize_code_query_packages_reports_none_when_unimportable_only(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "sift_gateway.tools.usage_hint._is_importable_root",
        lambda _root: False,
    )
    summary = summarize_code_query_packages(configured_roots=["scipy"])
    assert summary == "none"


def test_summarize_code_query_packages_reports_stdlib_when_only_stdlib_allowed(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "sift_gateway.tools.usage_hint._is_importable_root",
        lambda _root: False,
    )
    summary = summarize_code_query_packages(configured_roots=["json", "math"])
    assert summary == "stdlib-only"


def test_compact_schema_primary_root_path_defaults_to_root() -> None:
    assert compact_schema_primary_root_path(None) == "$"
    assert compact_schema_primary_root_path([]) == "$"


def test_compact_schema_primary_root_path_uses_first_schema() -> None:
    root_path = compact_schema_primary_root_path(
        [{"rp": "$.items"}, {"rp": "$.other"}]
    )
    assert root_path == "$.items"


def test_build_code_query_usage_mcp_mentions_packages(monkeypatch) -> None:
    monkeypatch.setattr(
        "sift_gateway.tools.usage_hint._is_importable_root",
        lambda root: root in {"numpy", "jmespath"},
    )
    usage = build_code_query_usage(
        interface="mcp",
        artifact_id="art_1",
        root_path="$.items",
        configured_roots=["json", "numpy", "jmespath"],
    )
    assert usage["interface"] == "mcp"
    assert usage["query_kind"] == "code"
    assert usage["packages"] == "jmespath,numpy"
    assert 'artifact_id="art_1"' in usage["example"]
    assert 'root_path="$.items"' in usage["example"]
    assert usage["entrypoint_single"] == "run(data, schema, params)"
    assert usage["entrypoint_multi"] == "run(artifacts, schemas, params)"
    assert usage["multi_input_shape"] == "dict[artifact_id -> list[dict]]"
    assert "scope=single" in usage["strategy"]


def test_build_code_query_usage_mcp_escapes_root_path_quotes(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "sift_gateway.tools.usage_hint._is_importable_root",
        lambda root: root in {"jmespath"},
    )
    root_path = "$['a\"b']"
    usage = build_code_query_usage(
        interface="mcp",
        artifact_id="art_1",
        root_path=root_path,
        configured_roots=["json", "jmespath"],
    )
    assert (
        f"root_path={json.dumps(root_path, ensure_ascii=False)}"
        in usage["example"]
    )


def test_build_code_query_usage_cli_mentions_packages(monkeypatch) -> None:
    monkeypatch.setattr(
        "sift_gateway.tools.usage_hint._is_importable_root",
        lambda root: root in {"pandas"},
    )
    usage = build_code_query_usage(
        interface="cli",
        artifact_id="art_1",
        root_path="$.items",
        configured_roots=["json", "pandas"],
    )
    assert usage["interface"] == "cli"
    assert usage["packages"] == "pandas"
    assert usage["entrypoint_single"] == "run(data, schema, params)"
    assert usage["entrypoint_multi"] == "run(artifacts, schemas, params)"
    assert usage["multi_input_shape"] == "dict[artifact_id -> list[dict]]"
    assert (
        usage["example"]
        == 'sift-gateway code art_1 \'$.items\' --code "def run(data, schema, params): return len(data)"'
    )


def test_build_code_query_usage_cli_shell_quotes_root_path(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "sift_gateway.tools.usage_hint._is_importable_root",
        lambda root: root in {"pandas"},
    )
    root_path = "$['owner\\'s']"
    usage = build_code_query_usage(
        interface="cli",
        artifact_id="art_1",
        root_path=root_path,
        configured_roots=["json", "pandas"],
    )
    assert shlex.quote(root_path) in usage["example"]


def test_render_code_query_usage_hint_cli() -> None:
    hint = render_code_query_usage_hint(
        {
            "interface": "cli",
            "example": 'sift-gateway code art_1 \'$.items\' --code "def run(data, schema, params): return len(data)"',
            "packages": "pandas",
        }
    )
    assert (
        hint
        == "use `sift-gateway code art_1 '$.items' --code \"def run(data, schema, params): return len(data)\"`; pkgs: pandas"
    )


def test_render_code_query_usage_hint_mcp() -> None:
    hint = render_code_query_usage_hint(
        {
            "interface": "mcp",
            "example": 'artifact(action=\"query\", query_kind=\"code\", artifact_id=\"art_1\", root_path=\"$.items\", code=\"def run(data, schema, params): ...\", params={})',
            "packages": "jmespath,numpy",
        }
    )
    assert "artifact(action=" in hint
    assert "; pkgs: jmespath,numpy" in hint
