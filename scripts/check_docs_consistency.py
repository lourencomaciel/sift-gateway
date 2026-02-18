#!/usr/bin/env python3
"""Validate that key documentation stays aligned with runtime contracts.

Checks:
- `docs/config.md` mentions all `GatewayConfig` and `UpstreamConfig` fields.
- `docs/api_contracts.md` documents the consolidated artifact query contract.
- `docs/api_contracts.md` defines response shapes by `query_kind`.
- `docs/api_contracts.md` does not mention removed `_gateway_context.cache_mode`.
- `docs/api_contracts.md` does not include unsupported `BUDGET_EXCEEDED` code.
- `docs/recipes.md` does not claim code-query result caching behavior.
- `README.md` documents `query_kind="search"` as current behavior, not future work.
- `docs/observability.md` lists all `LogEvents` event values.
- `docs/errors.md` includes required core gateway error codes.
"""

from __future__ import annotations

from pathlib import Path
import re

from sift_mcp.config.settings import GatewayConfig, UpstreamConfig
from sift_mcp.obs.logging import LogEvents

ROOT = Path(__file__).resolve().parents[1]
README_PATH = ROOT / "README.md"
API_CONTRACTS_PATH = ROOT / "docs" / "api_contracts.md"
RECIPES_DOC_PATH = ROOT / "docs" / "recipes.md"
CONFIG_DOC_PATH = ROOT / "docs" / "config.md"
ERRORS_DOC_PATH = ROOT / "docs" / "errors.md"
OBS_DOC_PATH = ROOT / "docs" / "observability.md"


def _read_text(path: Path) -> str:
    if not path.exists():
        raise FileNotFoundError(f"missing required doc file: {path}")
    return path.read_text(encoding="utf-8")


def _iter_log_event_values() -> list[str]:
    values: list[str] = []
    for name, value in vars(LogEvents).items():
        if name.startswith("_"):
            continue
        if not name.isupper():
            continue
        if isinstance(value, str):
            values.append(value)
    return sorted(values)


def _missing_field_tokens(text: str, fields: list[str]) -> list[str]:
    missing: list[str] = []
    for field in fields:
        if f"`{field}`" not in text:
            missing.append(field)
    return missing


def _normalize_whitespace(text: str) -> str:
    """Collapse whitespace and lower-case text for robust phrase matching."""
    return re.sub(r"\s+", " ", text).strip().casefold()


def _contains_phrase(text: str, phrase: str) -> bool:
    """Return True when phrase appears in text, ignoring case/whitespace."""
    return _normalize_whitespace(phrase) in _normalize_whitespace(text)


def _has_query_kind_token(text: str, kind: str) -> bool:
    """Return True when text contains ``query_kind="<kind>"``."""
    pattern = rf'query_kind\s*=\s*"{re.escape(kind)}"'
    return re.search(pattern, text, flags=re.IGNORECASE) is not None


def _has_query_kind_heading(text: str, kind: str) -> bool:
    """Return True when text has a markdown heading for ``query_kind``."""
    pattern = (
        rf'^#{{2,6}}\s+`?query_kind\s*=\s*"{re.escape(kind)}"`?'
    )
    return re.search(pattern, text, flags=re.IGNORECASE | re.MULTILINE) is not None


def main() -> int:
    """Run consistency checks and return process exit code."""
    failures: list[str] = []

    readme = _read_text(README_PATH)
    api_contracts_doc = _read_text(API_CONTRACTS_PATH)
    recipes_doc = _read_text(RECIPES_DOC_PATH)
    config_doc = _read_text(CONFIG_DOC_PATH)
    errors_doc = _read_text(ERRORS_DOC_PATH)
    observability_doc = _read_text(OBS_DOC_PATH)

    gateway_fields = sorted(GatewayConfig.model_fields.keys())
    missing_gateway = _missing_field_tokens(config_doc, gateway_fields)
    if missing_gateway:
        failures.append(
            "docs/config.md missing GatewayConfig keys: "
            + ", ".join(missing_gateway)
        )

    upstream_fields = sorted(UpstreamConfig.model_fields.keys())
    missing_upstream = _missing_field_tokens(config_doc, upstream_fields)
    if missing_upstream:
        failures.append(
            "docs/config.md missing UpstreamConfig keys: "
            + ", ".join(missing_upstream)
        )

    required_api_contract_phrases = [
        'action="query"',
        "_gateway_context.session_id",
    ]
    for phrase in required_api_contract_phrases:
        if not _contains_phrase(api_contracts_doc, phrase):
            failures.append(
                "docs/api_contracts.md missing required contract phrase: "
                f"{phrase}"
            )

    query_kinds = ["describe", "get", "select", "search", "code"]
    for kind in query_kinds:
        if not _has_query_kind_token(api_contracts_doc, kind):
            failures.append(
                "docs/api_contracts.md missing required query_kind token: "
                f'{kind}'
            )

    for kind in query_kinds:
        if not _has_query_kind_heading(api_contracts_doc, kind):
            failures.append(
                "docs/api_contracts.md missing response-shape section: "
                f'query_kind="{kind}"'
            )

    forbidden_api_contract_snippets = [
        "_gateway_context.cache_mode",
        "BUDGET_EXCEEDED",
        "return a consistent response format",
    ]
    for snippet in forbidden_api_contract_snippets:
        if _contains_phrase(api_contracts_doc, snippet):
            failures.append(
                "docs/api_contracts.md contains removed contract snippet: "
                f"{snippet}"
            )

    forbidden_recipes_snippets = [
        "may return cached results",
    ]
    for snippet in forbidden_recipes_snippets:
        if _contains_phrase(recipes_doc, snippet):
            failures.append(
                "docs/recipes.md contains unsupported behavior claim: "
                f"{snippet}"
            )

    if "query_kind" not in _normalize_whitespace(readme):
        failures.append("README.md missing query_kind=search guidance")
    if "`search`" not in readme:
        failures.append("README.md missing search query kind guidance")
    if "session artifacts" not in _normalize_whitespace(readme):
        failures.append("README.md missing session-artifact search guidance")

    forbidden_readme_snippets = [
        "(future) Search within artifact content",
    ]
    for snippet in forbidden_readme_snippets:
        if _contains_phrase(readme, snippet):
            failures.append(
                "README.md contains outdated query_kind=search guidance: "
                f"{snippet}"
            )

    required_error_codes = [
        "INVALID_ARGUMENT",
        "NOT_FOUND",
        "GONE",
        "RESOURCE_EXHAUSTED",
        "NOT_IMPLEMENTED",
        "CURSOR_EXPIRED",
        "CURSOR_STALE",
        "INTERNAL",
    ]
    for code in required_error_codes:
        if re.search(rf"\b{re.escape(code)}\b", errors_doc) is None:
            failures.append(f"docs/errors.md missing core error code: {code}")

    for event_value in _iter_log_event_values():
        if event_value not in observability_doc:
            failures.append(
                "docs/observability.md missing log event value: "
                f"{event_value}"
            )

    if failures:
        print("docs consistency check failed:")
        for failure in failures:
            print(f"- {failure}")
        return 1

    print("docs consistency check passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
