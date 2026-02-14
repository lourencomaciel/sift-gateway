#!/usr/bin/env python3
"""Validate that key documentation stays aligned with runtime contracts.

Checks:
- `docs/config.md` mentions all `GatewayConfig` and `UpstreamConfig` fields.
- `README.md` documents the consolidated artifact query contract.
- `README.md` does not mention removed `_gateway_context.cache_mode`.
- `docs/observability.md` lists all `LogEvents` event values.
- `docs/errors.md` includes required core gateway error codes.
"""

from __future__ import annotations

from pathlib import Path

from sift_mcp.config.settings import GatewayConfig, UpstreamConfig
from sift_mcp.obs.logging import LogEvents

ROOT = Path(__file__).resolve().parents[1]
README_PATH = ROOT / "README.md"
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


def main() -> int:
    """Run consistency checks and return process exit code."""
    failures: list[str] = []

    readme = _read_text(README_PATH)
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

    required_readme_snippets = [
        'action="query"',
        'query_kind="describe|get|select|search"',
        "_gateway_context.allow_reuse",
    ]
    for snippet in required_readme_snippets:
        if snippet not in readme:
            failures.append(f"README.md missing required contract snippet: {snippet}")

    forbidden_readme_snippets = [
        "_gateway_context.cache_mode",
    ]
    for snippet in forbidden_readme_snippets:
        if snippet in readme:
            failures.append(f"README.md contains removed contract snippet: {snippet}")

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
        if f"`{code}`" not in errors_doc:
            failures.append(f"docs/errors.md missing core error code: {code}")

    for event_value in _iter_log_event_values():
        if f"`{event_value}`" not in observability_doc:
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
