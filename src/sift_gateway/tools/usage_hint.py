"""Shared pagination hint text helpers.

This module intentionally exposes only the pagination completeness rule used by
tool descriptions and pagination response hints.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from functools import lru_cache
import importlib.util
import json
import shlex
import sys
from typing import Any, Literal

from sift_gateway.codegen.ast_guard import allowed_import_roots

PAGINATION_COMPLETENESS_RULE = (
    "Do not claim completeness until pagination.retrieval_status == COMPLETE."
)
_DEFAULT_HINT_ROOT_PATH = "$"
UsageInterface = Literal["cli", "mcp"]


@lru_cache(maxsize=512)
def _is_importable_root(root: str) -> bool:
    """Return whether an import root can be resolved in this runtime."""
    try:
        return importlib.util.find_spec(root) is not None
    except Exception:
        return False


def with_pagination_completeness_rule(text: str) -> str:
    """Append the pagination completion rule to hint text.

    Args:
        text: Base hint text.

    Returns:
        Hint text including the completion rule.
    """
    trimmed = text.strip()
    if not trimmed:
        return PAGINATION_COMPLETENESS_RULE
    if trimmed.endswith(PAGINATION_COMPLETENESS_RULE):
        return trimmed
    if not trimmed.endswith("."):
        trimmed = f"{trimmed}."
    return f"{trimmed} {PAGINATION_COMPLETENESS_RULE}"


def available_code_query_packages(
    *,
    configured_roots: Sequence[str] | None,
) -> list[str]:
    """Return sorted third-party package roots available to code queries."""
    roots = sorted(allowed_import_roots(configured_roots=configured_roots))
    return [
        root
        for root in roots
        if root not in sys.stdlib_module_names and _is_importable_root(root)
    ]


def summarize_code_query_packages(
    *,
    configured_roots: Sequence[str] | None,
    max_items: int = 4,
) -> str:
    """Return available code-query package roots as a comma-separated list.

    ``max_items`` is retained for backwards compatibility but package hints are
    no longer truncated.
    """
    _ = max_items
    roots = allowed_import_roots(configured_roots=configured_roots)
    if not roots:
        return "none"
    stdlib_roots = sys.stdlib_module_names
    packages = sorted(
        root
        for root in roots
        if root not in stdlib_roots and _is_importable_root(root)
    )
    if not packages:
        if any(root in stdlib_roots for root in roots):
            return "stdlib-only"
        return "none"
    return ",".join(packages)


def schema_primary_root_path(
    schemas: Sequence[Mapping[str, Any]] | None,
    *,
    default: str = _DEFAULT_HINT_ROOT_PATH,
) -> str:
    """Return first schema root path from a schema list."""
    if schemas is None:
        return default
    for schema in schemas:
        root_path = schema.get("root_path")
        if not isinstance(root_path, str) or not root_path:
            # Backward-compatible fallback for historic compact schema payloads.
            root_path = schema.get("rp")
        if isinstance(root_path, str) and root_path:
            return root_path
    return default


def build_code_query_usage(
    *,
    interface: UsageInterface,
    artifact_id: str,
    root_path: str,
    configured_roots: Sequence[str] | None,
) -> dict[str, Any]:
    """Build structured usage guidance for follow-up code queries."""
    package_summary = summarize_code_query_packages(
        configured_roots=configured_roots,
    )
    if interface == "mcp":
        artifact_id_literal = json.dumps(artifact_id, ensure_ascii=False)
        root_path_literal = json.dumps(root_path, ensure_ascii=False)
        example = (
            'artifact(action="query", query_kind="code", '
            f"artifact_id={artifact_id_literal}, "
            f"root_path={root_path_literal}, "
            'code="def run(data, schema, params): ...", params={})'
        )
    else:
        artifact_id_token = shlex.quote(artifact_id)
        root_path_token = shlex.quote(root_path)
        example = (
            f"sift-gateway code {artifact_id_token} "
            f'{root_path_token} --code "def run(data, schema, params): return len(data)"'
        )
    return {
        "interface": interface,
        "query_kind": "code",
        "artifact_id": artifact_id,
        "root_path": root_path,
        "entrypoint_single": "run(data, schema, params)",
        "entrypoint_multi": "run(artifacts, schemas, params)",
        "multi_input_shape": "dict[artifact_id -> list[dict]]",
        "strategy": (
            "Prefer scope=single first. Use scope=all_related when you need "
            "one query across pagination-chain artifacts. Keep outputs compact "
            "(aggregates or top <= 20 rows)."
        ),
        "packages": package_summary,
        "example": example,
    }


def render_code_query_usage_hint(usage: Mapping[str, Any]) -> str:
    """Render one usage hint line from structured usage guidance."""
    example = usage.get("example")
    packages = usage.get("packages")
    interface = usage.get("interface")
    rendered = ""
    if isinstance(example, str) and example.strip():
        rendered = f"use `{example}`" if interface == "cli" else example
    if isinstance(packages, str) and packages.strip():
        suffix = f"pkgs: {packages}"
        if rendered:
            return f"{rendered}; {suffix}"
        return suffix
    return rendered
