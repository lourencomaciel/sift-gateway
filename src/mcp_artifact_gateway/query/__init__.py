"""Query layer for MCP Artifact Gateway.

This package provides:
- JSONPath subset parsing and evaluation (section 12.3)
- select_paths canonicalization and hashing (section 12.3.1)
- Where clause hashing (section 12.3.1)
- Where DSL parsing and evaluation (Addendum E)
"""

from mcp_artifact_gateway.query.jsonpath import (
    BudgetExceededError,
    evaluate_path,
    normalize_jsonpath,
    parse_jsonpath,
)
from mcp_artifact_gateway.query.select_paths import (
    canonicalize_select_paths,
    select_paths_hash,
)
from mcp_artifact_gateway.query.where_dsl import evaluate_where, parse_where
from mcp_artifact_gateway.query.where_hash import where_hash

__all__ = [
    "BudgetExceededError",
    "canonicalize_select_paths",
    "evaluate_path",
    "evaluate_where",
    "normalize_jsonpath",
    "parse_jsonpath",
    "parse_where",
    "select_paths_hash",
    "where_hash",
]
