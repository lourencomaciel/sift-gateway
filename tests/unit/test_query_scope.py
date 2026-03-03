from __future__ import annotations

from sift_gateway.core.query_scope import resolve_scope


def test_resolve_scope_defaults_to_single() -> None:
    scope, error = resolve_scope(raw_scope=None, cursor_payload=None)
    assert error is None
    assert scope == "single"


def test_resolve_scope_respects_explicit_all_related() -> None:
    scope, error = resolve_scope(raw_scope="all_related", cursor_payload=None)
    assert error is None
    assert scope == "all_related"
