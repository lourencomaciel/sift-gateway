from __future__ import annotations

from decimal import Decimal

from sift_mcp.retrieval.response import (
    apply_output_budgets,
    build_retrieval_response,
)


def test_retrieval_response_requires_cursor_when_truncated() -> None:
    try:
        build_retrieval_response(items=[1], truncated=True, cursor=None)
    except ValueError as exc:
        assert "cursor is required" in str(exc)
    else:
        raise AssertionError("expected ValueError")


def test_retrieval_response_shape() -> None:
    payload = build_retrieval_response(
        items=[{"id": 1}],
        truncated=False,
        cursor=None,
        omitted=0,
        stats={"bytes_out": 20},
    )
    assert payload["items"] == [{"id": 1}]
    assert payload["truncated"] is False
    assert payload["cursor"] is None
    assert payload["stats"]["bytes_out"] == 20
    assert payload["pagination"]["layer"] == "artifact_retrieval"
    assert payload["pagination"]["retrieval_status"] == "COMPLETE"
    assert payload["pagination"]["has_more"] is False
    assert payload["pagination"]["next_cursor"] is None


def test_retrieval_response_partial_includes_next_cursor() -> None:
    payload = build_retrieval_response(
        items=[{"id": 1}],
        truncated=True,
        cursor="cur_next",
    )
    assert payload["pagination"]["layer"] == "artifact_retrieval"
    assert payload["pagination"]["retrieval_status"] == "PARTIAL"
    assert payload["pagination"]["partial_reason"] == "CURSOR_AVAILABLE"
    assert payload["pagination"]["has_more"] is True
    assert payload["pagination"]["next_cursor"] == "cur_next"


def test_apply_output_budgets_truncates() -> None:
    items = [{"n": i} for i in range(10)]
    selected, truncated, omitted, used = apply_output_budgets(
        items,
        max_items=3,
        max_bytes_out=10_000,
    )
    assert len(selected) == 3
    assert truncated is True
    assert omitted == 7
    assert used > 0


def test_apply_output_budgets_accepts_decimal_values() -> None:
    items = [{"price": Decimal("1.23")}]
    selected, truncated, omitted, used = apply_output_budgets(
        items,
        max_items=10,
        max_bytes_out=10_000,
    )
    assert selected == items
    assert truncated is False
    assert omitted == 0
    assert used > 0
