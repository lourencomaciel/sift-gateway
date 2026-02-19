from __future__ import annotations

from decimal import Decimal

from sift_gateway.retrieval.response import apply_output_budgets


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
