"""Tests for auto-pagination merge logic and helpers."""

from __future__ import annotations

from sift_mcp.config.settings import GatewayConfig, UpstreamConfig
from sift_mcp.envelope.model import (
    Envelope,
    JsonContentPart,
    TextContentPart,
)
from sift_mcp.pagination.auto import (
    _count_json_records,
    _count_json_value_records,
    _extract_json_content,
    _merge_json_values,
    merge_envelopes,
    resolve_auto_paginate_limits,
)
from sift_mcp.pagination.contract import (
    RETRIEVAL_STATUS_COMPLETE,
    RETRIEVAL_STATUS_PARTIAL,
)
from sift_mcp.pagination.extract import (
    PaginationAssessment,
    PaginationState,
)


def _make_envelope(
    json_value: object | None = None,
    *,
    status: str = "ok",
    meta: dict | None = None,
) -> Envelope:
    """Build a minimal envelope for testing."""
    content = []
    if json_value is not None:
        content.append(JsonContentPart(value=json_value))
    return Envelope(
        upstream_instance_id="inst_1",
        upstream_prefix="test",
        tool="get_items",
        status=status,
        content=content,
        meta=meta or {},
    )


def _make_assessment(
    *,
    has_more: bool = False,
    state: PaginationState | None = None,
    page_number: int = 0,
) -> PaginationAssessment:
    """Build a minimal assessment for testing."""
    return PaginationAssessment(
        state=state,
        has_more=has_more,
        retrieval_status=(
            RETRIEVAL_STATUS_PARTIAL if has_more else RETRIEVAL_STATUS_COMPLETE
        ),
        partial_reason=None,
        warning=None,
        page_number=page_number,
    )


def _make_state(
    page_number: int = 0,
    next_params: dict | None = None,
) -> PaginationState:
    """Build a minimal PaginationState."""
    return PaginationState(
        upstream_prefix="test",
        tool_name="get_items",
        original_args={"limit": 10},
        next_params=next_params or {"after": "cursor_abc"},
        page_number=page_number,
    )


# ---- resolve_auto_paginate_limits ----


def test_resolve_limits_uses_gateway_defaults() -> None:
    gateway = GatewayConfig(
        auto_paginate_max_pages=10,
        auto_paginate_max_records=1000,
        auto_paginate_timeout_seconds=30.0,
    )
    upstream = UpstreamConfig(prefix="test", transport="stdio", command="echo")
    limits = resolve_auto_paginate_limits(gateway, upstream)
    assert limits.max_pages == 10
    assert limits.max_records == 1000
    assert limits.timeout == 30.0


def test_resolve_limits_upstream_overrides() -> None:
    gateway = GatewayConfig(
        auto_paginate_max_pages=10,
        auto_paginate_max_records=1000,
        auto_paginate_timeout_seconds=30.0,
    )
    upstream = UpstreamConfig(
        prefix="test",
        transport="stdio",
        command="echo",
        auto_paginate_max_pages=5,
        auto_paginate_max_records=500,
        auto_paginate_timeout_seconds=15.0,
    )
    limits = resolve_auto_paginate_limits(gateway, upstream)
    assert limits.max_pages == 5
    assert limits.max_records == 500
    assert limits.timeout == 15.0


def test_resolve_limits_partial_upstream_override() -> None:
    gateway = GatewayConfig(
        auto_paginate_max_pages=10,
        auto_paginate_max_records=1000,
        auto_paginate_timeout_seconds=30.0,
    )
    upstream = UpstreamConfig(
        prefix="test",
        transport="stdio",
        command="echo",
        auto_paginate_max_pages=3,
    )
    limits = resolve_auto_paginate_limits(gateway, upstream)
    assert limits.max_pages == 3
    assert limits.max_records == 1000
    assert limits.timeout == 30.0


def test_resolve_limits_zero_disables() -> None:
    gateway = GatewayConfig(auto_paginate_max_pages=0)
    upstream = UpstreamConfig(prefix="test", transport="stdio", command="echo")
    limits = resolve_auto_paginate_limits(gateway, upstream)
    assert limits.max_pages == 0


def test_upstream_rejects_negative_max_pages() -> None:
    import pytest

    with pytest.raises(ValueError, match="auto_paginate_max_pages"):
        UpstreamConfig(
            prefix="test",
            transport="stdio",
            command="echo",
            auto_paginate_max_pages=-1,
        )


def test_upstream_rejects_negative_timeout() -> None:
    import pytest

    with pytest.raises(ValueError, match="auto_paginate_timeout_seconds"):
        UpstreamConfig(
            prefix="test",
            transport="stdio",
            command="echo",
            auto_paginate_timeout_seconds=-5.0,
        )


# ---- _count_json_records ----


def test_count_records_bare_list() -> None:
    env = _make_envelope([{"id": 1}, {"id": 2}, {"id": 3}])
    assert _count_json_records(env) == 3


def test_count_records_wrapped_data() -> None:
    env = _make_envelope({"data": [{"id": 1}, {"id": 2}]})
    assert _count_json_records(env) == 2


def test_count_records_wrapped_results() -> None:
    env = _make_envelope({"results": [1, 2, 3, 4]})
    assert _count_json_records(env) == 4


def test_count_records_wrapped_items() -> None:
    env = _make_envelope({"items": [1]})
    assert _count_json_records(env) == 1


def test_count_records_no_json() -> None:
    env = Envelope(
        upstream_instance_id="i",
        upstream_prefix="p",
        tool="t",
        status="ok",
        content=[TextContentPart(text="hello")],
    )
    assert _count_json_records(env) == 0


def test_count_records_empty_envelope() -> None:
    env = _make_envelope(None)
    assert _count_json_records(env) == 0


def test_count_records_dict_no_known_key() -> None:
    env = _make_envelope({"unknown_key": [1, 2]})
    assert _count_json_records(env) == 0


def test_count_records_entries_key() -> None:
    env = _make_envelope({"entries": [1, 2, 3]})
    assert _count_json_records(env) == 3


# ---- _extract_json_content ----


def test_extract_json_returns_value() -> None:
    env = _make_envelope({"key": "val"})
    assert _extract_json_content(env) == {"key": "val"}


def test_extract_json_returns_none_for_text_only() -> None:
    env = Envelope(
        upstream_instance_id="i",
        upstream_prefix="p",
        tool="t",
        status="ok",
        content=[TextContentPart(text="hello")],
    )
    assert _extract_json_content(env) is None


# ---- _merge_json_values ----


def test_merge_bare_lists() -> None:
    result = _merge_json_values([1, 2], [3, 4])
    assert result == [1, 2, 3, 4]


def test_merge_data_wrapped() -> None:
    base = {"data": [1, 2], "total": 10}
    additional = {"data": [3, 4], "total": 10}
    result = _merge_json_values(base, additional)
    assert result["data"] == [1, 2, 3, 4]


def test_merge_results_wrapped() -> None:
    base = {"results": ["a"]}
    additional = {"results": ["b"]}
    result = _merge_json_values(base, additional)
    assert result["results"] == ["a", "b"]


def test_merge_wrapper_metadata_from_latest_page() -> None:
    base = {"data": [1], "has_more": True, "next": "cur_1"}
    additional = {"data": [2], "has_more": False, "next": None}
    result = _merge_json_values(base, additional)
    assert result["data"] == [1, 2]
    assert result["has_more"] is False
    assert result["next"] is None


def test_merge_fallback_wraps_in_list() -> None:
    result = _merge_json_values("hello", "world")
    assert result == ["hello", "world"]


def test_merge_dict_no_common_key_wraps() -> None:
    result = _merge_json_values({"x": 1}, {"y": 2})
    assert result == [{"x": 1}, {"y": 2}]


# ---- merge_envelopes ----


def test_merge_envelopes_basic() -> None:
    base = _make_envelope({"data": [1, 2]})
    assessment = _make_assessment(has_more=False, page_number=1)
    result = merge_envelopes(base, [{"data": [3, 4]}], assessment)
    json_val = _extract_json_content(result)
    assert json_val["data"] == [1, 2, 3, 4]


def test_merge_envelopes_no_json_returns_base() -> None:
    env = Envelope(
        upstream_instance_id="i",
        upstream_prefix="p",
        tool="t",
        status="ok",
        content=[TextContentPart(text="hello")],
    )
    assessment = _make_assessment()
    result = merge_envelopes(env, [{"data": [1]}], assessment)
    assert result is env


def test_merge_envelopes_updates_pagination_state() -> None:
    base = _make_envelope(
        {"data": [1]},
        meta={"_gateway_pagination": {"page_number": 0}},
    )
    state = _make_state(page_number=2)
    assessment = _make_assessment(has_more=True, state=state, page_number=2)
    result = merge_envelopes(base, [{"data": [2]}], assessment)
    pg = result.meta.get("_gateway_pagination")
    assert pg is not None
    assert pg["page_number"] == 2


def test_merge_envelopes_clears_pagination_when_complete() -> None:
    base = _make_envelope(
        {"data": [1]},
        meta={"_gateway_pagination": {"page_number": 0}},
    )
    assessment = _make_assessment(has_more=False)
    result = merge_envelopes(base, [{"data": [2]}], assessment)
    assert "_gateway_pagination" not in result.meta


def test_merge_envelopes_multiple_pages() -> None:
    base = _make_envelope([1, 2])
    assessment = _make_assessment(has_more=False)
    result = merge_envelopes(base, [[3, 4], [5, 6]], assessment)
    json_val = _extract_json_content(result)
    assert json_val == [1, 2, 3, 4, 5, 6]


# ---- _count_json_value_records ----


def test_count_value_records_bare_list() -> None:
    assert _count_json_value_records([1, 2, 3]) == 3


def test_count_value_records_wrapped_data() -> None:
    assert _count_json_value_records({"data": [1, 2]}) == 2


def test_count_value_records_wrapped_items() -> None:
    assert _count_json_value_records({"items": [1]}) == 1


def test_count_value_records_no_known_key() -> None:
    assert _count_json_value_records({"unknown": [1, 2]}) == 0


def test_count_value_records_non_countable() -> None:
    assert _count_json_value_records("scalar") == 0


def test_count_value_records_empty_list() -> None:
    assert _count_json_value_records([]) == 0
