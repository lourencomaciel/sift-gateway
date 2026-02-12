from __future__ import annotations

import dataclasses
from typing import Any

from sidepouch_mcp.config.settings import PaginationConfig, UpstreamConfig
from sidepouch_mcp.envelope.model import (
    Envelope,
    JsonContentPart,
    TextContentPart,
)
from sidepouch_mcp.envelope.responses import gateway_tool_result
from sidepouch_mcp.mcp.handlers.mirrored_tool import (
    _inject_pagination_state,
    _pagination_response_meta,
)
from sidepouch_mcp.pagination.extract import (
    PaginationAssessment,
    PaginationState,
)

# -- _inject_pagination_state --


def _meta_ads_config() -> UpstreamConfig:
    return UpstreamConfig(
        prefix="meta-ads",
        transport="stdio",
        command="/usr/bin/echo",
        pagination=PaginationConfig(
            strategy="cursor",
            cursor_response_path="$.paging.cursors.after",
            cursor_param_name="after",
            has_more_response_path="$.paging.next",
        ),
    )


def _no_pagination_config() -> UpstreamConfig:
    return UpstreamConfig(
        prefix="demo",
        transport="stdio",
        command="/usr/bin/echo",
    )


def _envelope(
    content_value: Any = None,
    status: str = "ok",
) -> Envelope:
    parts = []
    if content_value is not None:
        parts.append(JsonContentPart(value=content_value))
    return Envelope(
        upstream_instance_id="inst_test",
        upstream_prefix="meta-ads",
        tool="get_ads",
        status=status,
        content=parts,
        meta={"warnings": []},
    )


def test_inject_no_pagination_config() -> None:
    config = _no_pagination_config()
    env = _envelope({"data": []})
    result_env, assessment = _inject_pagination_state(env, config, {}, "demo")
    assert result_env is env
    assert assessment is None


def test_inject_error_envelope_skipped() -> None:
    config = _meta_ads_config()
    env = _envelope(
        {"data": [], "paging": {"cursors": {"after": "C"}, "next": "u"}},
        status="error",
    )
    result_env, assessment = _inject_pagination_state(
        env, config, {}, "meta-ads"
    )
    assert result_env is env
    assert assessment is not None
    assert assessment.retrieval_status == "PARTIAL"
    assert assessment.partial_reason == "SIGNAL_INCONCLUSIVE"


def test_inject_no_json_content() -> None:
    config = _meta_ads_config()
    env = Envelope(
        upstream_instance_id="inst_test",
        upstream_prefix="meta-ads",
        tool="get_ads",
        status="ok",
        content=[TextContentPart(text="plain text")],
        meta={"warnings": []},
    )
    result_env, assessment = _inject_pagination_state(
        env, config, {}, "meta-ads"
    )
    assert result_env is env
    assert assessment is not None
    assert assessment.retrieval_status == "PARTIAL"
    assert assessment.partial_reason == "SIGNAL_INCONCLUSIVE"


def test_inject_no_next_page() -> None:
    config = _meta_ads_config()
    env = _envelope(
        {
            "data": [{"id": "1"}],
            "paging": {"cursors": {"after": "ABC"}},
        }
    )
    result_env, assessment = _inject_pagination_state(
        env, config, {}, "meta-ads"
    )
    assert result_env is env
    assert assessment is not None
    assert assessment.retrieval_status == "COMPLETE"
    assert assessment.partial_reason is None


def test_inject_cursor_detected() -> None:
    config = _meta_ads_config()
    json_value = {
        "data": [{"id": "1"}, {"id": "2"}],
        "paging": {
            "cursors": {"after": "CURSOR_ABC"},
            "next": "https://graph.facebook.com/page2",
        },
    }
    env = _envelope(json_value)
    result_env, assessment = _inject_pagination_state(
        env,
        config,
        {"account_id": "act_123", "limit": 200},
        "meta-ads",
    )
    assert assessment is not None
    assert assessment.state is not None
    assert assessment.state.next_params == {"after": "CURSOR_ABC"}
    assert assessment.retrieval_status == "PARTIAL"
    assert assessment.partial_reason == "MORE_PAGES_AVAILABLE"
    assert result_env is not env
    assert result_env.meta["_gateway_pagination"] == assessment.state.to_dict()
    assert result_env.meta["warnings"] == []


def test_inject_preserves_existing_meta() -> None:
    config = _meta_ads_config()
    json_value = {
        "data": [],
        "paging": {
            "cursors": {"after": "C"},
            "next": "https://example.com",
        },
    }
    env = _envelope(json_value)
    env = dataclasses.replace(
        env,
        meta={"warnings": ["w1"], "upstream_meta": {"x": 1}},
    )
    result_env, assessment = _inject_pagination_state(
        env, config, {}, "meta-ads"
    )
    assert assessment is not None
    assert assessment.state is not None
    assert result_env.meta["warnings"] == ["w1"]
    assert result_env.meta["upstream_meta"] == {"x": 1}
    assert "_gateway_pagination" in result_env.meta


def test_inject_page_number_passed_through() -> None:
    config = _meta_ads_config()
    json_value = {
        "data": [{"id": "3"}],
        "paging": {
            "cursors": {"after": "C2"},
            "next": "url",
        },
    }
    env = _envelope(json_value)
    result_env, assessment = _inject_pagination_state(
        env, config, {}, "meta-ads", page_number=2
    )
    assert assessment is not None
    assert assessment.state is not None
    assert assessment.state.page_number == 2
    assert assessment.page_number == 2


# -- _pagination_response_meta --


def test_pagination_response_meta_shape() -> None:
    assessment = PaginationAssessment(
        state=PaginationState(
            upstream_prefix="meta-ads",
            tool_name="get_ads",
            original_args={},
            next_params={"after": "ABC"},
            page_number=0,
        ),
        has_more=True,
        retrieval_status="PARTIAL",
        partial_reason="MORE_PAGES_AVAILABLE",
        warning="INCOMPLETE_RESULT_SET",
        page_number=0,
    )
    meta = _pagination_response_meta(assessment, "art_123")
    assert meta["layer"] == "upstream"
    assert meta["retrieval_status"] == "PARTIAL"
    assert meta["partial_reason"] == "MORE_PAGES_AVAILABLE"
    assert meta["has_more"] is True
    assert meta["next_action"]["tool"] == "artifact_next_page"
    assert meta["next_action"]["arguments"] == {"artifact_id": "art_123"}
    assert meta["warning"] == "INCOMPLETE_RESULT_SET"
    assert meta["has_next_page"] is True
    assert meta["page_number"] == 0
    assert "art_123" in meta["hint"]
    assert "artifact_next_page" in meta["hint"]
    assert "retrieval_status == COMPLETE" in meta["hint"]


def test_pagination_response_meta_complete_page() -> None:
    assessment = PaginationAssessment(
        state=None,
        has_more=False,
        retrieval_status="COMPLETE",
        partial_reason=None,
        warning=None,
        page_number=3,
    )
    meta = _pagination_response_meta(assessment, "art_123")
    assert meta["layer"] == "upstream"
    assert meta["retrieval_status"] == "COMPLETE"
    assert meta["partial_reason"] is None
    assert meta["has_more"] is False
    assert meta["next_action"] is None
    assert meta["warning"] is None
    assert meta["has_next_page"] is False
    assert meta["page_number"] == 3


# -- gateway_tool_result with pagination --


def test_gateway_tool_result_includes_pagination() -> None:
    pagination = {
        "layer": "upstream",
        "retrieval_status": "PARTIAL",
        "partial_reason": "MORE_PAGES_AVAILABLE",
        "has_more": True,
        "next_action": {
            "tool": "artifact_next_page",
            "arguments": {"artifact_id": "art_1"},
        },
        "warning": "INCOMPLETE_RESULT_SET",
        "has_next_page": True,
        "page_number": 0,
        "hint": "Call artifact_next_page...",
    }
    result = gateway_tool_result(
        artifact_id="art_1",
        pagination=pagination,
    )
    assert result["pagination"] == pagination


def test_gateway_tool_result_no_pagination_key_when_none() -> None:
    result = gateway_tool_result(artifact_id="art_1")
    assert "pagination" not in result
