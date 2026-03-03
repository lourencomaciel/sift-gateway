from __future__ import annotations

import dataclasses
from typing import Any

from sift_gateway.config.settings import PaginationConfig, UpstreamConfig
from sift_gateway.envelope.model import (
    Envelope,
    JsonContentPart,
    TextContentPart,
)
from sift_gateway.envelope.responses import gateway_tool_result
from sift_gateway.mcp.handlers.mirrored_tool import (
    _inject_pagination_state,
    _pagination_response_meta,
)
from sift_gateway.pagination.extract import (
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


def test_inject_discovery_without_pagination_config() -> None:
    config = _no_pagination_config()
    env = _envelope(
        {
            "result": {
                "data": [{"id": "1"}],
                "paging": {
                    "cursors": {"after": "CURSOR_2"},
                    "next": "https://example.test/items?after=CURSOR_2",
                },
            }
        }
    )
    result_env, assessment = _inject_pagination_state(
        env, config, {"limit": 100}, "demo"
    )
    assert assessment is not None
    assert assessment.state is not None
    assert assessment.state.next_params == {"after": "CURSOR_2"}
    assert result_env is not env
    assert result_env.meta["_gateway_pagination"] == assessment.state.to_dict()


def test_inject_discovery_from_header_pagination_without_config() -> None:
    config = _no_pagination_config()
    env = _envelope({"items": [{"id": "1"}]})
    env = dataclasses.replace(
        env,
        meta={
            "warnings": [],
            "upstream_meta": {
                "headers": {
                    "Link": (
                        "<https://api.example.test/items?limit=100&after=CUR_2>; "
                        'rel="next"'
                    )
                }
            },
        },
    )
    result_env, assessment = _inject_pagination_state(
        env,
        config,
        {"limit": 100, "after": "CUR_1"},
        "demo",
    )
    assert assessment is not None
    assert assessment.state is not None
    assert assessment.state.next_params == {
        "limit": 100,
        "after": "CUR_2",
    }
    assert result_env is not env


def test_inject_discovery_non_advancing_next_url_skips_injection() -> None:
    config = _no_pagination_config()
    env = _envelope(
        {
            "next": "https://api.example.test/items/page/2?limit=100",
            "items": [{"id": "1"}],
        }
    )
    result_env, assessment = _inject_pagination_state(
        env, config, {"limit": 100}, "demo"
    )
    assert result_env is env
    assert assessment is not None
    assert assessment.state is None
    assert assessment.has_more is False
    assert assessment.retrieval_status == "PARTIAL"
    assert assessment.partial_reason == "NEXT_TOKEN_MISSING"


def test_inject_discovery_from_header_next_url_key_without_config() -> None:
    config = _no_pagination_config()
    env = _envelope({"items": [{"id": "1"}]})
    env = dataclasses.replace(
        env,
        meta={
            "warnings": [],
            "upstream_meta": {
                "headers": {
                    "x-next-url": (
                        "https://api.example.test/items?offset=200&limit=100"
                    )
                }
            },
        },
    )
    result_env, assessment = _inject_pagination_state(
        env,
        config,
        {"offset": 100, "limit": 100},
        "demo",
    )
    assert assessment is not None
    assert assessment.state is not None
    assert assessment.state.next_params == {
        "offset": 200,
        "limit": 100,
    }
    assert result_env is not env


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


def test_inject_uses_text_json_content_for_discovery() -> None:
    config = _no_pagination_config()
    env = Envelope(
        upstream_instance_id="inst_test",
        upstream_prefix="demo",
        tool="list_items",
        status="ok",
        content=[
            TextContentPart(
                text=(
                    '{"items":[{"id":"1"}],'
                    '"paging":{"next":"https://example.test/items?after=C2"}}'
                )
            )
        ],
        meta={"warnings": []},
    )
    result_env, assessment = _inject_pagination_state(
        env,
        config,
        {"after": "C1"},
        "demo",
    )
    assert assessment is not None
    assert assessment.state is not None
    assert assessment.state.next_params == {"after": "C2"}
    assert result_env is not env
    assert result_env.meta["_gateway_pagination"] == assessment.state.to_dict()


def test_inject_uses_text_json_content_with_configured_pagination() -> None:
    config = _meta_ads_config()
    env = Envelope(
        upstream_instance_id="inst_test",
        upstream_prefix="meta-ads",
        tool="get_ads",
        status="ok",
        content=[
            TextContentPart(
                text=(
                    '{"data":[{"id":"1"}],'
                    '"paging":{"cursors":{"after":"CURSOR_2"},'
                    '"next":"https://example.test/next"}}'
                )
            )
        ],
        meta={"warnings": []},
    )
    result_env, assessment = _inject_pagination_state(
        env,
        config,
        {"limit": 100},
        "meta-ads",
    )
    assert assessment is not None
    assert assessment.state is not None
    assert assessment.state.next_params == {"after": "CURSOR_2"}
    assert result_env.meta["_gateway_pagination"] == assessment.state.to_dict()


def test_inject_no_json_content_discovery_followup_clears_state() -> None:
    config = _no_pagination_config()
    env = Envelope(
        upstream_instance_id="inst_test",
        upstream_prefix="demo",
        tool="get_ads",
        status="ok",
        content=[TextContentPart(text="plain text")],
        meta={"warnings": []},
    )
    result_env, assessment = _inject_pagination_state(
        env, config, {"page": 2, "limit": 100}, "demo", page_number=1
    )
    assert result_env is env
    assert assessment is not None
    assert assessment.state is None
    assert assessment.has_more is False
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
    _result_env, assessment = _inject_pagination_state(
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
            original_args={"limit": 100},
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
    assert meta["next"] == {
        "kind": "tool_call",
        "artifact_id": "art_123",
        "tool": "artifact",
        "arguments": {
            "action": "next_page",
            "artifact_id": "art_123",
        },
        "params": {"after": "ABC"},
    }
    assert meta["warning"] == "INCOMPLETE_RESULT_SET"
    assert meta["warnings"] == [{"code": "INCOMPLETE_RESULT_SET"}]
    assert meta["page_number"] == 0
    capability = meta.get("capability")
    assert capability == {
        "has_more_signal_detected": True,
        "continuable": True,
        "next_params_detected": True,
    }
    assert "art_123" in meta["hint"]
    assert "limit=100" in meta["hint"]
    assert 'after="ABC"' in meta["hint"]
    assert "next_page" in meta["hint"]
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
    assert meta["capability"] == {
        "has_more_signal_detected": False,
        "continuable": False,
        "next_params_detected": False,
    }
    assert meta["next"] is None
    assert meta["warning"] is None
    assert "warnings" not in meta
    assert meta["page_number"] == 3


def test_pagination_response_meta_merges_extra_warnings() -> None:
    assessment = PaginationAssessment(
        state=PaginationState(
            upstream_prefix="meta-ads",
            tool_name="get_ads",
            original_args={},
            next_params={"after": "NEXT"},
            page_number=1,
        ),
        has_more=True,
        retrieval_status="PARTIAL",
        partial_reason="MORE_PAGES_AVAILABLE",
        warning="INCOMPLETE_RESULT_SET",
        page_number=1,
    )
    meta = _pagination_response_meta(
        assessment,
        "art_123",
        extra_warnings=[
            {
                "code": "PAGINATION_DUPLICATE_PAGE",
                "previous_artifact_id": "art_prev",
            }
        ],
    )
    assert meta["warnings"] == [
        {"code": "INCOMPLETE_RESULT_SET"},
        {
            "code": "PAGINATION_DUPLICATE_PAGE",
            "previous_artifact_id": "art_prev",
        },
    ]


# -- gateway_tool_result with pagination --


def test_gateway_tool_result_includes_pagination() -> None:
    pagination = {
        "layer": "upstream",
        "retrieval_status": "PARTIAL",
        "partial_reason": "MORE_PAGES_AVAILABLE",
        "has_more": True,
        "next": {
            "kind": "tool_call",
            "artifact_id": "art_1",
            "tool": "artifact",
            "arguments": {"action": "next_page", "artifact_id": "art_1"},
            "params": {"after": "C2"},
        },
        "warning": "INCOMPLETE_RESULT_SET",
        "page_number": 0,
        "hint": "Call artifact...",
    }
    result = gateway_tool_result(
        response_mode="schema_ref",
        artifact_id="art_1",
        schemas=[],
        pagination=pagination,
    )
    assert result["pagination"] == pagination


def test_gateway_tool_result_no_pagination_key_when_none() -> None:
    result = gateway_tool_result(
        response_mode="full",
        artifact_id="art_1",
        payload={"ok": True},
    )
    assert "pagination" not in result
