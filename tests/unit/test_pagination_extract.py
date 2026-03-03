from __future__ import annotations

from sift_gateway.config.settings import PaginationConfig
from sift_gateway.pagination.extract import (
    PaginationAssessment,
    PaginationState,
    _evaluate_path,
    _has_more,
    assess_pagination,
    extract_pagination_state,
)

# -- _evaluate_path --


def test_evaluate_path_basic() -> None:
    data = {"paging": {"cursors": {"after": "ABC"}}}
    assert _evaluate_path(data, "$.paging.cursors.after") == "ABC"


def test_evaluate_path_root() -> None:
    data = {"key": "value"}
    assert _evaluate_path(data, "$.key") == "value"


def test_evaluate_path_missing_segment() -> None:
    data = {"paging": {"cursors": {}}}
    assert _evaluate_path(data, "$.paging.cursors.after") is None


def test_evaluate_path_non_dict_segment() -> None:
    data = {"paging": "string_value"}
    assert _evaluate_path(data, "$.paging.cursors.after") is None


def test_evaluate_path_no_dollar_prefix() -> None:
    data = {"key": "value"}
    assert _evaluate_path(data, "key") is None


def test_evaluate_path_dollar_only() -> None:
    data = {"key": "value"}
    result = _evaluate_path(data, "$")
    assert result == data


def test_evaluate_path_array_index() -> None:
    data = {"items": [{"cursor": "A"}, {"cursor": "B"}]}
    assert _evaluate_path(data, "$.items[0].cursor") == "A"
    assert _evaluate_path(data, "$.items[1].cursor") == "B"


def test_evaluate_path_bracket_quoted_field() -> None:
    data = {"next-page": {"token": "T"}}
    assert _evaluate_path(data, "$['next-page'].token") == "T"


def test_evaluate_path_empty_string() -> None:
    assert _evaluate_path({"key": "v"}, "") is None


def test_evaluate_path_invalid_syntax() -> None:
    assert _evaluate_path({"key": "v"}, "$[invalid") is None


# -- _has_more --


def test_has_more_no_path_returns_true() -> None:
    assert _has_more({"data": []}, None) is True


def test_has_more_empty_path_returns_true() -> None:
    assert _has_more({"data": []}, "") is True


def test_has_more_path_present_string() -> None:
    data = {"paging": {"next": "https://example.com/page2"}}
    assert _has_more(data, "$.paging.next") is True


def test_has_more_path_missing() -> None:
    data = {"paging": {}}
    assert _has_more(data, "$.paging.next") is False


def test_has_more_path_none_value() -> None:
    data = {"paging": {"next": None}}
    assert _has_more(data, "$.paging.next") is False


def test_has_more_path_empty_string() -> None:
    data = {"has_more": "  "}
    assert _has_more(data, "$.has_more") is False


def test_has_more_path_bool_true() -> None:
    data = {"has_more": True}
    assert _has_more(data, "$.has_more") is True


def test_has_more_path_bool_false() -> None:
    data = {"has_more": False}
    assert _has_more(data, "$.has_more") is False


def test_has_more_path_empty_list() -> None:
    data = {"items": []}
    assert _has_more(data, "$.items") is False


def test_has_more_path_non_empty_list() -> None:
    data = {"items": [1, 2, 3]}
    assert _has_more(data, "$.items") is True


# -- PaginationState serialization --


def test_pagination_state_to_dict() -> None:
    state = PaginationState(
        upstream_prefix="meta-ads",
        tool_name="get_ads",
        original_args={"account_id": "act_123", "limit": 200},
        next_params={"after": "CURSOR_ABC"},
        page_number=0,
    )
    d = state.to_dict()
    assert d["upstream_prefix"] == "meta-ads"
    assert d["tool_name"] == "get_ads"
    assert d["original_args"] == {"account_id": "act_123", "limit": 200}
    assert d["next_params"] == {"after": "CURSOR_ABC"}
    assert d["page_number"] == 0


def test_pagination_state_roundtrip() -> None:
    state = PaginationState(
        upstream_prefix="google",
        tool_name="list_files",
        original_args={"q": "test"},
        next_params={"pageToken": "TOKEN_XYZ"},
        page_number=2,
    )
    restored = PaginationState.from_dict(state.to_dict())
    assert restored == state


def test_pagination_state_from_dict_defaults() -> None:
    data = {
        "upstream_prefix": "demo",
        "tool_name": "list",
    }
    state = PaginationState.from_dict(data)
    assert state.original_args == {}
    assert state.next_params == {}
    assert state.page_number == 0


# -- extract_pagination_state: cursor strategy --


def _cursor_config() -> PaginationConfig:
    return PaginationConfig(
        strategy="cursor",
        cursor_response_path="$.paging.cursors.after",
        cursor_param_name="after",
        has_more_response_path="$.paging.next",
    )


def test_cursor_strategy_extracts_next() -> None:
    json_value = {
        "data": [{"id": "1"}, {"id": "2"}],
        "paging": {
            "cursors": {"after": "CURSOR_ABC"},
            "next": "https://graph.facebook.com/page2",
        },
    }
    state = extract_pagination_state(
        json_value=json_value,
        pagination_config=_cursor_config(),
        original_args={"account_id": "act_123", "limit": 200},
        upstream_prefix="meta-ads",
        tool_name="get_ads",
    )
    assert state is not None
    assert state.next_params == {"after": "CURSOR_ABC"}
    assert state.page_number == 0
    assert state.upstream_prefix == "meta-ads"
    assert state.tool_name == "get_ads"


def test_cursor_strategy_no_next_page() -> None:
    json_value = {
        "data": [{"id": "1"}],
        "paging": {"cursors": {"after": "CURSOR"}},
    }
    state = extract_pagination_state(
        json_value=json_value,
        pagination_config=_cursor_config(),
        original_args={},
        upstream_prefix="meta-ads",
        tool_name="get_ads",
    )
    assert state is None


def test_cursor_strategy_empty_cursor() -> None:
    json_value = {
        "data": [],
        "paging": {
            "cursors": {"after": "  "},
            "next": "https://example.com",
        },
    }
    state = extract_pagination_state(
        json_value=json_value,
        pagination_config=_cursor_config(),
        original_args={},
        upstream_prefix="meta-ads",
        tool_name="get_ads",
    )
    assert state is None


def test_cursor_strategy_missing_cursor_path() -> None:
    json_value = {
        "data": [],
        "paging": {"next": "https://example.com"},
    }
    state = extract_pagination_state(
        json_value=json_value,
        pagination_config=_cursor_config(),
        original_args={},
        upstream_prefix="meta-ads",
        tool_name="get_ads",
    )
    assert state is None


def test_cursor_strategy_page_number_carries() -> None:
    json_value = {
        "data": [{"id": "3"}],
        "paging": {
            "cursors": {"after": "CURSOR_2"},
            "next": "https://example.com",
        },
    }
    state = extract_pagination_state(
        json_value=json_value,
        pagination_config=_cursor_config(),
        original_args={"limit": 200},
        upstream_prefix="meta-ads",
        tool_name="get_ads",
        page_number=1,
    )
    assert state is not None
    assert state.page_number == 1


# -- extract_pagination_state: offset strategy --


def _offset_config() -> PaginationConfig:
    return PaginationConfig(
        strategy="offset",
        offset_param_name="offset",
        page_size_param_name="limit",
        has_more_response_path="$.has_more",
    )


def test_offset_strategy_first_page() -> None:
    json_value = {"items": [1, 2, 3], "has_more": True}
    state = extract_pagination_state(
        json_value=json_value,
        pagination_config=_offset_config(),
        original_args={"limit": 100},
        upstream_prefix="api",
        tool_name="list_items",
    )
    assert state is not None
    assert state.next_params == {"offset": 100}


def test_offset_strategy_second_page() -> None:
    json_value = {"items": [4, 5, 6], "has_more": True}
    state = extract_pagination_state(
        json_value=json_value,
        pagination_config=_offset_config(),
        original_args={"offset": 100, "limit": 100},
        upstream_prefix="api",
        tool_name="list_items",
    )
    assert state is not None
    assert state.next_params == {"offset": 200}


def test_offset_strategy_no_more_pages() -> None:
    json_value = {"items": [7, 8], "has_more": False}
    state = extract_pagination_state(
        json_value=json_value,
        pagination_config=_offset_config(),
        original_args={"offset": 200, "limit": 100},
        upstream_prefix="api",
        tool_name="list_items",
    )
    assert state is None


def test_offset_strategy_zero_page_size() -> None:
    json_value = {"items": [], "has_more": True}
    state = extract_pagination_state(
        json_value=json_value,
        pagination_config=_offset_config(),
        original_args={"limit": 0},
        upstream_prefix="api",
        tool_name="list_items",
    )
    assert state is None


def test_offset_strategy_missing_page_size() -> None:
    json_value = {"items": [], "has_more": True}
    state = extract_pagination_state(
        json_value=json_value,
        pagination_config=_offset_config(),
        original_args={},
        upstream_prefix="api",
        tool_name="list_items",
    )
    assert state is None


def test_offset_strategy_custom_param_names() -> None:
    config = PaginationConfig(
        strategy="offset",
        offset_param_name="skip",
        page_size_param_name="count",
        has_more_response_path="$.has_more",
    )
    state = extract_pagination_state(
        json_value={"items": [1, 2], "has_more": True},
        pagination_config=config,
        original_args={"skip": 20, "count": 20},
        upstream_prefix="api",
        tool_name="list_items",
    )
    assert state is not None
    assert state.next_params == {"skip": 40}


# -- extract_pagination_state: page_number strategy --


def _page_number_config() -> PaginationConfig:
    return PaginationConfig(
        strategy="page_number",
        page_param_name="page",
        has_more_response_path="$.has_more",
    )


def test_page_number_strategy_first_page() -> None:
    json_value = {"items": [1, 2], "has_more": True}
    state = extract_pagination_state(
        json_value=json_value,
        pagination_config=_page_number_config(),
        original_args={},
        upstream_prefix="api",
        tool_name="list_users",
    )
    assert state is not None
    assert state.next_params == {"page": 2}


def test_page_number_strategy_page_three() -> None:
    json_value = {"items": [5, 6], "has_more": True}
    state = extract_pagination_state(
        json_value=json_value,
        pagination_config=_page_number_config(),
        original_args={"page": 3},
        upstream_prefix="api",
        tool_name="list_users",
    )
    assert state is not None
    assert state.next_params == {"page": 4}


def test_page_number_strategy_no_more() -> None:
    json_value = {"items": [7], "has_more": False}
    state = extract_pagination_state(
        json_value=json_value,
        pagination_config=_page_number_config(),
        original_args={"page": 5},
        upstream_prefix="api",
        tool_name="list_users",
    )
    assert state is None


def test_page_number_strategy_custom_param_name() -> None:
    config = PaginationConfig(
        strategy="page_number",
        page_param_name="page_number",
        has_more_response_path="$.has_more",
    )
    state = extract_pagination_state(
        json_value={"items": [7], "has_more": True},
        pagination_config=config,
        original_args={"page_number": 5},
        upstream_prefix="api",
        tool_name="list_users",
    )
    assert state is not None
    assert state.next_params == {"page_number": 6}


# -- extract_pagination_state: param_map strategy --


def _param_map_config() -> PaginationConfig:
    return PaginationConfig(
        strategy="param_map",
        next_params_response_paths={
            "page_token": "$.next.page_token",
            "checkpoint": "$.next.checkpoint",
        },
        has_more_response_path="$.has_more",
    )


def test_param_map_strategy_extracts_multiple_params() -> None:
    json_value = {
        "has_more": True,
        "next": {
            "page_token": "tok_2",
            "checkpoint": "cp_9",
        },
    }
    state = extract_pagination_state(
        json_value=json_value,
        pagination_config=_param_map_config(),
        original_args={"limit": 100},
        upstream_prefix="api",
        tool_name="list_events",
    )
    assert state is not None
    assert state.next_params == {
        "page_token": "tok_2",
        "checkpoint": "cp_9",
    }


def test_param_map_strategy_missing_mapped_value_returns_none() -> None:
    json_value = {
        "has_more": True,
        "next": {
            "page_token": "tok_2",
        },
    }
    state = extract_pagination_state(
        json_value=json_value,
        pagination_config=_param_map_config(),
        original_args={},
        upstream_prefix="api",
        tool_name="list_events",
    )
    assert state is None


def test_param_map_strategy_blank_mapped_value_returns_none() -> None:
    state = extract_pagination_state(
        json_value={
            "has_more": True,
            "next": {
                "page_token": "tok_2",
                "checkpoint": "   ",
            },
        },
        pagination_config=_param_map_config(),
        original_args={},
        upstream_prefix="api",
        tool_name="list_events",
    )
    assert state is None


# -- extract_pagination_state: unknown strategy --


def test_unknown_strategy_returns_none() -> None:
    config = PaginationConfig.__new__(PaginationConfig)
    object.__setattr__(config, "strategy", "unknown")
    object.__setattr__(config, "has_more_response_path", None)
    state = extract_pagination_state(
        json_value={"data": []},
        pagination_config=config,
        original_args={},
        upstream_prefix="x",
        tool_name="y",
    )
    assert state is None


# -- assess_pagination: completion semantics --


def test_assess_pagination_cursor_partial_with_next_action_state() -> None:
    assessment = assess_pagination(
        json_value={
            "data": [1],
            "paging": {
                "cursors": {"after": "CURSOR_X"},
                "next": "https://example.com/page2",
            },
        },
        pagination_config=_cursor_config(),
        original_args={"limit": 100},
        upstream_prefix="meta-ads",
        tool_name="get_ads",
        page_number=0,
    )
    assert isinstance(assessment, PaginationAssessment)
    assert assessment.retrieval_status == "PARTIAL"
    assert assessment.partial_reason == "MORE_PAGES_AVAILABLE"
    assert assessment.has_more is True
    assert assessment.warning == "INCOMPLETE_RESULT_SET"
    assert assessment.state is not None
    assert assessment.state.next_params == {"after": "CURSOR_X"}


def test_assess_pagination_cursor_complete_on_explicit_terminal() -> None:
    assessment = assess_pagination(
        json_value={
            "data": [1],
            "paging": {
                "cursors": {"after": "CURSOR_X"},
                "next": None,
            },
        },
        pagination_config=_cursor_config(),
        original_args={"limit": 100},
        upstream_prefix="meta-ads",
        tool_name="get_ads",
        page_number=1,
    )
    assert assessment.retrieval_status == "COMPLETE"
    assert assessment.partial_reason is None
    assert assessment.warning is None
    assert assessment.has_more is False
    assert assessment.state is None


def test_assess_pagination_cursor_inconclusive_without_signal_or_token() -> (
    None
):
    assessment = assess_pagination(
        json_value={"data": [1], "paging": {"cursors": {}}},
        pagination_config=PaginationConfig(
            strategy="cursor",
            cursor_response_path="$.paging.cursors.after",
            cursor_param_name="after",
            has_more_response_path=None,
        ),
        original_args={},
        upstream_prefix="meta-ads",
        tool_name="get_ads",
    )
    assert assessment.retrieval_status == "PARTIAL"
    assert assessment.partial_reason == "SIGNAL_INCONCLUSIVE"
    assert assessment.warning == "INCOMPLETE_RESULT_SET"
    assert assessment.has_more is False
    assert assessment.state is None


def test_assess_pagination_cursor_missing_token_with_has_more_signal() -> None:
    assessment = assess_pagination(
        json_value={"data": [1], "has_more": True},
        pagination_config=PaginationConfig(
            strategy="cursor",
            cursor_response_path="$.paging.cursors.after",
            cursor_param_name="after",
            has_more_response_path="$.has_more",
        ),
        original_args={},
        upstream_prefix="meta-ads",
        tool_name="get_ads",
    )
    assert assessment.retrieval_status == "PARTIAL"
    assert assessment.partial_reason == "NEXT_TOKEN_MISSING"
    assert assessment.warning == "INCOMPLETE_RESULT_SET"
    assert assessment.has_more is False
    assert assessment.state is None


def test_assess_pagination_param_map_complete_on_terminal_signal() -> None:
    assessment = assess_pagination(
        json_value={
            "has_more": False,
            "next": {
                "page_token": "tok_2",
                "checkpoint": "cp_9",
            },
        },
        pagination_config=_param_map_config(),
        original_args={},
        upstream_prefix="api",
        tool_name="list_events",
    )
    assert assessment.retrieval_status == "COMPLETE"
    assert assessment.partial_reason is None
    assert assessment.warning is None
    assert assessment.has_more is False
    assert assessment.state is None


def test_assess_pagination_without_config_discovers_cursor_state() -> None:
    assessment = assess_pagination(
        json_value={
            "result": {
                "data": [{"id": 1}],
                "paging": {
                    "cursors": {"after": "CURSOR_2"},
                    "next": "https://example.test/items?after=CURSOR_2",
                },
            }
        },
        pagination_config=None,
        original_args={"limit": 100},
        upstream_prefix="api",
        tool_name="list_items",
    )
    assert assessment is not None
    assert assessment.state is not None
    assert assessment.state.next_params == {"after": "CURSOR_2"}
    assert assessment.retrieval_status == "PARTIAL"


def test_assess_pagination_without_config_discovers_relative_query_next() -> (
    None
):
    assessment = assess_pagination(
        json_value={"next": "?after=CURSOR_2&limit=100"},
        pagination_config=None,
        original_args={"after": "CURSOR_1", "limit": 100},
        upstream_prefix="api",
        tool_name="list_items",
    )
    assert assessment is not None
    assert assessment.state is not None
    assert assessment.state.next_params == {
        "after": "CURSOR_2",
        "limit": 100,
    }
    assert assessment.retrieval_status == "PARTIAL"


def test_assess_pagination_without_config_no_evidence_returns_none() -> None:
    assessment = assess_pagination(
        json_value={"items": [{"id": 1}]},
        pagination_config=None,
        original_args={"q": "x"},
        upstream_prefix="api",
        tool_name="list_items",
    )
    assert assessment is None


def test_assess_pagination_without_config_first_page_missing_token() -> None:
    assessment = assess_pagination(
        json_value={"has_more": True, "items": [{"id": 1}]},
        pagination_config=None,
        original_args={"q": "x"},
        upstream_prefix="api",
        tool_name="list_items",
    )
    assert assessment is not None
    assert assessment.state is None
    assert assessment.has_more is False
    assert assessment.retrieval_status == "PARTIAL"
    assert assessment.partial_reason == "NEXT_TOKEN_MISSING"
    assert assessment.warning == "INCOMPLETE_RESULT_SET"
    assert assessment.detector is not None
    assert assessment.detector.get("mode") == "discovery"


def test_assess_pagination_without_config_non_advancing_reason() -> None:
    assessment = assess_pagination(
        json_value={
            "paging": {
                "next": (
                    "https://api.example.test/items?limit=100&after=CURSOR_1"
                ),
            }
        },
        pagination_config=None,
        original_args={"limit": 100, "after": "CURSOR_1"},
        upstream_prefix="api",
        tool_name="list_items",
    )
    assert assessment is not None
    assert assessment.state is None
    assert assessment.has_more is False
    assert assessment.retrieval_status == "PARTIAL"
    assert assessment.partial_reason == "NEXT_TOKEN_MISSING"
    assert assessment.warning == "INCOMPLETE_RESULT_SET"
    assert assessment.detector is not None
    assert assessment.detector.get("mode") == "discovery"
    assert assessment.detector.get("rejected_reason") == (
        "non_advancing_cursor"
    )


def test_assess_pagination_without_config_followup_no_signal_marks_complete() -> (
    None
):
    assessment = assess_pagination(
        json_value={"items": [{"id": 2}]},
        pagination_config=None,
        original_args={"page": 2, "limit": 100},
        upstream_prefix="api",
        tool_name="list_items",
        page_number=1,
    )
    assert assessment is not None
    assert assessment.state is None
    assert assessment.has_more is False
    assert assessment.retrieval_status == "COMPLETE"
    assert assessment.partial_reason is None
    assert assessment.warning is None


def test_assess_pagination_without_config_respects_terminal_signal() -> None:
    assessment = assess_pagination(
        json_value={
            "pageInfo": {
                "hasNextPage": False,
                "endCursor": "CURSOR_LAST_PAGE",
            }
        },
        pagination_config=None,
        original_args={"after": "CURSOR_PREV"},
        upstream_prefix="api",
        tool_name="list_items",
    )
    assert assessment is not None
    assert assessment.state is None
    assert assessment.has_more is False
    assert assessment.retrieval_status == "COMPLETE"
    assert assessment.partial_reason is None
    assert assessment.warning is None


def test_assess_pagination_without_config_empty_next_object_marks_complete() -> (
    None
):
    assessment = assess_pagination(
        json_value={"next": {}},
        pagination_config=None,
        original_args={"page": 2, "limit": 100},
        upstream_prefix="api",
        tool_name="list_items",
    )
    assert assessment is not None
    assert assessment.state is None
    assert assessment.has_more is False
    assert assessment.retrieval_status == "COMPLETE"
    assert assessment.partial_reason is None
    assert assessment.warning is None


def test_assess_pagination_without_config_terminal_signal_without_cursor() -> (
    None
):
    assessment = assess_pagination(
        json_value={"has_more": False, "items": [{"id": 1}]},
        pagination_config=None,
        original_args={"page": 2, "limit": 100},
        upstream_prefix="api",
        tool_name="list_items",
    )
    assert assessment is not None
    assert assessment.state is None
    assert assessment.has_more is False
    assert assessment.retrieval_status == "COMPLETE"
    assert assessment.partial_reason is None
    assert assessment.warning is None


def test_assess_pagination_with_config_skips_discovery(monkeypatch) -> None:
    def _fail_discovery(**_kwargs: object) -> None:
        raise AssertionError("discovery should not run when config exists")

    monkeypatch.setattr(
        "sift_gateway.pagination.extract.discover_pagination",
        _fail_discovery,
    )

    assessment = assess_pagination(
        json_value={"paging": {"next": None}},
        pagination_config=_cursor_config(),
        original_args={"limit": 100},
        upstream_prefix="meta-ads",
        tool_name="get_ads",
    )
    assert assessment is not None
    assert assessment.retrieval_status == "COMPLETE"
    assert assessment.has_more is False
