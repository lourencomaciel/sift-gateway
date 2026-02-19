from __future__ import annotations

from sift_gateway.pagination.discovery import discover_pagination


def test_discover_pagination_next_url_query_params() -> None:
    discovered = discover_pagination(
        json_value={
            "paging": {
                "next": (
                    "https://api.example.test/items?"
                    "limit=100&pageToken=TOKEN_2&access_token=SECRET"
                ),
            }
        },
        original_args={"limit": 100, "pageToken": "TOKEN_1"},
    )
    assert discovered.next_params == {
        "limit": 100,
        "pageToken": "TOKEN_2",
    }
    assert discovered.strategy == "next_url_query"
    assert discovered.has_more is True


def test_discover_pagination_next_url_ignores_access_token_drift() -> None:
    discovered = discover_pagination(
        json_value={
            "next": (
                "https://api.example.test/items?"
                "access_token=SECRET_2&pageToken=TOKEN_2&limit=100"
            ),
        },
        original_args={
            "access_token": "SECRET_1",
            "pageToken": "TOKEN_1",
            "limit": 100,
        },
    )
    assert discovered.next_params == {
        "pageToken": "TOKEN_2",
        "limit": 100,
    }
    assert discovered.strategy == "next_url_query"
    assert discovered.has_more is True


def test_discover_pagination_next_url_reuses_original_param_name() -> None:
    discovered = discover_pagination(
        json_value={
            "next": (
                "https://api.example.test/items?limit=100&page_token=TOKEN_2"
            ),
        },
        original_args={"limit": 100, "pageToken": "TOKEN_1"},
    )
    assert discovered.next_params == {
        "limit": 100,
        "pageToken": "TOKEN_2",
    }
    assert discovered.strategy == "next_url_query"
    assert discovered.has_more is True


def test_discover_pagination_next_url_supports_relative_query_only() -> None:
    discovered = discover_pagination(
        json_value={
            "next": "?limit=100&after=CUR_2",
        },
        original_args={"limit": 100, "after": "CUR_1"},
    )
    assert discovered.next_params == {
        "limit": 100,
        "after": "CUR_2",
    }
    assert discovered.strategy == "next_url_query"
    assert discovered.has_more is True


def test_discover_pagination_cursor_path_graphql_shape() -> None:
    discovered = discover_pagination(
        json_value={
            "pageInfo": {
                "hasNextPage": True,
                "endCursor": "CURSOR_2",
            }
        },
        original_args={},
    )
    assert discovered.next_params == {"after": "CURSOR_2"}
    assert discovered.strategy == "cursor_path"
    assert discovered.has_more is True


def test_discover_pagination_cursor_path_skips_non_advancing_value() -> None:
    discovered = discover_pagination(
        json_value={"cursor": "CURSOR_2"},
        original_args={"cursor": "CURSOR_2", "limit": 100},
    )
    assert discovered.next_params is None
    assert discovered.strategy is None
    assert discovered.has_more is None


def test_discover_pagination_next_object_params() -> None:
    discovered = discover_pagination(
        json_value={
            "has_more": True,
            "next": {"offset": 200, "foo": "bar"},
        },
        original_args={"offset": 100, "limit": 100},
    )
    assert discovered.next_params == {"offset": 200}
    assert discovered.strategy == "next_object"
    assert discovered.has_more is True


def test_discover_pagination_next_object_reuses_original_arg_name() -> None:
    discovered = discover_pagination(
        json_value={
            "has_more": True,
            "next": {"page_token": "TOKEN_2"},
        },
        original_args={"pageToken": "TOKEN_1", "limit": 100},
    )
    assert discovered.next_params == {"pageToken": "TOKEN_2"}
    assert discovered.strategy == "next_object"
    assert discovered.has_more is True


def test_discover_pagination_empty_next_object_is_terminal() -> None:
    discovered = discover_pagination(
        json_value={"next": {}},
        original_args={"page": 1, "limit": 100},
    )
    assert discovered.has_more is False
    assert discovered.next_params is None
    assert discovered.strategy is None


def test_discover_pagination_numeric_progression_from_args() -> None:
    discovered = discover_pagination(
        json_value={"has_more": True, "items": [1, 2, 3]},
        original_args={"offset": 100, "limit": 100},
    )
    assert discovered.next_params == {"offset": 200}
    assert discovered.strategy == "numeric_args"
    assert discovered.has_more is True


def test_discover_pagination_numeric_progression_from_zero_offset() -> None:
    discovered = discover_pagination(
        json_value={"has_more": True, "items": [1, 2, 3]},
        original_args={"offset": 0, "limit": 100},
    )
    assert discovered.next_params == {"offset": 100}
    assert discovered.strategy == "numeric_args"
    assert discovered.has_more is True


def test_discover_pagination_numeric_progression_from_page_arg() -> None:
    discovered = discover_pagination(
        json_value={"has_more": True, "items": [1]},
        original_args={"page": 3},
    )
    assert discovered.next_params == {"page": 4}
    assert discovered.strategy == "numeric_args"
    assert discovered.has_more is True


def test_discover_pagination_numeric_progression_from_zero_page() -> None:
    discovered = discover_pagination(
        json_value={"has_more": True, "items": [1]},
        original_args={"page": 0},
    )
    assert discovered.next_params == {"page": 1}
    assert discovered.strategy == "numeric_args"
    assert discovered.has_more is True


def test_discover_pagination_numeric_progression_from_limit_hit() -> None:
    discovered = discover_pagination(
        json_value={"data": [{"id": i} for i in range(50)]},
        original_args={"offset": 100, "limit": 50},
    )
    assert discovered.next_params == {"offset": 150}
    assert discovered.strategy == "numeric_args"
    assert discovered.has_more is True


def test_discover_pagination_reports_limit_hit() -> None:
    discovered = discover_pagination(
        json_value={"data": [{"id": i} for i in range(100)]},
        original_args={"limit": 100},
    )
    assert discovered.next_params is None
    assert discovered.limit_hit is True


def test_discover_pagination_no_signal() -> None:
    discovered = discover_pagination(
        json_value={"items": [{"id": 1}]},
        original_args={"q": "search"},
    )
    assert discovered.has_more is None
    assert discovered.next_params is None
    assert discovered.strategy is None
    assert discovered.limit_hit is False


def test_discover_pagination_header_link_next() -> None:
    discovered = discover_pagination(
        json_value={"items": [{"id": 1}]},
        original_args={"limit": 100, "after": "CUR_1"},
        upstream_meta={
            "headers": {
                "Link": (
                    "<https://api.example.test/items?limit=100&after=CUR_2>; "
                    'rel="next", <https://api.example.test/items?limit=100>; '
                    'rel="prev"'
                ),
            }
        },
    )
    assert discovered.next_params == {"limit": 100, "after": "CUR_2"}
    assert discovered.strategy == "header_link"
    assert discovered.has_more is True


def test_discover_pagination_header_cursor_token() -> None:
    discovered = discover_pagination(
        json_value={"items": [{"id": 1}]},
        original_args={"after": "CUR_1"},
        upstream_meta={"headers": {"x-next-cursor": "CUR_2"}},
    )
    assert discovered.next_params == {"after": "CUR_2"}
    assert discovered.strategy == "header_cursor"
    assert discovered.has_more is True


def test_discover_pagination_header_cursor_skips_non_advancing_value() -> None:
    discovered = discover_pagination(
        json_value={"items": [{"id": 1}]},
        original_args={"after": "CUR_2"},
        upstream_meta={"headers": {"x-next-cursor": "CUR_2"}},
    )
    assert discovered.has_more is True
    assert discovered.next_params is None
    assert discovered.strategy is None


def test_discover_pagination_header_page_token_reuses_request_key() -> None:
    discovered = discover_pagination(
        json_value={"items": [{"id": 1}]},
        original_args={"nextPageToken": "TOKEN_1"},
        upstream_meta={"headers": {"x-next-page-token": "TOKEN_2"}},
    )
    assert discovered.next_params == {"nextPageToken": "TOKEN_2"}
    assert discovered.strategy == "header_cursor"
    assert discovered.has_more is True


def test_discover_pagination_header_precedes_numeric_inference() -> None:
    discovered = discover_pagination(
        json_value={"items": [{"id": 1}]},
        original_args={"page": 1, "limit": 100},
        upstream_meta={
            "headers": {
                "Link": (
                    "<https://api.example.test/items?page=99&after=CUR_2>; "
                    'rel="next"'
                ),
            },
        },
    )
    assert discovered.next_params == {"page": 99, "after": "CUR_2"}
    assert discovered.strategy == "header_link"
    assert discovered.has_more is True


def test_discover_pagination_header_next_url_key() -> None:
    discovered = discover_pagination(
        json_value={"items": [{"id": 1}]},
        original_args={"offset": 100, "limit": 100},
        upstream_meta={
            "headers": {
                "x-next-url": (
                    "https://api.example.test/items?offset=200&limit=100"
                ),
            },
        },
    )
    assert discovered.next_params == {"offset": 200, "limit": 100}
    assert discovered.strategy == "header_next_url"
    assert discovered.has_more is True


def test_discover_pagination_header_list_value_supported() -> None:
    discovered = discover_pagination(
        json_value={"items": [{"id": 1}]},
        original_args={"after": "CUR_1"},
        upstream_meta={
            "headers": {
                "Link": [
                    '<https://api.example.test/items?after=CUR_2>; rel="next"',
                ],
            },
        },
    )
    assert discovered.next_params == {"after": "CUR_2"}
    assert discovered.strategy == "header_link"
    assert discovered.has_more is True


def test_discover_pagination_header_list_uses_rel_next_not_first_entry() -> (
    None
):
    discovered = discover_pagination(
        json_value={"items": [{"id": 1}]},
        original_args={"after": "CUR_1"},
        upstream_meta={
            "headers": {
                "Link": [
                    '<https://api.example.test/items?after=CUR_0>; rel="prev"',
                    '<https://api.example.test/items?after=CUR_2>; rel="next"',
                ],
            },
        },
    )
    assert discovered.next_params == {"after": "CUR_2"}
    assert discovered.strategy == "header_link"
    assert discovered.has_more is True


def test_discover_pagination_header_link_rel_not_first_attribute() -> None:
    discovered = discover_pagination(
        json_value={"items": [{"id": 1}]},
        original_args={"after": "CUR_1"},
        upstream_meta={
            "headers": {
                "Link": (
                    "<https://api.example.test/items?after=CUR_2>; "
                    'title="next"; rel="next"'
                ),
            },
        },
    )
    assert discovered.next_params == {"after": "CUR_2"}
    assert discovered.strategy == "header_link"
    assert discovered.has_more is True


def test_discover_pagination_header_has_more_false_without_next_params() -> (
    None
):
    discovered = discover_pagination(
        json_value={"items": [{"id": 1}]},
        original_args={"limit": 100},
        upstream_meta={"headers": {"x-has-more": "false"}},
    )
    assert discovered.has_more is False
    assert discovered.next_params is None
    assert discovered.strategy is None


def test_discover_pagination_cursor_path_next_page_token() -> None:
    discovered = discover_pagination(
        json_value={"nextPageToken": "TOKEN_2"},
        original_args={"nextPageToken": "TOKEN_1"},
    )
    assert discovered.next_params == {"nextPageToken": "TOKEN_2"}
    assert discovered.strategy == "cursor_path"
    assert discovered.has_more is True


def test_discover_pagination_next_url_allows_page_token_without_original_arg() -> (
    None
):
    discovered = discover_pagination(
        json_value={
            "next": (
                "https://api.example.test/items?limit=100&pageToken=TOKEN_2"
            ),
        },
        original_args={"limit": 100},
    )
    assert discovered.next_params == {
        "limit": 100,
        "pageToken": "TOKEN_2",
    }
    assert discovered.strategy == "next_url_query"
    assert discovered.has_more is True


def test_discover_pagination_skips_non_advancing_url_query_params() -> None:
    discovered = discover_pagination(
        json_value={
            "next": "https://api.example.test/items/page/2?limit=100",
        },
        original_args={"limit": 100},
    )
    assert discovered.has_more is True
    assert discovered.next_params is None
    assert discovered.strategy is None


def test_discover_pagination_non_advancing_url_falls_back_to_numeric() -> None:
    discovered = discover_pagination(
        json_value={
            "next": "https://api.example.test/items?page=1&limit=200",
        },
        original_args={"page": 1, "limit": 100},
    )
    assert discovered.has_more is True
    assert discovered.next_params == {"page": 2}
    assert discovered.strategy == "numeric_args"


def test_discover_pagination_next_object_non_advancing_falls_back_to_numeric() -> (
    None
):
    discovered = discover_pagination(
        json_value={"has_more": True, "next": {"page": 1}},
        original_args={"page": 1, "limit": 100},
    )
    assert discovered.has_more is True
    assert discovered.next_params == {"page": 2}
    assert discovered.strategy == "numeric_args"
