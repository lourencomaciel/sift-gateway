from __future__ import annotations

import pytest

from sift_mcp.query.jsonpath import (
    JsonPathError,
    Segment,
    canonicalize_jsonpath,
    evaluate_jsonpath,
    parse_jsonpath,
)

# ---------------------------------------------------------------------------
# Existing tests (preserved)
# ---------------------------------------------------------------------------


def test_jsonpath_canonicalizes_bracket_and_dot() -> None:
    assert canonicalize_jsonpath("$['a'].b[0]") == "$.a.b[0]"


def test_jsonpath_evaluates_wildcards_deterministically() -> None:
    doc = {"obj": {"b": 2, "a": 1}}
    assert evaluate_jsonpath(doc, "$.obj[*]") == [1, 2]


def test_jsonpath_rejects_invalid() -> None:
    try:
        evaluate_jsonpath({"a": 1}, "a.b")
    except JsonPathError:
        pass
    else:
        raise AssertionError("expected JsonPathError")


def test_jsonpath_decodes_supported_bracket_escapes() -> None:
    doc = {
        "line\nbreak": "nl",
        "carriage\rreturn": "cr",
        "tab\tchar": "tab",
        "quote'char": "quote",
        "slash\\char": "slash",
    }
    assert evaluate_jsonpath(doc, r"$['line\nbreak']") == ["nl"]
    assert evaluate_jsonpath(doc, r"$['carriage\rreturn']") == ["cr"]
    assert evaluate_jsonpath(doc, r"$['tab\tchar']") == ["tab"]
    assert evaluate_jsonpath(doc, r"$['quote\'char']") == ["quote"]
    assert evaluate_jsonpath(doc, r"$['slash\\char']") == ["slash"]
    assert canonicalize_jsonpath(r"$['line\nbreak']") == r"$['line\nbreak']"


def test_jsonpath_rejects_unsupported_bracket_escape() -> None:
    with pytest.raises(JsonPathError, match="unsupported escape sequence"):
        evaluate_jsonpath({"a": 1}, r"$['a\q']")


def test_jsonpath_enforces_max_length_cap() -> None:
    with pytest.raises(JsonPathError, match="max length"):
        evaluate_jsonpath({"a": 1}, "$.a", max_length=2)


def test_jsonpath_enforces_max_segments_cap() -> None:
    with pytest.raises(JsonPathError, match="max segments"):
        canonicalize_jsonpath("$.a.b", max_segments=1)


def test_jsonpath_enforces_wildcard_expansion_cap() -> None:
    doc = {"items": [1, 2, 3]}
    with pytest.raises(
        JsonPathError, match="wildcard expansion exceeds max total"
    ):
        evaluate_jsonpath(
            doc,
            "$.items[*]",
            max_wildcard_expansion_total=2,
        )


# ---------------------------------------------------------------------------
# G70: JSONPath parser — supported grammar ($, .name, ['..'], [n], [*])
# ---------------------------------------------------------------------------


def test_parse_root_only() -> None:
    segments = parse_jsonpath("$")
    assert segments == []


def test_parse_dot_field() -> None:
    segments = parse_jsonpath("$.name")
    assert segments == [Segment(kind="field", value="name")]


def test_parse_bracket_string_field() -> None:
    segments = parse_jsonpath("$['name']")
    assert segments == [Segment(kind="field", value="name")]


def test_parse_array_index() -> None:
    segments = parse_jsonpath("$.items[0]")
    assert segments == [
        Segment(kind="field", value="items"),
        Segment(kind="index", value=0),
    ]


def test_parse_wildcard() -> None:
    segments = parse_jsonpath("$.items[*]")
    assert segments == [
        Segment(kind="field", value="items"),
        Segment(kind="wildcard", value=None),
    ]


def test_parse_complex_path() -> None:
    segments = parse_jsonpath("$.data[0].items[*].name")
    assert len(segments) == 5
    assert segments[0] == Segment(kind="field", value="data")
    assert segments[1] == Segment(kind="index", value=0)
    assert segments[2] == Segment(kind="field", value="items")
    assert segments[3] == Segment(kind="wildcard", value=None)
    assert segments[4] == Segment(kind="field", value="name")


def test_parse_multiple_brackets() -> None:
    segments = parse_jsonpath("$['a']['b']")
    assert segments == [
        Segment(kind="field", value="a"),
        Segment(kind="field", value="b"),
    ]


def test_parse_rejects_empty_string() -> None:
    with pytest.raises(JsonPathError, match="must start with"):
        parse_jsonpath("")


def test_parse_rejects_no_dollar() -> None:
    with pytest.raises(JsonPathError, match="must start with"):
        parse_jsonpath("foo.bar")


def test_parse_rejects_invalid_token_after_dollar() -> None:
    with pytest.raises(JsonPathError, match="invalid token"):
        parse_jsonpath("$@")


def test_parse_rejects_invalid_dot_field() -> None:
    with pytest.raises(JsonPathError, match="invalid dotted field"):
        parse_jsonpath("$.")


def test_parse_rejects_invalid_array_index() -> None:
    with pytest.raises(JsonPathError, match="invalid array index"):
        parse_jsonpath("$.items[abc]")


def test_parse_rejects_filter_predicate_with_helpful_message() -> None:
    """Filter predicates should give a helpful error, not 'invalid token'."""
    with pytest.raises(JsonPathError, match="filter predicates"):
        parse_jsonpath('$.data[?(@.spend!="0")].ad_name')


def test_parse_rejects_bare_question_mark() -> None:
    """Bare ? after a path should also give the helpful message."""
    with pytest.raises(JsonPathError, match="filter predicates"):
        parse_jsonpath("$.data?")


# ---------------------------------------------------------------------------
# G70: Caps enforcement — length, segments, wildcard expansion total
# ---------------------------------------------------------------------------


def test_parse_max_length_at_boundary() -> None:
    """Path exactly at max_length should succeed."""
    path = "$.a"
    segments = parse_jsonpath(path, max_length=3)
    assert len(segments) == 1


def test_parse_max_length_exceeded() -> None:
    with pytest.raises(JsonPathError, match="max length"):
        parse_jsonpath("$.abc", max_length=3)


def test_parse_max_segments_at_boundary() -> None:
    """Exactly max_segments should succeed."""
    segments = parse_jsonpath("$.a.b", max_segments=2)
    assert len(segments) == 2


def test_parse_max_segments_exceeded() -> None:
    with pytest.raises(JsonPathError, match="max segments"):
        parse_jsonpath("$.a.b.c", max_segments=2)


def test_evaluate_wildcard_expansion_at_boundary() -> None:
    """Expansion exactly at limit should succeed."""
    doc = {"items": [1, 2]}
    result = evaluate_jsonpath(
        doc, "$.items[*]", max_wildcard_expansion_total=2
    )
    assert result == [1, 2]


def test_evaluate_wildcard_expansion_exceeded() -> None:
    doc = {"items": [1, 2, 3]}
    with pytest.raises(
        JsonPathError, match="wildcard expansion exceeds max total"
    ):
        evaluate_jsonpath(doc, "$.items[*]", max_wildcard_expansion_total=2)


def test_evaluate_wildcard_expansion_cumulative() -> None:
    """Multiple wildcard steps should accumulate expansion count."""
    doc = {"a": [{"b": [1, 2]}, {"b": [3]}]}
    # First wildcard expands 2 items, second wildcard expands 2+1=3 more
    # Total = 2 + 3 = 5
    result = evaluate_jsonpath(
        doc, "$.a[*].b[*]", max_wildcard_expansion_total=5
    )
    assert sorted(result) == [1, 2, 3]


def test_evaluate_wildcard_expansion_cumulative_exceeds() -> None:
    doc = {"a": [{"b": [1, 2]}, {"b": [3]}]}
    with pytest.raises(
        JsonPathError, match="wildcard expansion exceeds max total"
    ):
        evaluate_jsonpath(doc, "$.a[*].b[*]", max_wildcard_expansion_total=3)


def test_caps_passed_through_to_evaluate() -> None:
    """evaluate_jsonpath should forward caps to parse_jsonpath."""
    with pytest.raises(JsonPathError, match="max length"):
        evaluate_jsonpath({"a": 1}, "$.abcdef", max_length=5)

    with pytest.raises(JsonPathError, match="max segments"):
        evaluate_jsonpath({"a": {"b": {"c": 1}}}, "$.a.b.c", max_segments=2)


# ---------------------------------------------------------------------------
# G70: Canonicalization
# ---------------------------------------------------------------------------


def test_canonicalize_bracket_to_dot() -> None:
    """Simple identifier bracket fields should be converted to dot notation."""
    assert canonicalize_jsonpath("$['abc']") == "$.abc"


def test_canonicalize_preserves_special_chars_in_brackets() -> None:
    """Non-identifier keys must stay in bracket notation."""
    assert canonicalize_jsonpath("$['a b']") == "$['a b']"


def test_canonicalize_escapes_special_chars() -> None:
    """Characters needing escaping should be escaped in bracket notation."""
    result = canonicalize_jsonpath("$['a\\nb']")
    assert result == "$['a\\nb']"


def test_canonicalize_preserves_index() -> None:
    assert canonicalize_jsonpath("$[0]") == "$[0]"


def test_canonicalize_preserves_wildcard() -> None:
    assert canonicalize_jsonpath("$[*]") == "$[*]"


def test_canonicalize_idempotent() -> None:
    """Canonicalizing an already canonical path should return the same result."""
    path = "$.data[0].items[*].name"
    assert canonicalize_jsonpath(path) == path


def test_canonicalize_mixed_notation() -> None:
    assert (
        canonicalize_jsonpath("$['data'][0]['items'][*]['name']")
        == "$.data[0].items[*].name"
    )


# ---------------------------------------------------------------------------
# G70: Evaluation — various document shapes
# ---------------------------------------------------------------------------


def test_evaluate_root_only() -> None:
    """Evaluating $ returns the document itself."""
    doc = {"a": 1}
    assert evaluate_jsonpath(doc, "$") == [{"a": 1}]


def test_evaluate_nested_field() -> None:
    doc = {"a": {"b": {"c": 42}}}
    assert evaluate_jsonpath(doc, "$.a.b.c") == [42]


def test_evaluate_array_index() -> None:
    doc = {"items": [10, 20, 30]}
    assert evaluate_jsonpath(doc, "$.items[1]") == [20]


def test_evaluate_array_index_out_of_bounds() -> None:
    doc = {"items": [10]}
    assert evaluate_jsonpath(doc, "$.items[5]") == []


def test_evaluate_missing_field() -> None:
    doc = {"a": 1}
    assert evaluate_jsonpath(doc, "$.b") == []


def test_evaluate_wildcard_on_array() -> None:
    doc = {"items": [1, 2, 3]}
    assert evaluate_jsonpath(doc, "$.items[*]") == [1, 2, 3]


def test_evaluate_wildcard_on_dict() -> None:
    """Wildcard on dict iterates values in sorted key order."""
    doc = {"c": 3, "a": 1, "b": 2}
    assert evaluate_jsonpath(doc, "$[*]") == [1, 2, 3]


def test_evaluate_wildcard_on_scalar() -> None:
    """Wildcard on non-collection produces empty list."""
    doc = {"x": 42}
    assert evaluate_jsonpath(doc, "$.x[*]") == []


def test_evaluate_chained_wildcards() -> None:
    doc = {"rows": [{"cols": [1, 2]}, {"cols": [3]}]}
    assert evaluate_jsonpath(doc, "$.rows[*].cols[*]") == [1, 2, 3]


def test_evaluate_field_on_non_dict() -> None:
    """Field access on non-dict returns empty."""
    doc = {"x": [1, 2]}
    assert evaluate_jsonpath(doc, "$.x.y") == []


def test_evaluate_index_on_non_list() -> None:
    """Index access on non-list returns empty."""
    doc = {"x": {"a": 1}}
    assert evaluate_jsonpath(doc, "$.x[0]") == []


def test_evaluate_negative_index_not_supported() -> None:
    """Negative indices are not in the supported grammar (parse rejects them)."""
    with pytest.raises(JsonPathError):
        parse_jsonpath("$.items[-1]")
