from __future__ import annotations

from mcp_artifact_gateway.query.jsonpath import JsonPathError, canonicalize_jsonpath, evaluate_jsonpath


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
    try:
        evaluate_jsonpath({"a": 1}, r"$['a\q']")
    except JsonPathError as exc:
        assert "unsupported escape sequence" in str(exc)
    else:
        raise AssertionError("expected JsonPathError")
