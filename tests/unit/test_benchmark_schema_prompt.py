"""Unit tests for benchmarks.tier1.schema_prompt."""

from __future__ import annotations

from benchmarks.tier1.schema_prompt import (
    _build_nesting_hint,
    _field_has_type,
    _field_path,
    _field_type_names,
    _is_direct_child,
    _split_path_segments,
    format_schema_for_prompt,
)

# -- _field_path / _field_type_names / _field_has_type --


class TestFieldHelpers:
    def test_field_path_prefers_path_key(self) -> None:
        assert _field_path({"path": "$.a", "field_path": "$.b"}) == "$.a"

    def test_field_path_falls_back_to_field_path(self) -> None:
        assert _field_path({"field_path": "$.b"}) == "$.b"

    def test_field_path_default(self) -> None:
        assert _field_path({}) == "?"

    def test_field_type_names_list(self) -> None:
        assert _field_type_names({"types": ["string"]}) == ["string"]

    def test_field_type_names_string(self) -> None:
        assert _field_type_names({"types": "number"}) == ["number"]

    def test_field_type_names_empty(self) -> None:
        assert _field_type_names({}) == []

    def test_field_has_type_exact_match(self) -> None:
        assert _field_has_type({"types": ["array"]}, "array")

    def test_field_has_type_parametric(self) -> None:
        assert _field_has_type({"types": "array<number>"}, "array")

    def test_field_has_type_no_match(self) -> None:
        assert not _field_has_type({"types": ["string"]}, "array")


# -- _is_direct_child --


class TestIsDirectChild:
    def test_dot_notation_child(self) -> None:
        assert _is_direct_child("$[*].country", "$[*].country.en") == "en"

    def test_bracket_notation_child(self) -> None:
        assert (
            _is_direct_child("$[*].data", "$[*].data['special.key']")
            == "special.key"
        )

    def test_not_a_child(self) -> None:
        assert _is_direct_child("$[*].country", "$[*].name") is None

    def test_grandchild_dot(self) -> None:
        assert (
            _is_direct_child("$[*].birth", "$[*].birth.place.country") is None
        )

    def test_grandchild_bracket(self) -> None:
        assert _is_direct_child("$[*].d", "$[*].d['a']['b']") is None

    def test_array_index_not_a_child(self) -> None:
        assert _is_direct_child("$[*].tags", "$[*].tags[*]") is None

    def test_same_path_not_a_child(self) -> None:
        assert _is_direct_child("$[*].x", "$[*].x") is None

    def test_prefix_mismatch(self) -> None:
        assert _is_direct_child("$.alpha", "$.alphabet") is None


# -- _split_path_segments --


class TestSplitPathSegments:
    def test_dot_notation(self) -> None:
        assert _split_path_segments("a.b.c") == ["a", "b", "c"]

    def test_single_segment(self) -> None:
        assert _split_path_segments("name") == ["name"]

    def test_bracket_notation(self) -> None:
        assert _split_path_segments("data['key.one']") == [
            "data",
            "key.one",
        ]

    def test_mixed_notation(self) -> None:
        assert _split_path_segments("data['key.one'].sub") == [
            "data",
            "key.one",
            "sub",
        ]

    def test_empty_string(self) -> None:
        assert _split_path_segments("") == []

    def test_consecutive_brackets(self) -> None:
        assert _split_path_segments("d['a']['b']") == ["d", "a", "b"]


# -- _build_nesting_hint --


class TestBuildNestingHint:
    def test_simple_nesting(self) -> None:
        fields = [
            {"path": "$[*].country", "types": ["object"]},
            {"path": "$[*].country.en", "types": ["string"]},
            {"path": "$[*].country.no", "types": ["string"]},
        ]
        hint = _build_nesting_hint("$[*].country", fields)
        assert hint == '{"en": string, "no": string}'

    def test_no_children(self) -> None:
        fields = [
            {"path": "$[*].name", "types": ["string"]},
        ]
        assert _build_nesting_hint("$[*].name", fields) is None

    def test_skips_nested_children(self) -> None:
        fields = [
            {"path": "$[*].birth", "types": ["object"]},
            {"path": "$[*].birth.date", "types": ["string"]},
            {"path": "$[*].birth.place", "types": ["object"]},
            {"path": "$[*].birth.place.country", "types": ["object"]},
        ]
        hint = _build_nesting_hint("$[*].birth", fields)
        # Should only show direct children, not deeper nesting.
        assert hint == '{"date": string, "place": object}'

    def test_truncates_many_keys(self) -> None:
        fields = [{"path": "$[*].obj", "types": ["object"]}]
        fields.extend(
            {"path": f"$[*].obj.k{i}", "types": ["string"]} for i in range(6)
        )
        hint = _build_nesting_hint("$[*].obj", fields)
        assert hint is not None
        assert hint.endswith(", ...}")
        # Should show at most 4 keys.
        assert hint.count(":") == 4

    def test_mixed_child_types(self) -> None:
        fields = [
            {"path": "$[*].meta", "types": ["object"]},
            {"path": "$[*].meta.id", "types": ["number"]},
            {"path": "$[*].meta.tags", "types": ["array"]},
            {"path": "$[*].meta.active", "types": ["boolean", "null"]},
        ]
        hint = _build_nesting_hint("$[*].meta", fields)
        assert hint == '{"id": number, "tags": array, "active": boolean/null}'

    def test_field_path_key_fallback(self) -> None:
        """Supports legacy field_path key for backward compat."""
        fields = [
            {"field_path": "$.x", "types": ["object"]},
            {"field_path": "$.x.a", "types": ["string"]},
        ]
        hint = _build_nesting_hint("$.x", fields)
        assert hint == '{"a": string}'

    def test_bracket_notation_children(self) -> None:
        """Bracket-notation child paths are detected."""
        fields = [
            {"path": "$[*].data", "types": ["object"]},
            {"path": "$[*].data['key.one']", "types": ["string"]},
            {"path": "$[*].data.simple", "types": ["number"]},
        ]
        hint = _build_nesting_hint("$[*].data", fields)
        assert hint == '{"key.one": string, "simple": number}'

    def test_key_with_quotes_is_escaped(self) -> None:
        """Keys containing quotes are JSON-escaped in the hint."""
        fields = [
            {"path": "$[*].obj", "types": ["object"]},
            {"path": "$[*].obj['a\"b']", "types": ["string"]},
        ]
        hint = _build_nesting_hint("$[*].obj", fields)
        assert hint == '{"a\\"b": string}'

    def test_fallback_field_path_returns_none(self) -> None:
        """Field with unresolvable path (?) is never a child."""
        fields = [
            {"path": "$[*].obj", "types": ["object"]},
            {"types": ["string"]},
        ]
        assert _build_nesting_hint("$[*].obj", fields) is None


# -- format_schema_for_prompt --


class TestFormatSchemaForPrompt:
    def test_columnar_hint_when_object_with_arrays(self) -> None:
        describe = {
            "roots": [
                {
                    "root_path": "$",
                    "count_estimate": 100,
                    "root_shape": "object",
                },
            ],
            "schemas": [
                {
                    "root_path": "$",
                    "fields": [
                        {"path": "$.temp", "types": ["array"]},
                        {"path": "$.humidity", "types": ["array"]},
                        {"path": "$.city", "types": ["array"]},
                    ],
                },
            ],
        }
        result = format_schema_for_prompt(describe)
        assert "IMPORTANT" in result
        assert "columnar" in result
        assert "dict of parallel arrays" in result
        assert 'data["temp"]' in result
        assert "sum(" in result

    def test_columnar_hint_with_string_types(self) -> None:
        """Columnar detection works with legacy string-format types."""
        describe = {
            "roots": [
                {
                    "root_path": "$",
                    "count_estimate": 100,
                    "root_shape": "object",
                },
            ],
            "schemas": [
                {
                    "root_path": "$",
                    "fields": [
                        {"path": "$.temp", "types": "array<number>"},
                        {
                            "path": "$.humidity",
                            "types": "array<number>",
                        },
                        {
                            "path": "$.city",
                            "types": "array<string>",
                        },
                    ],
                },
            ],
        }
        result = format_schema_for_prompt(describe)
        assert "columnar" in result

    def test_columnar_hint_nullable_warning(self) -> None:
        describe = {
            "roots": [
                {
                    "root_path": "$",
                    "count_estimate": 100,
                    "root_shape": "object",
                },
            ],
            "schemas": [
                {
                    "root_path": "$",
                    "fields": [
                        {
                            "path": "$.temp",
                            "types": ["array"],
                            "nullable": True,
                        },
                        {
                            "path": "$.wind",
                            "types": ["array"],
                            "nullable": True,
                        },
                        {"path": "$.time", "types": ["array"]},
                    ],
                },
            ],
        }
        result = format_schema_for_prompt(describe)
        assert "columnar" in result
        assert "is not None" in result
        assert "filter nulls first" in result
        assert "sum(valid)" in result
        assert "len(valid)" in result
        assert 'sum(data["temp"])' not in result
        assert 'len(data["temp"])' not in result

    def test_columnar_hint_no_nullable_warning_when_clean(self) -> None:
        describe = {
            "roots": [
                {
                    "root_path": "$",
                    "count_estimate": 100,
                    "root_shape": "object",
                },
            ],
            "schemas": [
                {
                    "root_path": "$",
                    "fields": [
                        {"path": "$.a", "types": ["array"]},
                        {"path": "$.b", "types": ["array"]},
                    ],
                },
            ],
        }
        result = format_schema_for_prompt(describe)
        assert "columnar" in result
        assert "None/null" not in result
        assert 'sum(data["a"])' in result

    def test_no_columnar_hint_for_array_root(self) -> None:
        describe = {
            "roots": [
                {
                    "root_path": "$",
                    "count_estimate": 50,
                    "root_shape": "array",
                },
            ],
            "schemas": [
                {
                    "root_path": "$",
                    "fields": [
                        {"path": "$.name", "types": ["string"]},
                        {"path": "$.age", "types": ["number"]},
                    ],
                },
            ],
        }
        result = format_schema_for_prompt(describe)
        assert "columnar" not in result

    def test_no_columnar_hint_when_minority_arrays(self) -> None:
        describe = {
            "roots": [
                {
                    "root_path": "$",
                    "count_estimate": 1,
                    "root_shape": "object",
                },
            ],
            "schemas": [
                {
                    "root_path": "$",
                    "fields": [
                        {"path": "$.name", "types": ["string"]},
                        {"path": "$.age", "types": ["number"]},
                        {"path": "$.tags", "types": ["array"]},
                    ],
                },
            ],
        }
        result = format_schema_for_prompt(describe)
        assert "columnar" not in result

    def test_nesting_hint_for_object_fields(self) -> None:
        describe = {
            "roots": [
                {
                    "root_path": "$",
                    "count_estimate": 100,
                    "root_shape": "array",
                },
            ],
            "schemas": [
                {
                    "root_path": "$",
                    "fields": [
                        {"path": "$[*].name", "types": ["string"]},
                        {
                            "path": "$[*].country",
                            "types": ["object"],
                        },
                        {
                            "path": "$[*].country.en",
                            "types": ["string"],
                        },
                        {
                            "path": "$[*].country.no",
                            "types": ["string"],
                        },
                    ],
                },
            ],
        }
        result = format_schema_for_prompt(describe)
        assert 'object {"en": string, "no": string}' in result
        assert "$[*].country.en" in result

    def test_nesting_hint_preserves_union_types(self) -> None:
        """Mixed-type fields show all types, not just 'object'."""
        describe = {
            "roots": [],
            "schemas": [
                {
                    "root_path": "$",
                    "fields": [
                        {
                            "path": "$[*].val",
                            "types": ["object", "string"],
                        },
                        {
                            "path": "$[*].val.en",
                            "types": ["string"],
                        },
                    ],
                },
            ],
        }
        result = format_schema_for_prompt(describe)
        assert 'object/string {"en": string}' in result

    def test_nested_field_hint_for_array_root(self) -> None:
        describe = {
            "roots": [
                {
                    "root_path": "$",
                    "count_estimate": 100,
                    "root_shape": "array",
                },
            ],
            "schemas": [
                {
                    "root_path": "$",
                    "fields": [
                        {"path": "$[*].id", "types": ["number"]},
                        {"path": "$[*].birth", "types": ["object"]},
                        {
                            "path": "$[*].birth.date",
                            "types": ["string"],
                        },
                        {
                            "path": "$[*].birth.place",
                            "types": ["object"],
                        },
                        {
                            "path": "$[*].birth.place.country",
                            "types": ["object"],
                        },
                        {
                            "path": "$[*].birth.place.country.en",
                            "types": ["string"],
                        },
                    ],
                },
            ],
        }
        result = format_schema_for_prompt(describe)
        assert "Nested field access" in result
        assert '["birth"]["place"]["country"]["en"]' in result
        assert (
            'item.get("birth", {}).get("place", {})'
            '.get("country", {}).get("en")'
        ) in result

    def test_no_nested_hint_for_shallow_fields(self) -> None:
        describe = {
            "roots": [
                {
                    "root_path": "$",
                    "count_estimate": 50,
                    "root_shape": "array",
                },
            ],
            "schemas": [
                {
                    "root_path": "$",
                    "fields": [
                        {"path": "$[*].name", "types": ["string"]},
                        {"path": "$[*].age", "types": ["number"]},
                    ],
                },
            ],
        }
        result = format_schema_for_prompt(describe)
        assert "Nested field access" not in result

    def test_nested_hint_bracket_notation(self) -> None:
        describe = {
            "roots": [
                {
                    "root_path": "$",
                    "count_estimate": 100,
                    "root_shape": "array",
                },
            ],
            "schemas": [
                {
                    "root_path": "$",
                    "fields": [
                        {"path": "$[*].id", "types": ["number"]},
                        {
                            "path": "$[*].data",
                            "types": ["object"],
                        },
                        {
                            "path": "$[*].data['key.one']",
                            "types": ["object"],
                        },
                        {
                            "path": "$[*].data['key.one'].val",
                            "types": ["string"],
                        },
                    ],
                },
            ],
        }
        result = format_schema_for_prompt(describe)
        assert "Nested field access" in result
        assert '["data"]["key.one"]["val"]' in result
        assert ('item.get("data", {}).get("key.one", {}).get("val")') in result

    def test_field_paths_displayed_from_path_key(self) -> None:
        """Describe result uses 'path' key, not 'field_path'."""
        describe = {
            "roots": [],
            "schemas": [
                {
                    "root_path": "$",
                    "fields": [
                        {"path": "$[*].id", "types": ["number"]},
                    ],
                },
            ],
        }
        result = format_schema_for_prompt(describe)
        assert "$[*].id" in result
        assert "?" not in result
