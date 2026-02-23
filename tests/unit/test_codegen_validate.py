"""Unit tests for sift_gateway.codegen.validate."""

from __future__ import annotations

from sift_gateway.codegen.validate import validate_code_for_execution


class TestValidateCodeForExecution:
    def test_valid_legacy_signature(self) -> None:
        code = "def run(data, schema, params):\n    return len(data)"
        result = validate_code_for_execution(code)
        assert result.valid is True
        assert result.signature == "legacy"
        assert result.error_code is None
        assert result.error_message is None

    def test_valid_multi_signature(self) -> None:
        code = "def run(artifacts, schemas, params):\n    return len(artifacts)"
        result = validate_code_for_execution(code)
        assert result.valid is True
        assert result.signature == "multi"

    def test_blocked_import(self) -> None:
        code = (
            "import subprocess\n"
            "def run(data, schema, params):\n"
            "    return subprocess.run(['ls'])"
        )
        result = validate_code_for_execution(code)
        assert result.valid is False
        assert result.error_code == "CODE_IMPORT_NOT_ALLOWED"
        assert result.error_message is not None

    def test_missing_entrypoint(self) -> None:
        code = "def helper(x):\n    return x + 1"
        result = validate_code_for_execution(code)
        assert result.valid is False
        assert result.error_code == "CODE_ENTRYPOINT_MISSING"

    def test_syntax_error(self) -> None:
        code = "def run(data, schema, params:\n    return data"
        result = validate_code_for_execution(code)
        assert result.valid is False
        assert result.error_code == "CODE_AST_REJECTED"

    def test_custom_import_roots(self) -> None:
        code = (
            "import math\n"
            "def run(data, schema, params):\n"
            "    return math.sqrt(len(data))"
        )
        result = validate_code_for_execution(
            code,
            allowed_import_roots_override=["math"],
        )
        assert result.valid is True

    def test_custom_import_roots_blocks_default(self) -> None:
        code = (
            "import json\n"
            "def run(data, schema, params):\n"
            "    return json.dumps(data)"
        )
        # With a restrictive allowlist, json is blocked.
        result = validate_code_for_execution(
            code,
            allowed_import_roots_override=["math"],
        )
        assert result.valid is False
        assert result.error_code == "CODE_IMPORT_NOT_ALLOWED"
