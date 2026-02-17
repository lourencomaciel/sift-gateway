"""Unit tests for code-query AST safety validation."""

from __future__ import annotations

import pytest

from sift_mcp.codegen.ast_guard import (
    CodeValidationError,
    allowed_import_roots,
    validate_code_ast,
)


def test_validate_code_ast_accepts_allowed_imports_and_entrypoint() -> None:
    code = """
import math
import pandas as pd

def helper(x):
    return math.floor(x)

def run(data, schema, params):
    return [helper(1.9), len(data), bool(schema), bool(params)]
"""
    module = validate_code_ast(code)
    assert module is not None


def test_validate_code_ast_accepts_multi_artifact_signature() -> None:
    code = """
def run(artifacts, schemas, params):
    return [{"count": len(artifacts)}]
"""
    module = validate_code_ast(code)
    assert module is not None


def test_validate_code_ast_rejects_forbidden_import() -> None:
    code = """
import os

def run(data, schema, params):
    return []
"""
    with pytest.raises(CodeValidationError) as exc:
        validate_code_ast(code)
    assert exc.value.code == "CODE_IMPORT_NOT_ALLOWED"


def test_validate_code_ast_requires_run_entrypoint() -> None:
    code = "def nope():\n    return 1\n"
    with pytest.raises(CodeValidationError) as exc:
        validate_code_ast(code)
    assert exc.value.code == "CODE_ENTRYPOINT_MISSING"


def test_validate_code_ast_rejects_blocked_builtin_calls() -> None:
    code = """
def run(data, schema, params):
    return open('/tmp/x')
"""
    with pytest.raises(CodeValidationError) as exc:
        validate_code_ast(code)
    assert exc.value.code == "CODE_AST_REJECTED"


def test_validate_code_ast_rejects_dunder_attribute_access() -> None:
    code = """
def run(data, schema, params):
    return data.__class__
"""
    with pytest.raises(CodeValidationError) as exc:
        validate_code_ast(code)
    assert exc.value.code == "CODE_AST_REJECTED"


def test_validate_code_ast_rejects_import_not_in_configured_allowlist() -> None:
    code = """
import numpy as np

def run(data, schema, params):
    return int(np.array([1, 2, 3]).sum())
"""
    with pytest.raises(CodeValidationError) as exc:
        validate_code_ast(
            code,
            allowed_import_roots_set=allowed_import_roots(
                configured_roots=["math", "json"]
            ),
        )
    assert exc.value.code == "CODE_IMPORT_NOT_ALLOWED"


def test_allowed_import_roots_honors_configured_override() -> None:
    roots = allowed_import_roots(
        configured_roots=["math", "json"],
    )
    assert roots == {"math", "json"}


@pytest.mark.parametrize(
    "module_name",
    ["csv", "io", "string", "textwrap"],
)
def test_validate_code_ast_accepts_new_stdlib_imports(
    module_name: str,
) -> None:
    code = f"""
import {module_name}

def run(data, schema, params):
    return []
"""
    module = validate_code_ast(code)
    assert module is not None


def test_validate_code_ast_accepts_io_stringio() -> None:
    code = """
import io

def run(data, schema, params):
    buf = io.StringIO()
    buf.write("hello")
    return [buf.getvalue()]
"""
    module = validate_code_ast(code)
    assert module is not None


def test_validate_code_ast_accepts_io_bytesio() -> None:
    code = """
import io

def run(data, schema, params):
    buf = io.BytesIO(b"hello")
    return [len(buf.getvalue())]
"""
    module = validate_code_ast(code)
    assert module is not None


@pytest.mark.parametrize(
    "blocked_attr",
    ["open", "FileIO", "BufferedReader", "BufferedWriter", "BufferedRandom"],
)
def test_validate_code_ast_rejects_io_file_access(
    blocked_attr: str,
) -> None:
    code = f"""
import io

def run(data, schema, params):
    return io.{blocked_attr}("/tmp/x")
"""
    with pytest.raises(CodeValidationError) as exc:
        validate_code_ast(code)
    assert exc.value.code == "CODE_AST_REJECTED"
    assert f"io.{blocked_attr}" in exc.value.message
