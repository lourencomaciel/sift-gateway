"""Consumer-facing pre-execution validation for generated code."""

from __future__ import annotations

import ast
from collections.abc import Sequence
from dataclasses import dataclass

from sift_gateway.codegen.ast_guard import (
    CodeValidationError,
    allowed_import_roots,
    validate_code_ast,
)


@dataclass(frozen=True)
class CodeValidationResult:
    """Outcome of pre-execution code validation.

    Attributes:
        valid: Whether the code passed all checks.
        error_code: Machine-readable error code when invalid.
        error_message: Human-readable message when invalid.
        signature: Detected entrypoint style — ``"legacy"`` for
            ``run(data, schema, params)`` or ``"multi"`` for
            ``run(artifacts, schemas, params)``.  ``None`` when
            the entrypoint could not be determined.
    """

    valid: bool
    error_code: str | None
    error_message: str | None
    signature: str | None


def _detect_signature(module: ast.Module) -> str | None:
    """Detect the ``run`` function signature style.

    Returns ``"legacy"`` for ``run(data, schema, params)`` and
    ``"multi"`` for ``run(artifacts, schemas, params)``.
    """
    for node in module.body:
        if isinstance(node, ast.FunctionDef) and node.name == "run":
            args = [a.arg for a in node.args.args]
            if args == ["data", "schema", "params"]:
                return "legacy"
            if args == ["artifacts", "schemas", "params"]:
                return "multi"
    return None


def validate_code_for_execution(
    code: str,
    *,
    allowed_import_roots_override: Sequence[str] | None = None,
) -> CodeValidationResult:
    """Validate generated code before execution.

    Wraps :func:`~sift_gateway.codegen.ast_guard.validate_code_ast`,
    catching :class:`CodeValidationError` and returning a result
    object instead of raising.

    Args:
        code: Python source code to validate.
        allowed_import_roots_override: Optional import allowlist
            that fully replaces the default policy.

    Returns:
        A ``CodeValidationResult`` describing the outcome.
    """
    import_roots = None
    if allowed_import_roots_override is not None:
        import_roots = allowed_import_roots(
            configured_roots=allowed_import_roots_override,
        )

    try:
        module = validate_code_ast(
            code,
            allowed_import_roots_set=import_roots,
        )
    except CodeValidationError as exc:
        return CodeValidationResult(
            valid=False,
            error_code=exc.code,
            error_message=exc.message,
            signature=None,
        )

    signature = _detect_signature(module)
    return CodeValidationResult(
        valid=True,
        error_code=None,
        error_message=None,
        signature=signature,
    )
