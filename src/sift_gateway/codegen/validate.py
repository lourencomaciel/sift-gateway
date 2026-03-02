"""Consumer-facing pre-execution validation for generated code."""

from __future__ import annotations

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
    """

    valid: bool
    error_code: str | None
    error_message: str | None


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
        validate_code_ast(
            code,
            allowed_import_roots_set=import_roots,
        )
    except CodeValidationError as exc:
        return CodeValidationResult(
            valid=False,
            error_code=exc.code,
            error_message=exc.message,
        )

    return CodeValidationResult(
        valid=True,
        error_code=None,
        error_message=None,
    )
