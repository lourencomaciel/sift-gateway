"""AST-level safety checks for generated Python code queries."""

from __future__ import annotations

import ast
from dataclasses import dataclass
from typing import Sequence

ALLOWED_STDLIB_IMPORTS = frozenset(
    {
        "math",
        "statistics",
        "decimal",
        "datetime",
        "re",
        "itertools",
        "collections",
        "functools",
        "operator",
        "heapq",
        "json",
    }
)

CORE_THIRD_PARTY_IMPORTS = frozenset(
    {
        "jmespath",
    }
)

ANALYTICS_IMPORT_ROOTS = frozenset(
    {
        "pandas",
        "numpy",
    }
)

ALLOWED_THIRD_PARTY_IMPORTS = frozenset(
    {*CORE_THIRD_PARTY_IMPORTS, *ANALYTICS_IMPORT_ROOTS}
)

ALLOWED_IMPORT_ROOTS = frozenset(
    {*ALLOWED_STDLIB_IMPORTS, *ALLOWED_THIRD_PARTY_IMPORTS}
)

_BLOCKED_BUILTINS = frozenset(
    {
        "eval",
        "exec",
        "compile",
        "open",
        "input",
        "__import__",
        "globals",
        "locals",
        "vars",
        "dir",
        "getattr",
        "setattr",
        "delattr",
        "breakpoint",
    }
)

_BLOCKED_NAME_PATTERNS = frozenset(
    {
        "os",
        "sys",
        "subprocess",
        "socket",
        "pathlib",
        "shutil",
        "importlib",
        "builtins",
    }
)


@dataclass(frozen=True)
class CodeValidationError(ValueError):
    """Validation error raised when generated code violates policy."""

    code: str
    message: str

    def __str__(self) -> str:
        """Return the human-readable validation message."""
        return self.message


def _import_root(name: str) -> str:
    """Return the top-level module root for an import name."""
    return name.split(".", 1)[0].strip()


def allowed_import_roots(
    *,
    allow_analytics_imports: bool = True,
    configured_roots: Sequence[str] | None = None,
) -> frozenset[str]:
    """Return import allowlist roots for the requested runtime profile.

    When ``configured_roots`` is provided, it fully overrides the
    default policy roots.
    """
    if configured_roots is not None:
        return frozenset(
            root.strip()
            for root in configured_roots
            if isinstance(root, str) and root.strip()
        )
    third_party = set(CORE_THIRD_PARTY_IMPORTS)
    if allow_analytics_imports:
        third_party.update(ANALYTICS_IMPORT_ROOTS)
    return frozenset({*ALLOWED_STDLIB_IMPORTS, *third_party})


def _ensure_allowed_import(
    root: str,
    *,
    raw: str,
    allowed_import_roots_set: frozenset[str],
) -> None:
    """Raise when an import root is not allowlisted."""
    if root not in allowed_import_roots_set:
        raise CodeValidationError(
            code="CODE_IMPORT_NOT_ALLOWED",
            message=f"import not allowed: {raw}",
        )


def _validate_run_signature(node: ast.FunctionDef) -> None:
    """Validate supported `run` signatures exactly."""
    allowed = [
        ["data", "schema", "params"],
        ["artifacts", "schemas", "params"],
    ]
    actual = [arg.arg for arg in node.args.args]
    if actual not in allowed:
        raise CodeValidationError(
            code="CODE_ENTRYPOINT_MISSING",
            message=(
                "run must have signature run(data, schema, params) "
                "or run(artifacts, schemas, params)"
            ),
        )
    if node.args.vararg is not None or node.args.kwarg is not None:
        raise CodeValidationError(
            code="CODE_ENTRYPOINT_MISSING",
            message="run may not use *args/**kwargs",
        )


def validate_code_ast(
    code: str,
    *,
    allowed_import_roots_set: frozenset[str] | None = None,
) -> ast.Module:
    """Parse and validate code AST against runtime safety policy.

    Args:
        code: Generated Python source code.
        allowed_import_roots_set: Optional import root allowlist.
            Defaults to the module policy (`ALLOWED_IMPORT_ROOTS`).

    Returns:
        Parsed AST module when validation succeeds.

    Raises:
        CodeValidationError: When policy checks fail.
    """
    try:
        module = ast.parse(code, mode="exec")
    except SyntaxError as exc:
        raise CodeValidationError(
            code="CODE_AST_REJECTED",
            message=f"syntax error: {exc.msg}",
        ) from exc

    run_node: ast.FunctionDef | None = None
    for node in module.body:
        if isinstance(node, ast.FunctionDef) and node.name == "run":
            run_node = node
            break

    if run_node is None:
        raise CodeValidationError(
            code="CODE_ENTRYPOINT_MISSING",
            message=(
                "code must define run(data, schema, params) "
                "or run(artifacts, schemas, params)"
            ),
        )
    _validate_run_signature(run_node)

    import_roots = allowed_import_roots_set or ALLOWED_IMPORT_ROOTS

    for module_node in ast.walk(module):
        if isinstance(module_node, ast.Import):
            for alias in module_node.names:
                root = _import_root(alias.name)
                _ensure_allowed_import(
                    root,
                    raw=alias.name,
                    allowed_import_roots_set=import_roots,
                )
        elif isinstance(module_node, ast.ImportFrom):
            if module_node.level and module_node.level > 0:
                raise CodeValidationError(
                    code="CODE_IMPORT_NOT_ALLOWED",
                    message="relative imports are not allowed",
                )
            if module_node.module is None:
                raise CodeValidationError(
                    code="CODE_IMPORT_NOT_ALLOWED",
                    message="import source missing",
                )
            root = _import_root(module_node.module)
            _ensure_allowed_import(
                root,
                raw=module_node.module,
                allowed_import_roots_set=import_roots,
            )
        elif isinstance(module_node, ast.Attribute):
            if module_node.attr.startswith("__"):
                raise CodeValidationError(
                    code="CODE_AST_REJECTED",
                    message="dunder attribute access is not allowed",
                )
        elif isinstance(module_node, ast.Call):
            fn = module_node.func
            if isinstance(fn, ast.Name) and fn.id in _BLOCKED_BUILTINS:
                raise CodeValidationError(
                    code="CODE_AST_REJECTED",
                    message=f"blocked builtin call: {fn.id}",
                )
            if isinstance(fn, ast.Attribute):
                base = fn.value
                if isinstance(base, ast.Name) and base.id in {
                    "importlib",
                    "builtins",
                }:
                    raise CodeValidationError(
                        code="CODE_AST_REJECTED",
                        message="dynamic import helpers are not allowed",
                    )
        elif isinstance(module_node, ast.Name):
            if module_node.id in _BLOCKED_NAME_PATTERNS and isinstance(
                module_node.ctx, ast.Load
            ):
                raise CodeValidationError(
                    code="CODE_AST_REJECTED",
                    message=f"blocked name access: {module_node.id}",
                )

    return module
