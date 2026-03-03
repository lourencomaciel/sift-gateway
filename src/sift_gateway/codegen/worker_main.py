"""Worker subprocess entrypoint for executing generated code queries."""

from __future__ import annotations

import ast
import asyncio
import builtins as _builtins
from functools import cache
import importlib.util
import inspect
from pathlib import Path
import sys
import traceback
from typing import Any

from sift_gateway.codegen.ast_guard import (
    ALLOWED_IMPORT_ROOTS,
    RUN_SIGNATURE_LEGACY,
    RUN_SIGNATURE_MULTI,
    CodeValidationError,
    validate_code_ast,
)
from sift_gateway.codegen.runtime import decode_json_bytes, encode_json_bytes

_ALLOWED_IMPORT_ROOTS: frozenset[str] = ALLOWED_IMPORT_ROOTS
_STDLIB_IMPORT_ROOTS = frozenset(sys.stdlib_module_names)
_TRUSTED_IMPORTED_STDLIB_ROOTS: set[str] = set()

_BLOCKED_BUILTINS = frozenset(
    {
        "eval",
        "exec",
        "compile",
        "open",
        "input",
        "globals",
        "locals",
        "vars",
        "dir",
        "getattr",
        "setattr",
        "delattr",
        "breakpoint",
        "quit",
        "exit",
    }
)


_ALLOWED_URLLIB_MODULES = frozenset({"urllib", "urllib.parse"})


def _safe_import(
    name: str,
    globals: dict[str, Any] | None = None,
    locals: dict[str, Any] | None = None,
    fromlist: tuple[str, ...] | list[str] = (),
    level: int = 0,
) -> Any:
    if level != 0:
        raise ImportError("relative imports are not allowed")
    root = name.split(".", 1)[0]
    if root not in _ALLOWED_IMPORT_ROOTS and not _is_allowed_transitive_stdlib_import(
        name=name,
        globals_dict=globals,
    ):
        raise ImportError(f"import not allowed: {name}")
    if root == "urllib" and name not in _ALLOWED_URLLIB_MODULES:
        raise ImportError(
            f"import not allowed: {name} (only urllib.parse is permitted)"
        )
    imported = _builtins.__import__(name, globals, locals, fromlist, level)
    if root in _ALLOWED_IMPORT_ROOTS and root in _STDLIB_IMPORT_ROOTS:
        _TRUSTED_IMPORTED_STDLIB_ROOTS.add(root)
    return imported


def _is_allowed_transitive_stdlib_import(
    *,
    name: str,
    globals_dict: dict[str, Any] | None,
) -> bool:
    """Allow stdlib-to-stdlib transitive imports from allowlisted stdlib roots.

    This keeps direct user imports policy-constrained while allowing internal
    stdlib imports (for example ``datetime`` loading ``_strptime``).
    """
    root = name.split(".", 1)[0]
    if root in _ALLOWED_IMPORT_ROOTS:
        return True
    if root not in _STDLIB_IMPORT_ROOTS:
        return False
    if not isinstance(globals_dict, dict):
        if not root.startswith("_") or not _TRUSTED_IMPORTED_STDLIB_ROOTS:
            return False
        return any(
            root in _transitive_stdlib_import_roots(trusted_root)
            for trusted_root in _TRUSTED_IMPORTED_STDLIB_ROOTS
        )
    importer_name = globals_dict.get("__name__")
    if isinstance(importer_name, str) and importer_name:
        importer_root = importer_name.split(".", 1)[0]
        if (
            importer_root in _ALLOWED_IMPORT_ROOTS
            and importer_root in _STDLIB_IMPORT_ROOTS
        ):
            return True
        if importer_name != "__main__":
            return False
    if not root.startswith("_") or not _TRUSTED_IMPORTED_STDLIB_ROOTS:
        return False
    return any(
        root in _transitive_stdlib_import_roots(trusted_root)
        for trusted_root in _TRUSTED_IMPORTED_STDLIB_ROOTS
    )


@cache
def _direct_stdlib_import_roots(module_name: str) -> frozenset[str]:
    """Return direct stdlib import roots referenced by a stdlib module."""
    try:
        spec = importlib.util.find_spec(module_name)
    except (ImportError, ValueError):
        return frozenset()
    if spec is None:
        return frozenset()
    origin = spec.origin
    if not isinstance(origin, str) or not origin.endswith(".py"):
        return frozenset()
    try:
        source = Path(origin).read_text(encoding="utf-8")
    except OSError:
        return frozenset()
    try:
        module = ast.parse(source, mode="exec")
    except SyntaxError:
        return frozenset()

    package_root = module_name.split(".", 1)[0]
    imports: set[str] = set()
    for node in ast.walk(module):
        if isinstance(node, ast.Import):
            for alias in node.names:
                root = alias.name.split(".", 1)[0]
                if root in _STDLIB_IMPORT_ROOTS:
                    imports.add(root)
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                root = node.module.split(".", 1)[0]
                if root in _STDLIB_IMPORT_ROOTS:
                    imports.add(root)
            elif node.level and package_root in _STDLIB_IMPORT_ROOTS:
                imports.add(package_root)
    return frozenset(imports)


@cache
def _transitive_stdlib_import_roots(module_root: str) -> frozenset[str]:
    """Return recursively discovered stdlib import roots for ``module_root``."""
    if module_root not in _STDLIB_IMPORT_ROOTS:
        return frozenset()
    discovered: set[str] = set()
    pending = [module_root]
    visited: set[str] = set()
    while pending:
        current = pending.pop()
        if current in visited:
            continue
        visited.add(current)
        for dep in _direct_stdlib_import_roots(current):
            if dep in discovered:
                continue
            discovered.add(dep)
            pending.append(dep)
    return frozenset(discovered)


def _safe_builtins() -> dict[str, Any]:
    allowed = dict(_builtins.__dict__)
    for name in _BLOCKED_BUILTINS:
        allowed.pop(name, None)
    allowed["__import__"] = _safe_import
    return allowed


def _worker_error(
    code: str,
    message: str,
    *,
    tb: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "ok": False,
        "error": {
            "code": code,
            "message": message,
        },
    }
    if tb:
        payload["error"]["traceback"] = tb[:2000]
    return payload


def _validate_run_callable(
    run_fn: Any,
) -> tuple[str, CodeValidationError | None]:
    if not callable(run_fn):
        return "invalid", CodeValidationError(
            code="CODE_ENTRYPOINT_MISSING",
            message=(
                "run(artifacts, schemas, params) "
                "or run(data, schema, params) is not callable"
            ),
        )
    sig = inspect.signature(run_fn)
    names = list(sig.parameters.keys())
    if names == RUN_SIGNATURE_MULTI:
        return "multi", None
    if names == RUN_SIGNATURE_LEGACY:
        return "legacy", None
    return "invalid", CodeValidationError(
        code="CODE_ENTRYPOINT_MISSING",
        message=(
            "run must have signature "
            "run(artifacts, schemas, params) "
            "or run(data, schema, params)"
        ),
    )


def _strip_locators(records: list[Any]) -> None:
    """Remove ``_locator`` metadata in place before user code runs.

    The ``_locator`` dict is entirely gateway-constructed.  Scalar
    wrappers are identified by ``_locator["_scalar"] is True`` — a
    flag that only the scalar branch of ``_with_locator`` sets — and
    unwrapped to their ``value``.  Dict records have ``_locator``
    removed.
    """
    for i, record in enumerate(records):
        if not isinstance(record, dict) or "_locator" not in record:
            continue
        locator = record["_locator"]
        if (
            isinstance(locator, dict)
            and locator.get("_scalar") is True
            and "value" in record
        ):
            records[i] = record["value"]
        else:
            del record["_locator"]


def _execute(payload: dict[str, Any]) -> dict[str, Any]:
    global _ALLOWED_IMPORT_ROOTS

    code_val = payload.get("code")
    artifacts_val = payload.get("artifacts")
    schemas_val = payload.get("schemas")
    params_val = payload.get("params")
    allowed_roots_val = payload.get("allowed_import_roots")

    if not isinstance(code_val, str):
        return _worker_error("CODE_AST_REJECTED", "missing code string")
    if not isinstance(params_val, dict):
        return _worker_error("CODE_AST_REJECTED", "params must be an object")

    if artifacts_val is None and schemas_val is None:
        legacy_data = payload.get("data")
        legacy_schema = payload.get("schema")
        if isinstance(legacy_data, list) and isinstance(legacy_schema, dict):
            artifacts_val = {"__single__": legacy_data}
            schemas_val = {"__single__": legacy_schema}

    if not isinstance(artifacts_val, dict):
        return _worker_error("CODE_AST_REJECTED", "artifacts must be an object")
    if not isinstance(schemas_val, dict):
        return _worker_error("CODE_AST_REJECTED", "schemas must be an object")

    for artifact_id, rows in artifacts_val.items():
        if not isinstance(artifact_id, str):
            return _worker_error(
                "CODE_AST_REJECTED",
                "artifacts keys must be strings",
            )
        if not isinstance(rows, list):
            return _worker_error(
                "CODE_AST_REJECTED",
                "artifacts values must be lists",
            )

    for artifact_id, schema in schemas_val.items():
        if not isinstance(artifact_id, str):
            return _worker_error(
                "CODE_AST_REJECTED",
                "schemas keys must be strings",
            )
        if not isinstance(schema, dict):
            return _worker_error(
                "CODE_AST_REJECTED",
                "schemas values must be objects",
            )

    allowed_roots: frozenset[str]
    if allowed_roots_val is None:
        allowed_roots = ALLOWED_IMPORT_ROOTS
    elif isinstance(allowed_roots_val, list) and all(
        isinstance(item, str) for item in allowed_roots_val
    ):
        allowed_roots = frozenset(allowed_roots_val)
    else:
        return _worker_error(
            "CODE_AST_REJECTED",
            "allowed_import_roots must be a list of strings",
        )

    _ALLOWED_IMPORT_ROOTS = allowed_roots
    _TRUSTED_IMPORTED_STDLIB_ROOTS.clear()

    try:
        module = validate_code_ast(
            code_val,
            allowed_import_roots_set=allowed_roots,
        )
    except CodeValidationError as exc:
        return _worker_error(exc.code, exc.message)

    globals_dict: dict[str, Any] = {"__builtins__": _safe_builtins()}

    try:
        compiled = compile(module, "<generated_code>", "exec")
        exec(compiled, globals_dict, globals_dict)
    except MemoryError:
        return _worker_error(
            "CODE_RUNTIME_MEMORY_LIMIT",
            "code execution exceeded memory limit",
        )
    except Exception as exc:  # pragma: no cover - defensive
        return _worker_error(
            "CODE_RUNTIME_EXCEPTION",
            str(exc),
            tb=traceback.format_exc(),
        )

    run_fn = globals_dict.get("run")
    run_mode, run_err = _validate_run_callable(run_fn)
    if run_err is not None:
        return _worker_error(run_err.code, run_err.message)
    assert callable(run_fn)

    data_arg: list[Any] | None = None
    schema_arg: dict[str, Any] | None = None
    if len(artifacts_val) == 1:
        single_key = next(iter(artifacts_val))
        single_rows = artifacts_val.get(single_key)
        single_schema = schemas_val.get(single_key)
        if isinstance(single_rows, list):
            data_arg = single_rows
        if isinstance(single_schema, dict):
            schema_arg = single_schema

    for rows in artifacts_val.values():
        _strip_locators(rows)

    try:
        if run_mode == "legacy":
            if data_arg is None or schema_arg is None:
                return _worker_error(
                    "CODE_ENTRYPOINT_MISSING",
                    (
                        "run(data, schema, params) requires exactly one "
                        "artifact input; use run(artifacts, schemas, params) "
                        "for multi-artifact queries"
                    ),
                )
            result = run_fn(data_arg, schema_arg, params_val)
        else:
            result = run_fn(artifacts_val, schemas_val, params_val)
        if inspect.iscoroutine(result):
            # Creates a fresh event loop; nested asyncio.run()
            # inside user code will raise RuntimeError.
            result = asyncio.run(result)
    except MemoryError:
        return _worker_error(
            "CODE_RUNTIME_MEMORY_LIMIT",
            "code execution exceeded memory limit",
        )
    except Exception as exc:
        return _worker_error(
            "CODE_RUNTIME_EXCEPTION",
            str(exc),
            tb=traceback.format_exc(),
        )

    try:
        # Assert output is JSON-serializable under runtime serializer.
        encode_json_bytes(result)
    except Exception as exc:
        return _worker_error(
            "CODE_RUNTIME_EXCEPTION",
            f"result is not JSON-serializable: {exc}",
            tb=traceback.format_exc(),
        )

    return {"ok": True, "result": result}


def main() -> int:
    """Run worker protocol: read request JSON, execute, write response JSON."""
    raw = sys.stdin.buffer.read()
    if not raw:
        sys.stdout.buffer.write(
            encode_json_bytes(_worker_error("CODE_AST_REJECTED", "empty input"))
        )
        return 0

    try:
        payload = decode_json_bytes(raw)
    except Exception as exc:
        sys.stdout.buffer.write(
            encode_json_bytes(
                _worker_error("CODE_AST_REJECTED", f"invalid input json: {exc}")
            )
        )
        return 0

    if not isinstance(payload, dict):
        sys.stdout.buffer.write(
            encode_json_bytes(
                _worker_error(
                    "CODE_AST_REJECTED", "input payload must be object"
                )
            )
        )
        return 0

    response = _execute(payload)
    sys.stdout.buffer.write(encode_json_bytes(response))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
