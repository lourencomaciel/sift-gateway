"""Subprocess runtime for deterministic code-query execution."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
import subprocess
import sys
from typing import Any

from sift_gateway.canon.rfc8785 import canonical_bytes, coerce_floats

CODE_RUNTIME_CONTRACT_VERSION = "code_runtime_v1"

_SAFE_WORKER_ENV_KEYS = frozenset(
    {
        "PATH",
        "PYTHONHOME",
        "PYTHONPATH",
        "PYTHONPYCACHEPREFIX",
        "PYTHONUSERBASE",
        "PYTHONUTF8",
        "PYTHONIOENCODING",
        "VIRTUAL_ENV",
        "TMP",
        "TEMP",
        "TMPDIR",
        "LANG",
        "LC_ALL",
        "LC_CTYPE",
    }
)

_SAFE_WORKER_ENV_KEYS_WINDOWS = frozenset(
    {
        "SYSTEMROOT",
        "WINDIR",
        "COMSPEC",
        "PATHEXT",
    }
)


def _to_json_compatible(value: Any) -> Any:
    """Normalize common numeric/dataframe scalar types to JSON primitives."""
    if value is None or isinstance(value, (str, bool, int, float)):
        return value
    if isinstance(value, dict):
        return {
            str(key): _to_json_compatible(item) for key, item in value.items()
        }
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_to_json_compatible(item) for item in value]

    # numpy/pandas scalars generally expose .item()
    item = getattr(value, "item", None)
    if callable(item):
        try:
            converted = item()
        except Exception:
            converted = value
        else:
            return _to_json_compatible(converted)

    # numpy arrays and pandas containers generally expose .tolist()
    tolist = getattr(value, "tolist", None)
    if callable(tolist):
        try:
            converted = tolist()
        except Exception:
            converted = value
        else:
            return _to_json_compatible(converted)

    return value


class CodeRuntimeError(RuntimeError):
    """Base runtime error for generated code execution."""

    code: str
    message: str
    traceback: str | None

    def __init__(
        self,
        *,
        code: str,
        message: str,
        traceback: str | None = None,
    ) -> None:
        """Initialize runtime error metadata."""
        super().__init__(message)
        self.code = code
        self.message = message
        self.traceback = traceback

    def __str__(self) -> str:
        """Return the human-readable runtime message."""
        return self.message


class CodeRuntimeTimeoutError(CodeRuntimeError):
    """Raised when worker execution exceeds wall-clock timeout."""


class CodeRuntimeMemoryLimitError(CodeRuntimeError):
    """Raised when worker hits configured memory cap."""


class CodeRuntimeInfrastructureError(CodeRuntimeError):
    """Raised when worker protocol/transport fails."""


@dataclass(frozen=True)
class CodeRuntimeConfig:
    """Limits for code-query subprocess execution."""

    timeout_seconds: float
    max_memory_mb: int


def encode_json_bytes(value: Any) -> bytes:
    """Encode an object to deterministic JSON bytes.

    Tries orjson first for performance, and falls back to
    RFC8785 canonical bytes for Decimal-safe serialization.
    """
    normalized = _to_json_compatible(value)
    try:
        import orjson

        return orjson.dumps(normalized)
    except Exception:
        return canonical_bytes(coerce_floats(normalized))


def decode_json_bytes(raw: bytes) -> Any:
    """Decode JSON bytes with orjson fallback to stdlib json."""
    try:
        import orjson

        return orjson.loads(raw)
    except Exception:
        return json.loads(raw.decode("utf-8"))


def sha256_hex(value: bytes) -> str:
    """Return a stable sha256 hex digest prefixed with `sha256:`."""
    digest = hashlib.sha256(value).hexdigest()
    return f"sha256:{digest}"


def _build_env() -> dict[str, str]:
    """Build a strict worker environment with minimal inherited keys."""
    allowed_keys = set(_SAFE_WORKER_ENV_KEYS)
    if os.name == "nt":
        allowed_keys.update(_SAFE_WORKER_ENV_KEYS_WINDOWS)

    env: dict[str, str] = {}
    for key in allowed_keys:
        value = os.environ.get(key)
        if isinstance(value, str):
            env[key] = value

    env["PYTHONHASHSEED"] = "0"
    env["TZ"] = "UTC"
    return env


def _preexec_set_memory_limit(max_memory_mb: int) -> Any | None:
    """Build a preexec function that applies an address-space cap."""
    if os.name == "nt" or max_memory_mb <= 0:
        return None

    limit_bytes = int(max_memory_mb) * 1024 * 1024

    def _preexec() -> None:
        import resource

        try:
            resource.setrlimit(resource.RLIMIT_AS, (limit_bytes, limit_bytes))
        except Exception:
            # Sandbox/container runtimes may reject rlimit changes.
            return

    return _preexec


def execute_code_in_subprocess(
    *,
    code: str,
    data: list[Any] | None = None,
    schema: dict[str, Any] | None = None,
    artifacts: dict[str, list[Any]] | None = None,
    schemas: dict[str, dict[str, Any]] | None = None,
    params: dict[str, Any],
    runtime: CodeRuntimeConfig,
    allowed_import_roots: list[str] | None = None,
) -> Any:
    """Execute generated code in the dedicated subprocess worker.

    Args:
        code: Python source that defines
            ``run(artifacts, schemas, params)``.
        data: Backward-compatible single-artifact input records for
            ``run(data, schema, params)``.
        schema: Backward-compatible single-artifact schema object.
        artifacts: Multi-artifact input records keyed by artifact id.
        schemas: Multi-artifact schema objects keyed by artifact id.
        params: Caller-provided params object.
        runtime: Runtime limits.
        allowed_import_roots: Optional import root allowlist passed
            to the worker.

    Returns:
        JSON-serializable value returned by ``run``.

    Raises:
        CodeRuntimeTimeoutError: Timeout exceeded.
        CodeRuntimeMemoryLimitError: Memory cap exceeded.
        CodeRuntimeError: User/runtime validation failure.
        CodeRuntimeInfrastructureError: Worker protocol failure.
    """
    if artifacts is None:
        if data is None:
            raise CodeRuntimeInfrastructureError(
                code="INTERNAL",
                message="code runtime launch failed: missing data/artifacts",
            )
        artifacts_payload: dict[str, list[Any]] = {"__single__": data}
        schemas_payload: dict[str, dict[str, Any]] = {
            "__single__": schema or {}
        }
    else:
        artifacts_payload = artifacts
        schemas_payload = schemas or {}

    payload = encode_json_bytes(
        {
            "code": code,
            "artifacts": artifacts_payload,
            "schemas": schemas_payload,
            "params": params,
            "allowed_import_roots": (
                sorted({str(root) for root in allowed_import_roots})
                if allowed_import_roots is not None
                else None
            ),
        }
    )

    worker_module = "sift_gateway.codegen.worker_main"
    preexec = _preexec_set_memory_limit(runtime.max_memory_mb)

    def _run(preexec_fn: Any) -> subprocess.CompletedProcess[bytes]:
        return subprocess.run(
            [sys.executable, "-m", worker_module],
            input=payload,
            capture_output=True,
            timeout=runtime.timeout_seconds,
            check=False,
            env=_build_env(),
            preexec_fn=preexec_fn,
        )

    try:
        completed = _run(preexec)
    except subprocess.TimeoutExpired as exc:
        raise CodeRuntimeTimeoutError(
            code="CODE_RUNTIME_TIMEOUT",
            message="code execution timed out",
        ) from exc
    except subprocess.SubprocessError as exc:
        # In sandboxed runtimes, preexec_fn may be disallowed.
        if preexec is not None and "preexec_fn" in str(exc):
            try:
                completed = _run(None)
            except subprocess.TimeoutExpired as timeout_exc:
                raise CodeRuntimeTimeoutError(
                    code="CODE_RUNTIME_TIMEOUT",
                    message="code execution timed out",
                ) from timeout_exc
            except Exception as retry_exc:
                raise CodeRuntimeInfrastructureError(
                    code="INTERNAL",
                    message=f"code runtime launch failed: {retry_exc}",
                ) from retry_exc
        else:
            raise CodeRuntimeInfrastructureError(
                code="INTERNAL",
                message=f"code runtime launch failed: {exc}",
            ) from exc
    except Exception as exc:
        raise CodeRuntimeInfrastructureError(
            code="INTERNAL",
            message=f"code runtime launch failed: {exc}",
        ) from exc

    stdout = completed.stdout or b""
    stderr = completed.stderr.decode("utf-8", errors="replace").strip()

    # Try structured worker response first, even on non-zero exit.
    parsed: Any = None
    if stdout:
        try:
            parsed = decode_json_bytes(stdout)
        except Exception:
            parsed = None

    if isinstance(parsed, dict) and parsed.get("ok") is False:
        err = parsed.get("error")
        if isinstance(err, dict):
            code_val = err.get("code")
            msg_val = err.get("message")
            traceback_val = err.get("traceback")
            if isinstance(code_val, str) and isinstance(msg_val, str):
                if code_val == "CODE_RUNTIME_MEMORY_LIMIT":
                    raise CodeRuntimeMemoryLimitError(
                        code=code_val,
                        message=msg_val,
                        traceback=(
                            str(traceback_val)
                            if isinstance(traceback_val, str)
                            else None
                        ),
                    )
                raise CodeRuntimeError(
                    code=code_val,
                    message=msg_val,
                    traceback=(
                        str(traceback_val)
                        if isinstance(traceback_val, str)
                        else None
                    ),
                )

    if completed.returncode != 0:
        message = stderr or "code runtime worker failed"
        lowered = message.lower()
        if "memory" in lowered or completed.returncode in {137, -9}:
            raise CodeRuntimeMemoryLimitError(
                code="CODE_RUNTIME_MEMORY_LIMIT",
                message="code execution exceeded memory limit",
            )
        raise CodeRuntimeInfrastructureError(
            code="INTERNAL",
            message=f"code runtime worker failure: {message}",
        )

    if not isinstance(parsed, dict) or parsed.get("ok") is not True:
        raise CodeRuntimeInfrastructureError(
            code="INTERNAL",
            message="code runtime worker returned invalid payload",
        )

    return parsed.get("result")
