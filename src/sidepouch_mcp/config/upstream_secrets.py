"""Per-upstream secret file store.

Manages externalized secrets (env vars and HTTP headers) for upstream
MCP servers, stored as individual JSON files under the data directory.
"""

from __future__ import annotations

import contextlib
import datetime
import json
import os
from pathlib import Path
import tempfile

from sidepouch_mcp.constants import STATE_SUBDIR

_SECRETS_SUBDIR = "upstream_secrets"
_VALID_TRANSPORTS = frozenset({"stdio", "http"})
_REQUIRED_KEYS = frozenset({"version", "transport"})


def secrets_dir(data_dir: str | Path) -> Path:
    """Return the upstream secrets directory, creating it if needed.

    The directory is created with 0o700 permissions so that only
    the owner can list or access secret files.

    Args:
        data_dir: Root data directory for SidePouch state.

    Returns:
        Path to the ``{data_dir}/state/upstream_secrets/``
        directory.
    """
    path = Path(data_dir) / STATE_SUBDIR / _SECRETS_SUBDIR
    path.mkdir(parents=True, exist_ok=True)
    os.chmod(path, 0o700)
    return path


def _validate_prefix(prefix: str) -> None:
    r"""Reject prefixes containing path separators or traversal.

    Args:
        prefix: Upstream prefix string to validate.

    Raises:
        ValueError: If the prefix contains ``/``, ``\\``,
            or ``..``.
    """
    if ".." in prefix:
        msg = f"Invalid prefix {prefix!r}: must not contain '..'"
        raise ValueError(msg)
    if "/" in prefix or "\\" in prefix:
        msg = f"Invalid prefix {prefix!r}: must not contain path separators"
        raise ValueError(msg)


def write_secret(
    data_dir: str | Path,
    prefix: str,
    *,
    transport: str,
    env: dict[str, str] | None = None,
    headers: dict[str, str] | None = None,
) -> Path:
    """Write a secret file for an upstream.

    Creates or overwrites a JSON file at
    ``{secrets_dir}/{prefix}.json`` containing the upstream's
    sensitive configuration (env vars or HTTP headers).

    Args:
        data_dir: Root data directory for SidePouch state.
        prefix: Upstream prefix name (used as filename).
        transport: Transport type (``"stdio"`` or ``"http"``).
        env: Environment variables for stdio transport.
        headers: HTTP headers for http transport.

    Returns:
        Path to the written secret file.

    Raises:
        ValueError: If *prefix* contains path separators or
            ``..``, or if *transport* is not a recognised
            value.
    """
    _validate_prefix(prefix)
    if transport not in _VALID_TRANSPORTS:
        msg = (
            f"Invalid transport {transport!r}: "
            f"must be one of {sorted(_VALID_TRANSPORTS)}"
        )
        raise ValueError(msg)

    now = datetime.datetime.now(datetime.timezone.utc)
    payload = {
        "version": 1,
        "transport": transport,
        "env": env,
        "headers": headers,
        "updated_at": now.isoformat(),
    }

    sdir = secrets_dir(data_dir)
    file_path = sdir / f"{prefix}.json"
    content = json.dumps(payload, indent=2).encode("utf-8")
    fd, tmp = tempfile.mkstemp(dir=str(sdir), suffix=".tmp")
    try:
        os.write(fd, content)
        os.fchmod(fd, 0o600)
        os.close(fd)
        fd = -1
        os.replace(tmp, str(file_path))
    except BaseException:
        if fd >= 0:
            os.close(fd)
        with contextlib.suppress(OSError):
            os.unlink(tmp)
        raise
    return file_path


def read_secret(data_dir: str | Path, prefix: str) -> dict:
    """Read a secret file for an upstream.

    Args:
        data_dir: Root data directory for SidePouch state.
        prefix: Upstream prefix name (filename stem).

    Returns:
        Parsed secret dict with keys ``version``,
        ``transport``, ``env``, ``headers``, and
        ``updated_at``.

    Raises:
        FileNotFoundError: If no secret file exists for
            *prefix*.
        ValueError: If the file contains invalid JSON or
            is missing required keys.
    """
    _validate_prefix(prefix)
    sdir = secrets_dir(data_dir)
    file_path = sdir / f"{prefix}.json"
    if not file_path.exists():
        msg = f"No secret file found for upstream {prefix!r} at {file_path}"
        raise FileNotFoundError(msg)

    raw = file_path.read_text(encoding="utf-8")
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        msg = f"Invalid JSON in secret file for upstream {prefix!r}: {exc}"
        raise ValueError(msg) from exc

    if not isinstance(data, dict):
        msg = f"Secret file for upstream {prefix!r} must contain a JSON object"
        raise ValueError(msg)

    missing = _REQUIRED_KEYS - data.keys()
    if missing:
        msg = (
            f"Secret file for upstream {prefix!r} is "
            f"missing required keys: {sorted(missing)}"
        )
        raise ValueError(msg)

    return data


def resolve_secret_ref(data_dir: str | Path, ref: str) -> dict:
    """Resolve a secret reference to its parsed content.

    Enforces path confinement: the *ref* must not contain
    ``..`` or be an absolute path.

    Args:
        data_dir: Root data directory for SidePouch state.
        ref: Secret reference string (upstream prefix name).

    Returns:
        Parsed secret dict.

    Raises:
        ValueError: If *ref* contains path traversal
            sequences or is an absolute path.
        FileNotFoundError: If the resolved secret file does
            not exist.
    """
    if ".." in ref:
        msg = f"Invalid secret ref {ref!r}: must not contain '..'"
        raise ValueError(msg)
    if Path(ref).is_absolute():
        msg = f"Invalid secret ref {ref!r}: must not be an absolute path"
        raise ValueError(msg)

    # Strip any .json suffix the caller may have included
    prefix = ref.removesuffix(".json")
    return read_secret(data_dir, prefix)


def validate_no_secret_conflict(
    config_env: dict | None,
    config_headers: dict | None,
    secret_ref: str | None,
) -> None:
    """Reject configs that specify both inline secrets and a ref.

    Args:
        config_env: Inline environment variables from the
            upstream config, or None.
        config_headers: Inline HTTP headers from the upstream
            config, or None.
        secret_ref: External secret reference string, or None.

    Raises:
        ValueError: If *secret_ref* is set and either
            *config_env* or *config_headers* contains values.
    """
    if secret_ref is None:
        return

    has_env = bool(config_env)
    has_headers = bool(config_headers)

    if has_env or has_headers:
        msg = (
            "Cannot specify both inline env/headers and "
            "secret_ref for upstream. Use one or the other."
        )
        raise ValueError(msg)
