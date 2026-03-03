"""Per-upstream secret file store.

Manages externalized secrets (env vars and HTTP headers) for upstream
MCP servers, stored as individual JSON files under the data directory.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
import contextlib
import datetime
import json
import os
from pathlib import Path
import tempfile
from typing import Any, SupportsFloat

from key_value.aio.protocols import AsyncKeyValue

from sift_gateway.constants import STATE_SUBDIR

_SECRETS_SUBDIR = "upstream_secrets"
_OAUTH_SUBDIR = "upstream_oauth"
_OAUTH_TOKEN_COLLECTION = "mcp-oauth-token"
_VALID_TRANSPORTS = frozenset({"stdio", "http"})
_REQUIRED_KEYS = frozenset({"version", "transport"})


class _OAuthTokenTtlCompatibleStore:
    """Proxy AsyncKeyValue store that disables TTL for OAuth token entries.

    Older OAuth client implementations may persist OAuth tokens using a TTL
    equal to access-token lifetime. That can evict refresh tokens too early and
    force repeated interactive re-login. This adapter keeps token entries
    durable by stripping TTL only for the OAuth token collection.
    """

    def __init__(self, inner: AsyncKeyValue) -> None:
        self._inner = inner

    @staticmethod
    def _effective_ttl(
        *,
        collection: str | None,
        ttl: SupportsFloat | None,
    ) -> SupportsFloat | None:
        if collection == _OAUTH_TOKEN_COLLECTION:
            return None
        return ttl

    async def get(
        self,
        key: str,
        *,
        collection: str | None = None,
    ) -> dict[str, Any] | None:
        return await self._inner.get(key=key, collection=collection)

    async def ttl(
        self,
        key: str,
        *,
        collection: str | None = None,
    ) -> tuple[dict[str, Any] | None, float | None]:
        return await self._inner.ttl(key=key, collection=collection)

    async def put(
        self,
        key: str,
        value: Mapping[str, Any],
        *,
        collection: str | None = None,
        ttl: SupportsFloat | None = None,
    ) -> None:
        await self._inner.put(
            key=key,
            value=value,
            collection=collection,
            ttl=self._effective_ttl(collection=collection, ttl=ttl),
        )

    async def delete(
        self,
        key: str,
        *,
        collection: str | None = None,
    ) -> bool:
        return await self._inner.delete(key=key, collection=collection)

    async def get_many(
        self,
        keys: Sequence[str],
        *,
        collection: str | None = None,
    ) -> list[dict[str, Any] | None]:
        return await self._inner.get_many(keys=keys, collection=collection)

    async def ttl_many(
        self,
        keys: Sequence[str],
        *,
        collection: str | None = None,
    ) -> list[tuple[dict[str, Any] | None, float | None]]:
        return await self._inner.ttl_many(keys=keys, collection=collection)

    async def put_many(
        self,
        keys: Sequence[str],
        values: Sequence[Mapping[str, Any]],
        *,
        collection: str | None = None,
        ttl: SupportsFloat | None = None,
    ) -> None:
        await self._inner.put_many(
            keys=keys,
            values=values,
            collection=collection,
            ttl=self._effective_ttl(collection=collection, ttl=ttl),
        )

    async def delete_many(
        self,
        keys: Sequence[str],
        *,
        collection: str | None = None,
    ) -> int:
        return await self._inner.delete_many(keys=keys, collection=collection)


def oauth_token_cache_key(server_url: str) -> str:
    """Return the OAuth token cache key for one MCP server URL.

    The key format mirrors FastMCP's token storage adapter behavior so
    gateway utilities can read/update persisted token records directly.
    """
    return f"{server_url.rstrip('/')}/tokens"


async def mark_oauth_access_token_stale(
    token_storage: AsyncKeyValue,
    *,
    server_url: str,
) -> bool:
    """Mark a stored OAuth access token stale while preserving refresh state.

    Returns ``True`` only when a refresh-capable token record was found and
    updated. The next OAuth request will then attempt refresh before sending
    the upstream call.
    """
    cache_key = oauth_token_cache_key(server_url)
    token_entry = await token_storage.get(
        key=cache_key,
        collection=_OAUTH_TOKEN_COLLECTION,
    )
    if not isinstance(token_entry, dict):
        return False

    refresh_token = token_entry.get("refresh_token")
    if not isinstance(refresh_token, str) or not refresh_token.strip():
        return False

    updated = dict(token_entry)
    # Force the next auth flow to refresh first using the stored refresh token.
    updated["access_token"] = ""
    updated["expires_in"] = 0
    await token_storage.put(
        key=cache_key,
        value=updated,
        collection=_OAUTH_TOKEN_COLLECTION,
        ttl=None,
    )
    return True


def secret_file_path(data_dir: str | Path, ref: str) -> Path:
    """Return the filesystem path for a secret reference.

    Strips a trailing ``.json`` suffix from *ref*, validates the
    resulting prefix, and returns the path **without** creating
    any directories.

    Args:
        data_dir: Root data directory for Sift state.
        ref: Secret reference string (upstream prefix, optionally
            with a ``.json`` suffix).

    Returns:
        Path to ``{data_dir}/state/upstream_secrets/{prefix}.json``.

    Raises:
        ValueError: If the derived prefix contains path separators
            or ``..``.
    """
    prefix = ref.removesuffix(".json")
    validate_prefix(prefix)
    return Path(data_dir) / STATE_SUBDIR / _SECRETS_SUBDIR / f"{prefix}.json"


def secrets_dir(data_dir: str | Path) -> Path:
    """Return the upstream secrets directory, creating it if needed.

    The directory is created with 0o700 permissions so that only
    the owner can list or access secret files.

    Args:
        data_dir: Root data directory for Sift state.

    Returns:
        Path to the ``{data_dir}/state/upstream_secrets/``
        directory.
    """
    path = Path(data_dir) / STATE_SUBDIR / _SECRETS_SUBDIR
    path.mkdir(parents=True, exist_ok=True)
    os.chmod(path, 0o700)
    return path


def oauth_cache_dir_path(data_dir: str | Path, ref: str) -> Path:
    """Return the OAuth cache directory path for one upstream ref.

    Args:
        data_dir: Root data directory for Sift state.
        ref: Secret reference string (upstream prefix, optionally
            with a ``.json`` suffix).

    Returns:
        Path to ``{data_dir}/state/upstream_oauth/{prefix}``.

    Raises:
        ValueError: If the derived prefix contains path separators
            or ``..``.
    """
    prefix = ref.removesuffix(".json")
    validate_prefix(prefix)
    return Path(data_dir) / STATE_SUBDIR / _OAUTH_SUBDIR / prefix


def oauth_cache_dir(data_dir: str | Path, ref: str) -> Path:
    """Return the OAuth cache directory, creating it if needed.

    Args:
        data_dir: Root data directory for Sift state.
        ref: Secret reference string (upstream prefix, optionally
            with a ``.json`` suffix).

    Returns:
        Path to ``{data_dir}/state/upstream_oauth/{prefix}``.
    """
    path = oauth_cache_dir_path(data_dir, ref)
    path.mkdir(parents=True, exist_ok=True)
    os.chmod(path, 0o700)
    return path


def validate_prefix(prefix: str) -> None:
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
    oauth: dict[str, Any] | None = None,
) -> Path:
    """Write a secret file for an upstream.

    Creates or overwrites a JSON file at
    ``{secrets_dir}/{prefix}.json`` containing the upstream's
    sensitive configuration (env vars or HTTP headers).

    Args:
        data_dir: Root data directory for Sift state.
        prefix: Upstream prefix name (used as filename).
        transport: Transport type (``"stdio"`` or ``"http"``).
        env: Environment variables for stdio transport.
        headers: HTTP headers for http transport.
        oauth: Optional OAuth metadata for http transport.

    Returns:
        Path to the written secret file.

    Raises:
        ValueError: If *prefix* contains path separators or
            ``..``, or if *transport* is not a recognised
            value.
    """
    validate_prefix(prefix)
    if transport not in _VALID_TRANSPORTS:
        msg = (
            f"Invalid transport {transport!r}: "
            f"must be one of {sorted(_VALID_TRANSPORTS)}"
        )
        raise ValueError(msg)

    now = datetime.datetime.now(datetime.UTC)
    payload = {
        "version": 1,
        "transport": transport,
        "env": env,
        "headers": headers,
        "oauth": oauth,
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


def read_secret(data_dir: str | Path, prefix: str) -> dict[str, Any]:
    """Read a secret file for an upstream.

    Args:
        data_dir: Root data directory for Sift state.
        prefix: Upstream prefix name (filename stem).

    Returns:
        Parsed secret dict with keys ``version``,
        ``transport``, ``env``, ``headers``, and
        ``updated_at``.  Optional ``oauth`` metadata
        may also be present.

    Raises:
        FileNotFoundError: If no secret file exists for
            *prefix*.
        ValueError: If the file contains invalid JSON or
            is missing required keys.
    """
    validate_prefix(prefix)
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


def resolve_secret_ref(data_dir: str | Path, ref: str) -> dict[str, Any]:
    """Resolve a secret reference to its parsed content.

    Enforces path confinement: the *ref* must not contain
    ``..`` or be an absolute path.

    Args:
        data_dir: Root data directory for Sift state.
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
    config_env: dict[str, str] | None,
    config_headers: dict[str, str] | None,
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


def oauth_token_storage(data_dir: str | Path, ref: str) -> AsyncKeyValue:
    """Create disk-backed OAuth token storage for one upstream.

    Returns an AsyncKeyValue-compatible store that persists under
    ``state/upstream_oauth/<prefix>`` and keeps OAuth token entries without
    short TTL expiry.
    """
    from key_value.aio.stores.disk import DiskStore

    return _OAuthTokenTtlCompatibleStore(
        DiskStore(directory=oauth_cache_dir(data_dir, ref))
    )
