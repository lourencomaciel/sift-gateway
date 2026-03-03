"""Connect to upstream MCP servers and discover their tools.

Manage client transport configuration, tool schema hashing, and
stable upstream identity computation.  Exports ``UpstreamInstance``,
``connect_upstreams``, and ``call_upstream_tool``.

Typical usage example::

    upstreams = await connect_upstreams(config.upstreams)
    result = await call_upstream_tool(
        upstreams[0], "list_repos", {"org": "acme"}
    )
"""

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
import json
import logging
import os
from pathlib import Path
import tempfile
from typing import Any
import uuid

from fastmcp import Client
from fastmcp.client.transports import StdioTransport

from sift_gateway.canon.rfc8785 import canonical_bytes
from sift_gateway.config.settings import UpstreamConfig
from sift_gateway.util.hashing import sha256_trunc

_USER_IDS_FILENAME = "upstream_user_ids.json"
SecretData = dict[str, Any]

try:
    import fcntl as _fcntl
except ImportError:  # Windows
    _fcntl = None  # type: ignore[assignment]


@contextmanager
def _file_lock(
    lock_path: Path,
) -> Generator[None, None, None]:
    """Acquire an exclusive file lock, portable across platforms.

    Uses ``fcntl.flock`` on POSIX.  On Windows (where ``fcntl``
    is unavailable) the lock is a no-op — concurrent processes
    may race, but the atomic-rename write strategy limits damage
    to a single lost entry that will be regenerated.

    Args:
        lock_path: Path to the lock file (created if absent).

    Yields:
        Nothing; the lock is held for the duration of the block.
    """
    if _fcntl is None:
        yield
        return

    fd = os.open(str(lock_path), os.O_CREAT | os.O_RDWR)
    try:
        _fcntl.flock(fd, _fcntl.LOCK_EX)
        yield
    finally:
        _fcntl.flock(fd, _fcntl.LOCK_UN)
        os.close(fd)


def _user_ids_path(data_dir: str | None) -> Path:
    """Return the path to the persistent user IDs file.

    Args:
        data_dir: Root data directory for Sift state.

    Returns:
        Absolute path to the user IDs JSON file.
    """
    from sift_gateway.constants import DEFAULT_DATA_DIR

    resolved = data_dir or DEFAULT_DATA_DIR
    return Path(resolved) / "state" / _USER_IDS_FILENAME


def _atomic_json_write(path: Path, data: dict[str, str]) -> None:
    """Write *data* as JSON to *path* atomically.

    Uses a temporary file in the same directory followed by an
    atomic rename so that readers never see a partial file.

    Args:
        path: Destination file path.
        data: JSON-serialisable dict to persist.
    """
    fd = -1
    tmp_path: Path | None = None
    try:
        fd, tmp = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
        tmp_path = Path(tmp)
        os.write(
            fd,
            (json.dumps(data, indent=2) + "\n").encode(),
        )
        os.fsync(fd)
        os.close(fd)
        fd = -1  # mark closed
        tmp_path.replace(path)
        tmp_path = None  # rename succeeded; nothing to clean up
    finally:
        if fd >= 0:
            os.close(fd)
        if tmp_path is not None:
            tmp_path.unlink(missing_ok=True)


def _read_user_ids(path: Path) -> dict[str, str]:
    """Read and validate the user IDs map from disk.

    Returns an empty dict on missing file, corrupt JSON, or
    unexpected content type.

    Args:
        path: Path to the ``upstream_user_ids.json`` file.

    Returns:
        Parsed user ID map, or empty dict on any error.
    """
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError, UnicodeDecodeError):
        logging.getLogger(__name__).warning("Corrupt %s — rebuilding", path)
        return {}
    if isinstance(data, dict):
        return data
    logging.getLogger(__name__).warning(
        "Unexpected type in %s — rebuilding", path
    )
    return {}


def resolve_external_user_id(
    config: UpstreamConfig,
    data_dir: str | None = None,
) -> str | None:
    """Resolve the external_user_id for an upstream.

    When ``external_user_id`` is ``"auto"``, a UUID4 is generated
    on first call and persisted to
    ``{data_dir}/state/upstream_user_ids.json`` so that subsequent
    restarts reuse the same identity.  Any other non-None value
    is returned verbatim.

    Args:
        config: Upstream configuration with optional
            ``external_user_id`` field.
        data_dir: Root data directory for Sift state.

    Returns:
        The resolved user ID string, or ``None`` if the field
        is not set.
    """
    raw = config.external_user_id
    if raw is None:
        return None
    if raw != "auto":
        return raw

    # "auto" — load or generate a persistent UUID.
    # Fast path: read without lock; if prefix exists, return it.
    path = _user_ids_path(data_dir)
    stored = _read_user_ids(path)
    existing = stored.get(config.prefix)
    if isinstance(existing, str) and existing:
        return existing

    # Slow path: acquire exclusive lock, re-read (another process
    # may have written while we waited), then generate if needed.
    path.parent.mkdir(parents=True, exist_ok=True)
    with _file_lock(path.with_suffix(".lock")):
        stored = _read_user_ids(path)
        existing = stored.get(config.prefix)
        if isinstance(existing, str) and existing:
            return existing

        generated = str(uuid.uuid4())
        stored[config.prefix] = generated
        _atomic_json_write(path, stored)
        return generated


@dataclass(frozen=True)
class UpstreamToolSchema:
    """Schema descriptor for a single tool discovered from upstream.

    Attributes:
        name: Tool name as reported by the upstream server.
        description: Human-readable tool description.
        input_schema: JSON Schema dict for the tool's arguments.
        schema_hash: Truncated SHA-256 of the canonical schema.
    """

    name: str
    description: str
    input_schema: dict[str, Any]
    schema_hash: str  # sha256(canonical_bytes(input_schema))[:32]


@dataclass(frozen=True)
class UpstreamInstance:
    """Immutable runtime descriptor for a connected upstream server.

    Created during upstream discovery and held for the lifetime of
    the gateway process.

    Attributes:
        config: Original upstream configuration.
        instance_id: Stable identity hash for cache keying.
        tools: Discovered tool schemas from the upstream.
        auth_fingerprint: Optional hash of auth-relevant values.
    """

    config: UpstreamConfig
    instance_id: str  # upstream_instance_id
    tools: list[UpstreamToolSchema]
    auth_fingerprint: str | None = None
    secret_data: SecretData | None = None
    resolved_external_user_id: str | None = None

    @property
    def prefix(self) -> str:
        """The upstream namespace prefix."""
        return self.config.prefix


_UNSET = object()  # Sentinel for "not provided" secret_data

_ENV_ALLOWLIST: frozenset[str] = frozenset(
    {
        "PATH",
        "HOME",
        "LANG",
        "LC_ALL",
        "TMPDIR",
        "TMP",
        "TEMP",
        "USER",
        "LOGNAME",
        "SHELL",
    }
)
_AUTH_STATUS_CODES: frozenset[int] = frozenset({401, 403})
_AUTH_ERROR_HINTS: tuple[str, ...] = (
    "unauthorized",
    "invalid_token",
    "access_denied",
    "interactive login",
)


def _secret_oauth_enabled(secret: SecretData | None) -> bool:
    """Return whether secret payload enables OAuth runtime auth mode."""
    if not isinstance(secret, dict):
        return False
    oauth = secret.get("oauth")
    return isinstance(oauth, dict) and bool(oauth.get("enabled"))


def _headers_without_authorization(
    headers: dict[str, str],
) -> dict[str, str]:
    """Return a copy of headers without Authorization entries."""
    return {
        key: value
        for key, value in headers.items()
        if key.lower() != "authorization"
    }


def _headers_have_authorization(headers: dict[str, str]) -> bool:
    """Return whether headers contain a non-empty Authorization value."""
    for key, value in headers.items():
        if key.lower() != "authorization":
            continue
        if isinstance(value, str) and value.strip():
            return True
    return False


def _build_stdio_env(
    config: UpstreamConfig,
    data_dir: str | None = None,
    *,
    secret: SecretData | None | object = _UNSET,
) -> dict[str, str]:
    """Build isolated environment for a stdio upstream.

    Composes a minimal environment from an allowlist of safe
    parent env vars, optional secret ref values, and explicit
    config env overrides.

    Args:
        config: Upstream configuration with env, secret_ref,
            and inherit_parent_env fields.
        data_dir: Root data directory for secret file lookup.
            Falls back to ``DEFAULT_DATA_DIR`` when ``None``.
        secret: Pre-resolved secret data dict, or sentinel
            ``_UNSET`` to resolve on demand.

    Returns:
        Merged environment dict for subprocess execution.
    """
    if config.inherit_parent_env:
        base: dict[str, str] = dict(os.environ)
    else:
        base = {k: v for k, v in os.environ.items() if k in _ENV_ALLOWLIST}

    if secret is _UNSET:
        secret = _resolve_secret_data(config, data_dir)
    if isinstance(secret, dict):
        secret_env_raw = secret.get("env")
        if isinstance(secret_env_raw, dict):
            base.update(
                {str(key): str(value) for key, value in secret_env_raw.items()}
            )

    base.update(config.env)
    return base


def _resolve_secret_data(
    config: UpstreamConfig,
    data_dir: str | None = None,
) -> SecretData | None:
    """Load secret file contents for an upstream, if configured.

    Args:
        config: Upstream configuration with optional secret_ref.
        data_dir: Root data directory for secret file lookup.

    Returns:
        Parsed secret dict, or ``None`` when no secret_ref is
        set.
    """
    if not config.secret_ref:
        return None
    from sift_gateway.config.upstream_secrets import (
        resolve_secret_ref,
    )
    from sift_gateway.constants import DEFAULT_DATA_DIR

    resolved_dir = data_dir or DEFAULT_DATA_DIR
    return resolve_secret_ref(resolved_dir, config.secret_ref)


def _build_http_headers(
    config: UpstreamConfig,
    data_dir: str | None = None,
    *,
    secret: SecretData | None | object = _UNSET,
) -> dict[str, str]:
    """Build merged HTTP headers for an HTTP upstream.

    Merges secret file headers (if ``secret_ref`` is set) with
    inline config headers.  Config headers take precedence.

    Args:
        config: Upstream configuration.
        data_dir: Root data directory for secret file lookup.
        secret: Pre-resolved secret data dict, or sentinel
            ``_UNSET`` to resolve on demand.

    Returns:
        Merged headers dict.
    """
    base: dict[str, str] = {}
    if secret is _UNSET:
        secret = _resolve_secret_data(config, data_dir)
    if isinstance(secret, dict):
        secret_headers_raw = secret.get("headers")
        if isinstance(secret_headers_raw, dict):
            base.update(
                {
                    str(key): str(value)
                    for key, value in secret_headers_raw.items()
                }
            )
    base.update(config.headers)
    return base


def _build_runtime_oauth_auth(
    config: UpstreamConfig,
    data_dir: str | None = None,
    *,
    secret: SecretData | None | object = _UNSET,
) -> Any | None:
    """Build non-interactive OAuth auth object for HTTP upstream runtime.

    Returns ``None`` when OAuth mode is not enabled in the resolved
    secret payload.
    """
    if secret is _UNSET:
        secret = _resolve_secret_data(config, data_dir)
    if not isinstance(secret, dict) or not _secret_oauth_enabled(secret):
        return None
    if not config.url:
        return None

    from fastmcp.client.auth import OAuth

    from sift_gateway.config.upstream_secrets import oauth_token_storage
    from sift_gateway.constants import DEFAULT_DATA_DIR

    class _RuntimeOAuth(OAuth):
        async def redirect_handler(self, authorization_url: str) -> None:
            _ = authorization_url
            msg = (
                "OAuth session requires interactive login. "
                f"Run `sift-gateway upstream login --server {config.prefix}`."
            )
            raise RuntimeError(msg)

    resolved_dir = data_dir or DEFAULT_DATA_DIR
    secret_ref = config.secret_ref or config.prefix
    token_storage = oauth_token_storage(resolved_dir, secret_ref)
    return _RuntimeOAuth(config.url, token_storage=token_storage)


def _exception_http_status(exc: Exception) -> int | None:
    """Best-effort extraction of HTTP status code from transport exceptions."""
    status = getattr(exc, "status_code", None)
    if isinstance(status, int):
        return status

    response = getattr(exc, "response", None)
    response_status = getattr(response, "status_code", None)
    if isinstance(response_status, int):
        return response_status
    return None


def _is_auth_failure_exception(exc: Exception) -> bool:
    """Return whether *exc* signals an authentication failure."""
    status = _exception_http_status(exc)
    if status in _AUTH_STATUS_CODES:
        return True

    text = str(exc).lower()
    return any(hint in text for hint in _AUTH_ERROR_HINTS)


async def _mark_runtime_oauth_access_token_stale(
    *,
    config: UpstreamConfig,
    data_dir: str | None,
) -> bool:
    """Mark stored access token stale to force refresh on next OAuth request."""
    if config.transport != "http":
        return False
    if not config.url:
        return False

    from sift_gateway.config.upstream_secrets import (
        mark_oauth_access_token_stale,
        oauth_token_storage,
    )
    from sift_gateway.constants import DEFAULT_DATA_DIR

    resolved_dir = data_dir or DEFAULT_DATA_DIR
    secret_ref = config.secret_ref or config.prefix
    token_storage = oauth_token_storage(resolved_dir, secret_ref)
    return await mark_oauth_access_token_stale(
        token_storage,
        server_url=config.url,
    )


async def _call_tool_once(
    *,
    instance: UpstreamInstance,
    tool_name: str,
    arguments: dict[str, Any],
    data_dir: str | None,
    disable_oauth: bool = False,
) -> dict[str, Any]:
    """Call upstream once and normalize the tool result."""
    transport = _client_transport(
        instance.config,
        data_dir,
        secret=instance.secret_data,
        resolved_user_id=instance.resolved_external_user_id,
        disable_oauth=disable_oauth,
    )
    async with Client(transport, timeout=30.0) as client:
        result = await client.call_tool(tool_name, arguments)

    normalized_content = _client_result_content(result)
    structured = getattr(result, "structured_content", None)
    is_error = bool(getattr(result, "is_error", False))
    meta = getattr(result, "meta", None)

    return {
        "content": normalized_content,
        "structuredContent": structured,
        "isError": is_error,
        "meta": meta if isinstance(meta, dict) else {},
    }


def _effective_external_user_id(
    args: list[str],
    resolved_user_id: str | None,
) -> str | None:
    """Determine the effective external user ID.

    If ``args`` already contains ``--external-user-id`` (either as
    a separate token or ``--external-user-id=<val>``), that value
    wins and ``resolved_user_id`` is ignored.  Otherwise returns
    ``resolved_user_id``.

    Args:
        args: The upstream CLI argument list.
        resolved_user_id: Value from ``resolve_external_user_id``.

    Returns:
        The user ID that will actually reach the upstream, or
        ``None`` when neither source provides one.
    """
    for i, token in enumerate(args):
        if token == "--external-user-id":
            # --external-user-id <value>
            if i + 1 < len(args):
                return args[i + 1]
            return None  # malformed; treat as absent
        if token.startswith("--external-user-id="):
            return token.split("=", 1)[1] or None
    return resolved_user_id


def _args_have_external_user_id(args: list[str]) -> bool:
    """Check if args already contain --external-user-id."""
    return any(
        a == "--external-user-id" or a.startswith("--external-user-id=")
        for a in args
    )


def _client_transport(
    config: UpstreamConfig,
    data_dir: str | None = None,
    *,
    secret: SecretData | None | object = _UNSET,
    resolved_user_id: str | None = None,
    disable_oauth: bool = False,
) -> Any:
    """Build a fastmcp client transport for ``Client``.

    Args:
        config: Upstream configuration with transport type and
            connection details.
        data_dir: Root data directory for secret file lookup.
            Passed through to ``_build_stdio_env``.
        secret: Pre-resolved secret data dict, or sentinel
            ``_UNSET`` to resolve on demand.
        resolved_user_id: Pre-resolved external user ID.  When
            provided, used directly instead of re-resolving
            from disk.
        disable_oauth: When true, skip runtime OAuth auth wiring
            and use static header auth only.

    Returns:
        A fastmcp transport object suitable for ``Client``.
    """
    if config.transport == "stdio":
        env = _build_stdio_env(config, data_dir, secret=secret)
        args = list(config.args)
        user_id = (
            resolved_user_id
            if resolved_user_id is not None
            else resolve_external_user_id(config, data_dir)
        )
        if user_id and not _args_have_external_user_id(args):
            args.extend(["--external-user-id", user_id])
        return StdioTransport(
            command=config.command or "",
            args=args,
            env=env,
        )

    # HTTP transport — merge headers from secret_ref
    headers = _build_http_headers(config, data_dir, secret=secret)
    oauth_auth = None
    if not disable_oauth:
        oauth_auth = _build_runtime_oauth_auth(
            config, data_dir, secret=secret
        )
    if oauth_auth is not None:
        # OAuth auth sets Authorization dynamically; avoid stale header clashes.
        headers = _headers_without_authorization(headers)
    url = config.url or ""
    if headers or oauth_auth is not None:
        from fastmcp.mcp_config import (
            infer_transport_type_from_url,
        )

        inferred = infer_transport_type_from_url(url)
        if inferred == "sse":
            from fastmcp.client.transports import (
                SSETransport,
            )

            return SSETransport(url=url, headers=headers, auth=oauth_auth)
        from fastmcp.client.transports import (
            StreamableHttpTransport,
        )

        return StreamableHttpTransport(
            url=url,
            headers=headers,
            auth=oauth_auth,
        )
    return url


def _client_result_content(result: Any) -> list[dict[str, Any]]:
    """Normalize content blocks from an MCP client result.

    Handles raw dicts, Pydantic models with ``model_dump``,
    and fallback string coercion.

    Args:
        result: Raw MCP tool call result object.

    Returns:
        List of normalized content block dicts.
    """
    content_blocks = getattr(result, "content", None)
    if not isinstance(content_blocks, list):
        return []

    normalized: list[dict[str, Any]] = []
    for block in content_blocks:
        if isinstance(block, dict):
            normalized.append(dict(block))
            continue
        model_dump = getattr(block, "model_dump", None)
        if callable(model_dump):
            normalized.append(model_dump(by_alias=True, exclude_none=True))
            continue
        normalized.append({"type": "text", "text": str(block)})
    return normalized


def compute_upstream_instance_id(
    config: UpstreamConfig,
    data_dir: str | None = None,
    *,
    resolved_user_id: str | None = None,
) -> str:
    """Compute a stable upstream instance identity hash.

    The identity includes transport type, stable endpoint data,
    prefix, and optional semantic salt values.  When secrets are
    externalized via ``secret_ref``, salt values are read from the
    secret file so the identity remains correct.

    Args:
        config: Upstream configuration.
        data_dir: Root data directory for resolving secret refs.
        resolved_user_id: Pre-resolved external user ID.  When
            provided, used directly instead of re-resolving from
            disk.  Pass ``None`` to resolve on demand (legacy
            callers).

    Returns:
        Truncated SHA-256 hex string (32 chars) suitable for
        cache keying.
    """
    identity: dict[str, Any] = {
        "transport": config.transport,
        "prefix": config.prefix,
    }
    secret = _resolve_secret_data(config, data_dir)
    if config.transport == "stdio":
        identity["command"] = config.command or ""
        identity["args"] = config.args
        # Include semantic salt env values (stable, non-secret)
        # Check both inline config.env and secret file env
        secret_env: dict[str, str] = (secret.get("env") or {}) if secret else {}
        for key in sorted(config.semantic_salt_env_keys):
            val = config.env.get(key, secret_env.get(key, ""))
            identity[f"salt_env_{key}"] = val
    elif config.transport == "http":
        identity["url"] = config.url or ""
        secret_hdrs: dict[str, str] = (
            (secret.get("headers") or {}) if secret else {}
        )
        for key in sorted(config.semantic_salt_headers):
            val = config.headers.get(key, secret_hdrs.get(key, ""))
            identity[f"salt_header_{key}"] = val

    # Include the effective external user ID — only for stdio
    # where it actually affects the launched process args.
    if config.transport == "stdio":
        effective_uid = _effective_external_user_id(
            list(config.args),
            resolved_user_id
            if resolved_user_id is not None
            else resolve_external_user_id(config, data_dir),
        )
        if effective_uid:
            identity["external_user_id"] = effective_uid

    return sha256_trunc(canonical_bytes(identity), 32)


def compute_auth_fingerprint(
    config: UpstreamConfig,
    data_dir: str | None = None,
) -> str | None:
    """Compute an optional auth fingerprint for diagnostics.

    Hashes non-salt headers and env values, including values
    from externalized secret files.

    Args:
        config: Upstream configuration.
        data_dir: Root data directory for resolving secret refs.

    Returns:
        Truncated SHA-256 hex string (16 chars), or ``None``
        when no auth-relevant values exist.
    """
    auth_values: dict[str, str] = {}
    secret = _resolve_secret_data(config, data_dir)

    if config.transport == "stdio":
        salt_keys = set(config.semantic_salt_env_keys)
        # Merge inline + secret env for fingerprinting
        all_env: dict[str, str] = {}
        if secret:
            all_env.update(secret.get("env") or {})
        all_env.update(config.env)
        for key, val in sorted(all_env.items()):
            if key not in salt_keys:
                auth_values[f"env_{key}"] = val
    elif config.transport == "http":
        salt_keys = set(config.semantic_salt_headers)
        all_headers: dict[str, str] = {}
        if secret:
            all_headers.update(secret.get("headers") or {})
        all_headers.update(config.headers)
        oauth_enabled = _secret_oauth_enabled(secret)
        if oauth_enabled:
            all_headers = _headers_without_authorization(all_headers)
            auth_values["oauth_enabled"] = "1"
        for key, val in sorted(all_headers.items()):
            if key not in salt_keys:
                auth_values[f"header_{key}"] = val

    if not auth_values:
        return None

    return sha256_trunc(canonical_bytes(auth_values), 16)


async def discover_tools(
    config: UpstreamConfig,
    data_dir: str | None = None,
    *,
    resolved_user_id: str | None = None,
) -> list[UpstreamToolSchema]:
    """Fetch and normalize tool list from an upstream server.

    Args:
        config: Upstream configuration with transport details.
        data_dir: Root data directory for resolving secret refs.
        resolved_user_id: Pre-resolved external user ID.

    Returns:
        List of ``UpstreamToolSchema`` descriptors with hashed
        input schemas.
    """
    transport = _client_transport(
        config, data_dir, resolved_user_id=resolved_user_id
    )
    async with Client(transport, timeout=30.0) as client:
        tools = await client.list_tools()

    discovered: list[UpstreamToolSchema] = []
    for tool in tools:
        input_schema = dict(getattr(tool, "inputSchema", {}) or {})
        schema_hash = sha256_trunc(canonical_bytes(input_schema), 32)
        discovered.append(
            UpstreamToolSchema(
                name=str(tool.name),
                description=str(getattr(tool, "description", "") or ""),
                input_schema=input_schema,
                schema_hash=schema_hash,
            )
        )
    return discovered


async def connect_upstream(
    config: UpstreamConfig,
    data_dir: str | None = None,
) -> UpstreamInstance:
    """Discover one upstream and build its runtime descriptor.

    Args:
        config: Upstream configuration.
        data_dir: Root data directory for resolving secret refs.

    Returns:
        Immutable ``UpstreamInstance`` with discovered tools,
        computed instance ID, and optional auth fingerprint.
    """
    user_id = (
        resolve_external_user_id(config, data_dir)
        if config.transport == "stdio"
        else None
    )
    tools = await discover_tools(config, data_dir, resolved_user_id=user_id)
    return UpstreamInstance(
        config=config,
        instance_id=compute_upstream_instance_id(
            config, data_dir, resolved_user_id=user_id
        ),
        tools=tools,
        auth_fingerprint=compute_auth_fingerprint(config, data_dir),
        secret_data=_resolve_secret_data(config, data_dir),
        resolved_external_user_id=user_id,
    )


async def connect_upstreams(
    configs: list[UpstreamConfig],
    data_dir: str | None = None,
) -> list[UpstreamInstance]:
    """Discover all configured upstreams sequentially.

    Args:
        configs: List of upstream configurations.
        data_dir: Root data directory for resolving secret refs.

    Returns:
        List of connected ``UpstreamInstance`` descriptors in
        the same order as *configs*.
    """
    return [await connect_upstream(config, data_dir) for config in configs]


async def call_upstream_tool(
    instance: UpstreamInstance,
    tool_name: str,
    arguments: dict[str, Any],
    data_dir: str | None = None,
) -> dict[str, Any]:
    """Call a tool on the upstream and return the raw response.

    Args:
        instance: Connected upstream runtime descriptor.
        tool_name: Name of the tool to invoke on the upstream.
        arguments: Forwarded tool arguments (reserved keys
            already stripped).
        data_dir: Root data directory for resolving secret refs.

    Returns:
        Dict with ``content`` (list), ``structuredContent``,
        ``isError`` (bool), and ``meta`` keys.
    """
    try:
        return await _call_tool_once(
            instance=instance,
            tool_name=tool_name,
            arguments=arguments,
            data_dir=data_dir,
        )
    except Exception as exc:
        if (
            instance.config.transport != "http"
            or not _secret_oauth_enabled(instance.secret_data)
            or not _is_auth_failure_exception(exc)
        ):
            raise
        auth_failure = exc

    # Restarted sessions can lose OAuth cache state while a valid static
    # Authorization header still exists in secret storage.
    has_static_auth = _headers_have_authorization(
        _build_http_headers(
            instance.config,
            data_dir,
            secret=instance.secret_data,
        )
    )

    refreshed = await _mark_runtime_oauth_access_token_stale(
        config=instance.config,
        data_dir=data_dir,
    )
    if refreshed:
        try:
            return await _call_tool_once(
                instance=instance,
                tool_name=tool_name,
                arguments=arguments,
                data_dir=data_dir,
            )
        except Exception as retry_exc:
            if not (
                has_static_auth and _is_auth_failure_exception(retry_exc)
            ):
                raise
            return await _call_tool_once(
                instance=instance,
                tool_name=tool_name,
                arguments=arguments,
                data_dir=data_dir,
                disable_oauth=True,
            )

    if has_static_auth:
        return await _call_tool_once(
            instance=instance,
            tool_name=tool_name,
            arguments=arguments,
            data_dir=data_dir,
            disable_oauth=True,
        )

    raise auth_failure
