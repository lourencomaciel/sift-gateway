"""Administrative helpers for upstream registration workflows.

These helpers provide ergonomic CRUD-style operations over the
``mcpServers`` section in ``state/config.json`` while preserving
secret externalization via ``state/upstream_secrets`` files.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urljoin, urlparse

from sift_gateway.config.mcp_servers import (
    extract_mcp_servers,
)
from sift_gateway.config.shared import (
    gateway_config_path,
    load_gateway_config_dict,
)
from sift_gateway.config.upstream_registry import (
    bootstrap_registry_from_config,
    entry_to_registry_payload,
    get_registry_upstream_record,
    load_registry_upstream_records,
    merge_missing_registry_from_config,
    mirror_registry_to_config,
    remove_registry_upstream,
    set_registry_upstream_enabled,
    set_registry_upstream_secret_ref,
    upsert_registry_from_mcp_servers,
)
from sift_gateway.config.upstream_registry_convert import (
    _extract_gateway_fields,
)
from sift_gateway.config.upstream_secrets import (
    read_secret,
    secret_file_path,
    validate_prefix,
    write_secret,
)
from sift_gateway.constants import DEFAULT_DATA_DIR
from sift_gateway.mcp.upstream import discover_tools
from sift_gateway.mcp.upstream_errors import classify_upstream_exception

_logger = logging.getLogger(__name__)

_OAUTH_REDIRECT_STATUS_CODES = (301, 302, 303, 307, 308)
_OAUTH_REDIRECT_MAX_HOPS = 20


def parse_kv_pairs(
    raw_pairs: list[str] | None,
    *,
    option_name: str,
) -> dict[str, str]:
    """Parse repeated ``KEY=VALUE`` CLI options to a dict."""
    if not raw_pairs:
        return {}
    parsed: dict[str, str] = {}
    for raw in raw_pairs:
        key, sep, value = raw.partition("=")
        if not sep:
            msg = f"invalid {option_name} value {raw!r}: expected KEY=VALUE"
            raise ValueError(msg)
        key = key.strip()
        if not key:
            msg = f"invalid {option_name} value {raw!r}: key must be non-empty"
            raise ValueError(msg)
        parsed[key] = value
    return parsed


def resolve_upstream_data_dir(
    data_dir: Path | None = None,
) -> Path:
    """Resolve effective data directory for upstream admin commands."""
    if data_dir is not None:
        return data_dir
    env_dir = os.environ.get("SIFT_GATEWAY_DATA_DIR")
    return Path(env_dir if env_dir else DEFAULT_DATA_DIR).resolve()


def _load_config_server_entry(
    *,
    data_dir: Path,
    server: str,
) -> dict[str, Any] | None:
    """Read one upstream entry directly from config.json without writes."""
    config_path = gateway_config_path(data_dir)
    if not config_path.exists():
        return None
    try:
        raw = json.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(raw, dict):
        return None
    try:
        servers = extract_mcp_servers(raw)
    except ValueError:
        return None
    entry = servers.get(server)
    return entry if isinstance(entry, dict) else None


def _record_from_config_server(
    *,
    data_dir: Path,
    server: str,
) -> dict[str, Any] | None:
    """Build a validated minimal registry-like record from config.json."""
    entry = _load_config_server_entry(data_dir=data_dir, server=server)
    if entry is None:
        return None

    # Reuse the same strict validator as registry sync to avoid dry-run drift.
    payload = entry_to_registry_payload(
        data_dir=data_dir,
        prefix=server,
        entry=entry,
        source_kind="manual",
        source_ref=None,
        pending_secret_writes=[],
    )

    raw_args: Any = []
    try:
        raw_args = json.loads(payload["args_json"])
    except (TypeError, json.JSONDecodeError):
        raw_args = []
    args = (
        [str(item) for item in raw_args] if isinstance(raw_args, list) else []
    )

    return {
        "prefix": server,
        "transport": payload["transport"],
        "command": payload["command"]
        if isinstance(payload["command"], str)
        else None,
        "url": payload["url"] if isinstance(payload["url"], str) else None,
        "args": args,
        "secret_ref": payload["secret_ref"]
        if isinstance(payload["secret_ref"], str)
        else None,
        "enabled": bool(payload["enabled"]),
    }


def _resolve_mutation_record(
    *,
    data_dir: Path,
    server: str,
    dry_run: bool,
) -> dict[str, Any] | None:
    """Resolve target record for mutating commands.

    Dry-run mode avoids registry bootstrap/sync side effects while still
    resolving from the canonical registry when available.
    """
    if dry_run:
        # Prefer the canonical registry when present so dry-run resolution
        # matches real mutation behavior under config/registry drift.
        record = get_registry_upstream_record(
            data_dir=data_dir,
            prefix=server,
        )
        if record is not None:
            return record
        return _record_from_config_server(data_dir=data_dir, server=server)
    bootstrap_registry_from_config(data_dir)
    merge_missing_registry_from_config(data_dir)
    return get_registry_upstream_record(
        data_dir=data_dir,
        prefix=server,
    )


def _read_secret_from_file(
    *,
    data_dir: Path,
    ref: str,
) -> dict[str, Any] | None:
    """Read an existing secret file without creating directories."""
    path = secret_file_path(data_dir, ref)
    if not path.is_file():
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return raw if isinstance(raw, dict) else None


def _delete_secret_file(
    *,
    data_dir: Path,
    ref: str | None,
) -> None:
    """Delete an upstream secret file for ``ref`` when present."""
    if not isinstance(ref, str) or not ref:
        return
    path = secret_file_path(data_dir, ref)
    with contextlib.suppress(FileNotFoundError):
        path.unlink()


def _secret_ref_is_still_referenced(
    *,
    data_dir: Path,
    ref: str,
) -> bool:
    """Return whether any upstream still references ``ref``."""
    normalized = ref.removesuffix(".json")
    for record in load_registry_upstream_records(
        data_dir,
        include_disabled=True,
    ):
        candidate = record.get("secret_ref")
        if not isinstance(candidate, str):
            continue
        if candidate.removesuffix(".json") == normalized:
            return True
    return False


def list_upstreams(
    *,
    data_dir: Path | None = None,
    sync: bool = True,
) -> list[dict[str, Any]]:
    """List configured upstream entries from registry.

    Args:
        data_dir: Override data directory.
        sync: When True, bootstrap/merge registry from config
            before reading. Set False for read-only access.
    """
    resolved_data_dir = resolve_upstream_data_dir(data_dir)
    if sync:
        bootstrap_registry_from_config(resolved_data_dir)
        merge_missing_registry_from_config(resolved_data_dir)
    records = load_registry_upstream_records(
        resolved_data_dir,
        include_disabled=True,
    )

    rows: list[dict[str, Any]] = []
    for record in records:
        name = record["prefix"]
        transport = record["transport"]
        rows.append(
            {
                "name": name,
                "transport": transport,
                "enabled": bool(record["enabled"]),
                "command": record["command"],
                "url": record["url"],
                "args": list(record["args"]),
                "secret_ref": record["secret_ref"],
            }
        )
    return rows


def inspect_upstream(
    *,
    server: str,
    data_dir: Path | None = None,
    sync: bool = True,
) -> dict[str, Any]:
    """Return detailed metadata for one upstream entry from registry.

    Args:
        server: Upstream prefix to inspect.
        data_dir: Override data directory.
        sync: When True, bootstrap/merge registry from config
            before reading. Set False for read-only access.
    """
    resolved_data_dir = resolve_upstream_data_dir(data_dir)
    if sync:
        bootstrap_registry_from_config(resolved_data_dir)
        merge_missing_registry_from_config(resolved_data_dir)
    record = get_registry_upstream_record(
        data_dir=resolved_data_dir,
        prefix=server,
    )
    if record is None:
        msg = f"upstream {server!r} not found"
        raise ValueError(msg)

    transport = str(record["transport"])
    gateway_ext = _extract_gateway_fields(record)
    if not record["enabled"]:
        gateway_ext["enabled"] = False

    secret_ref = record["secret_ref"]
    secret_meta: dict[str, Any] | None = None
    if isinstance(secret_ref, str):
        try:
            secret = read_secret(
                resolved_data_dir,
                secret_ref.removesuffix(".json"),
            )
            env = secret.get("env")
            headers = secret.get("headers")
            secret_meta = {
                "ref": secret_ref,
                "transport": secret.get("transport"),
                "env_keys": sorted(env.keys()) if isinstance(env, dict) else [],
                "header_keys": sorted(headers.keys())
                if isinstance(headers, dict)
                else [],
                "updated_at": secret.get("updated_at"),
            }
        except Exception as exc:
            secret_meta = {"ref": secret_ref, "error": str(exc)}

    return {
        "name": server,
        "enabled": bool(record["enabled"]),
        "transport": transport,
        "command": record["command"],
        "url": record["url"],
        "args": list(record["args"]),
        "gateway": gateway_ext,
        "secret": secret_meta,
        "config_path": str(resolved_data_dir / "state" / "config.json"),
    }


def remove_upstream(
    *,
    server: str,
    data_dir: Path | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Remove one upstream entry from registry and mirror config."""
    resolved_data_dir = resolve_upstream_data_dir(data_dir)
    record = _resolve_mutation_record(
        data_dir=resolved_data_dir,
        server=server,
        dry_run=dry_run,
    )
    if record is None:
        msg = f"upstream {server!r} not found"
        raise ValueError(msg)
    secret_ref = (
        record["secret_ref"] if isinstance(record["secret_ref"], str) else None
    )

    if not dry_run:
        remove_registry_upstream(
            data_dir=resolved_data_dir,
            prefix=server,
        )
        config_path = mirror_registry_to_config(resolved_data_dir)
        if (
            isinstance(secret_ref, str)
            and secret_ref
            and not _secret_ref_is_still_referenced(
                data_dir=resolved_data_dir,
                ref=secret_ref,
            )
        ):
            _delete_secret_file(data_dir=resolved_data_dir, ref=secret_ref)
    else:
        config_path = resolved_data_dir / "state" / "config.json"

    return {
        "removed": server,
        "config_path": str(config_path),
        "dry_run": dry_run,
    }


def set_upstream_enabled(
    *,
    server: str,
    enabled: bool,
    data_dir: Path | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Enable or disable one upstream entry in registry."""
    resolved_data_dir = resolve_upstream_data_dir(data_dir)
    record = _resolve_mutation_record(
        data_dir=resolved_data_dir,
        server=server,
        dry_run=dry_run,
    )
    if record is None:
        msg = f"upstream {server!r} not found"
        raise ValueError(msg)

    if not dry_run:
        set_registry_upstream_enabled(
            data_dir=resolved_data_dir,
            prefix=server,
            enabled=enabled,
        )
        config_path = mirror_registry_to_config(resolved_data_dir)
    else:
        config_path = resolved_data_dir / "state" / "config.json"

    return {
        "server": server,
        "enabled": enabled,
        "config_path": str(config_path),
        "dry_run": dry_run,
    }


def set_upstream_auth(
    *,
    server: str,
    env_updates: dict[str, str] | None,
    header_updates: dict[str, str] | None,
    data_dir: Path | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Set upstream auth material and externalize to secret file."""
    updates_env = dict(env_updates or {})
    updates_headers = dict(header_updates or {})
    if not updates_env and not updates_headers:
        msg = "at least one of --env or --header is required"
        raise ValueError(msg)

    resolved_data_dir = resolve_upstream_data_dir(data_dir)
    record = _resolve_mutation_record(
        data_dir=resolved_data_dir,
        server=server,
        dry_run=dry_run,
    )
    if record is None:
        msg = f"upstream {server!r} not found"
        raise ValueError(msg)

    transport = str(record["transport"])
    if transport == "stdio" and updates_headers:
        msg = "--header is only supported for http upstreams"
        raise ValueError(msg)
    if transport == "http" and updates_env:
        msg = "--env is only supported for stdio upstreams"
        raise ValueError(msg)

    secret_ref = record["secret_ref"]
    if isinstance(secret_ref, str) and secret_ref:
        target_ref = secret_ref.removesuffix(".json")
    else:
        target_ref = server
    validate_prefix(target_ref)

    merged_env: dict[str, str] = {}
    merged_headers: dict[str, str] = {}
    secret_data: dict[str, Any] | None = None
    if dry_run:
        secret_data = _read_secret_from_file(
            data_dir=resolved_data_dir,
            ref=target_ref,
        )
    else:
        try:
            secret_data = read_secret(resolved_data_dir, target_ref)
        except (FileNotFoundError, json.JSONDecodeError):
            pass
        except Exception:
            _logger.warning(
                "failed to read existing secret %r for merge",
                target_ref,
                exc_info=True,
            )

    if isinstance(secret_data, dict):
        if transport == "stdio" and isinstance(secret_data.get("env"), dict):
            merged_env.update(
                {
                    str(k): str(v)
                    for k, v in (secret_data.get("env") or {}).items()
                }
            )
        if transport == "http" and isinstance(secret_data.get("headers"), dict):
            merged_headers.update(
                {
                    str(k): str(v)
                    for k, v in (secret_data.get("headers") or {}).items()
                }
            )

    merged_env.update(updates_env)
    merged_headers.update(updates_headers)

    if not dry_run:
        write_secret(
            resolved_data_dir,
            target_ref,
            transport=transport,
            env=merged_env if merged_env else None,
            headers=merged_headers if merged_headers else None,
        )
        set_registry_upstream_secret_ref(
            data_dir=resolved_data_dir,
            prefix=server,
            secret_ref=target_ref,
        )
        config_path = mirror_registry_to_config(resolved_data_dir)
    else:
        config_path = resolved_data_dir / "state" / "config.json"

    return {
        "server": server,
        "transport": transport,
        "secret_ref": target_ref,
        "updated_env_keys": sorted(updates_env),
        "updated_header_keys": sorted(updates_headers),
        "config_path": str(config_path),
        "dry_run": dry_run,
    }


async def _resolve_oauth_callback_url_headless(
    *,
    authorization_url: str,
    callback_port: int,
) -> str:
    """Resolve the local OAuth callback URL by following redirects."""
    import httpx

    next_url = authorization_url
    async with httpx.AsyncClient(timeout=30.0) as client:
        for _ in range(_OAUTH_REDIRECT_MAX_HOPS):
            response = await client.get(
                next_url,
                follow_redirects=False,
            )
            if response.status_code in _OAUTH_REDIRECT_STATUS_CODES:
                location = response.headers.get("location", "")
                if not isinstance(location, str) or not location:
                    msg = (
                        "OAuth authorization failed: redirect missing "
                        "location header"
                    )
                    raise RuntimeError(msg)
                redirect_url = urljoin(next_url, location)
                parsed = urlparse(redirect_url)
                if (
                    parsed.path == "/callback"
                    and parsed.port == callback_port
                    and parsed.hostname in ("localhost", "127.0.0.1")
                ):
                    return redirect_url
                next_url = redirect_url
                continue
            if response.status_code == 200:
                msg = (
                    "OAuth authorization requires interactive browser "
                    "login; headless flow cannot complete automatically."
                )
                raise RuntimeError(msg)
            msg = f"OAuth authorization failed: {response.status_code}"
            raise RuntimeError(msg)

    msg = (
        "OAuth authorization failed: too many redirects while resolving "
        "authorization callback URL"
    )
    raise RuntimeError(msg)


async def _oauth_login_access_token(
    *,
    url: str,
    headless: bool = False,
) -> str:
    """Run OAuth flow for an HTTP upstream and return an access token."""
    from fastmcp import Client
    from fastmcp.client.auth import OAuth
    from fastmcp.client.transports import (
        SSETransport,
        StreamableHttpTransport,
    )
    from fastmcp.mcp_config import infer_transport_type_from_url

    class _HeadlessOAuth(OAuth):
        """OAuth provider variant that avoids browser interaction."""

        def __init__(self, mcp_url: str):
            self._callback_url: str | None = None
            super().__init__(mcp_url)

        async def redirect_handler(self, authorization_url: str) -> None:
            self._callback_url = await _resolve_oauth_callback_url_headless(
                authorization_url=authorization_url,
                callback_port=self.redirect_port,
            )

        async def callback_handler(self) -> tuple[str, str | None]:
            callback_url = self._callback_url
            if callback_url is None:
                msg = (
                    "No authorization response received. OAuth redirect did "
                    "not start."
                )
                raise RuntimeError(msg)
            params = parse_qs(urlparse(callback_url).query)
            if "error" in params:
                error = params.get("error", ["unknown_error"])[0]
                error_desc = params.get(
                    "error_description", ["Unknown error"]
                )[0]
                msg = f"OAuth authorization failed: {error} - {error_desc}"
                raise RuntimeError(msg)

            auth_code = params.get("code", [None])[0]
            if not isinstance(auth_code, str) or not auth_code:
                msg = "OAuth authorization failed: missing authorization code"
                raise RuntimeError(msg)
            state = params.get("state", [None])[0]
            return auth_code, state if isinstance(state, str) else None

    oauth = _HeadlessOAuth(url) if headless else OAuth(url)
    inferred = infer_transport_type_from_url(url)
    if inferred == "sse":
        transport = SSETransport(url=url, auth=oauth)
    else:
        transport = StreamableHttpTransport(url=url, auth=oauth)

    async with Client(transport, timeout=30.0) as client:
        await client.list_tools()

    tokens = getattr(oauth, "context", None)
    current_tokens = (
        getattr(tokens, "current_tokens", None) if tokens is not None else None
    )
    raw_access_token = (
        getattr(current_tokens, "access_token", None)
        if current_tokens is not None
        else None
    )
    access_token = (
        str(raw_access_token).strip()
        if isinstance(raw_access_token, str)
        else ""
    )
    if not access_token:
        msg = (
            "OAuth login completed but no access token was returned by the "
            "upstream."
        )
        raise RuntimeError(msg)
    return access_token


def login_upstream(
    *,
    server: str,
    data_dir: Path | None = None,
    dry_run: bool = False,
    headless: bool = False,
) -> dict[str, Any]:
    """Run OAuth login for one HTTP upstream and persist auth header."""
    resolved_data_dir = resolve_upstream_data_dir(data_dir)
    record = _resolve_mutation_record(
        data_dir=resolved_data_dir,
        server=server,
        dry_run=dry_run,
    )
    if record is None:
        msg = f"upstream {server!r} not found"
        raise ValueError(msg)

    transport = str(record["transport"])
    if transport != "http":
        msg = "upstream login is only supported for http upstreams"
        raise ValueError(msg)

    url = record.get("url")
    if not isinstance(url, str) or not url:
        msg = f"upstream {server!r} has no HTTP url configured"
        raise ValueError(msg)

    if dry_run:
        result = set_upstream_auth(
            server=server,
            env_updates=None,
            header_updates={"Authorization": "Bearer __OAUTH_ACCESS_TOKEN__"},
            data_dir=resolved_data_dir,
            dry_run=True,
        )
        result["login"] = "oauth"
        return result

    access_token = asyncio.run(
        _oauth_login_access_token(url=url, headless=headless)
    )
    result = set_upstream_auth(
        server=server,
        env_updates=None,
        header_updates={"Authorization": f"Bearer {access_token}"},
        data_dir=resolved_data_dir,
        dry_run=False,
    )
    result["login"] = "oauth"
    return result


def normalize_input_servers(
    raw_servers: dict[str, Any],
    *,
    strict: bool = True,
) -> dict[str, dict[str, Any]]:
    """Normalize raw snippet input to a bare mcpServers-like map.

    Args:
        raw_servers: Raw JSON dict that may be a bare server map or
            wrapped in ``mcpServers`` / ``mcp.servers``.
        strict: When True, propagate ``ValueError`` from
            ``extract_mcp_servers`` and do not filter non-dict
            entries (suitable for user-facing validation).  When
            False, swallow ``ValueError`` and silently skip
            non-dict entries (suitable for best-effort
            reconciliation).

    Returns:
        Normalized server map (name -> config dict).
    """
    mcp_block = raw_servers.get("mcp")
    is_wrapped = "mcpServers" in raw_servers or (
        isinstance(mcp_block, dict) and "servers" in mcp_block
    )
    if is_wrapped:
        if strict:
            return extract_mcp_servers(raw_servers)
        try:
            extracted = extract_mcp_servers(raw_servers)
        except ValueError:
            return {}
        return {
            name: entry
            for name, entry in extracted.items()
            if isinstance(entry, dict)
        }
    if strict:
        return raw_servers
    return {
        str(name): entry
        for name, entry in raw_servers.items()
        if isinstance(name, str) and isinstance(entry, dict)
    }


def reconcile_after_add(
    *,
    data_dir: Path,
    raw_input: dict[str, Any],
    added_names: set[str],
    warnings: list[str],
) -> None:
    """Reconcile newly-added upstreams with the canonical registry.

    Args:
        data_dir: Resolved data directory.
        raw_input: Original raw snippet/flag input dict.
        added_names: Set of upstream prefixes that were added.
        warnings: Mutable list that receives warning messages.
    """
    if not added_names:
        return

    registry_sync_failed = False
    try:
        bootstrap_registry_from_config(data_dir)
        merge_missing_registry_from_config(data_dir)
    except Exception as exc:
        registry_sync_failed = True
        if isinstance(exc, ValueError):
            warnings.append(
                "skipped full registry sync due to invalid "
                f"mcpServers mirror: {exc}"
            )
        else:
            warnings.append(
                f"skipped full registry sync due to runtime error: {exc}"
            )

    can_reconcile = True
    if registry_sync_failed:
        load_warned = False
        try:
            can_reconcile = bool(
                load_registry_upstream_records(
                    data_dir,
                    include_disabled=True,
                )
            )
        except Exception as exc:
            can_reconcile = False
            load_warned = True
            warnings.append(
                "skipped registry reconciliation for "
                "newly-added upstream(s) because registry "
                f"snapshot could not be loaded: {exc}"
            )

        if not can_reconcile and not load_warned:
            warnings.append(
                "skipped registry reconciliation for "
                "newly-added upstream(s) because registry "
                "bootstrap did not establish a canonical "
                "snapshot."
            )

    if not can_reconcile:
        return

    config_path = gateway_config_path(data_dir)
    raw_config = load_gateway_config_dict(config_path)

    added_servers: dict[str, dict[str, Any]] = {}
    try:
        config_servers = extract_mcp_servers(raw_config)
    except ValueError:
        config_servers = {}
    else:
        added_servers = {
            name: entry
            for name, entry in config_servers.items()
            if name in added_names and isinstance(entry, dict)
        }

    if not added_servers:
        source_servers = normalize_input_servers(raw_input, strict=False)
        added_servers = {
            name: entry
            for name, entry in source_servers.items()
            if name in added_names
        }

    if added_servers:
        try:
            upsert_registry_from_mcp_servers(
                data_dir=data_dir,
                servers=added_servers,
                merge_missing=False,
                source_kind="snippet_add",
            )
            mirror_registry_to_config(data_dir)
        except Exception as exc:
            warnings.append(
                "upstream add wrote config.json but "
                f"registry reconciliation failed: {exc}"
            )


async def _probe_one_upstream(
    upstream: Any,
    data_dir: Path,
) -> dict[str, Any]:
    """Probe a single upstream and return its result dict."""
    try:
        tools = await discover_tools(
            upstream,
            data_dir=str(data_dir),
        )
    except Exception as exc:
        return {
            "name": upstream.prefix,
            "ok": False,
            "error_code": classify_upstream_exception(exc),
            "error": str(exc),
        }
    return {
        "name": upstream.prefix,
        "ok": True,
        "tool_count": len(tools),
    }


async def _probe_upstream_configs(
    *,
    upstreams: list[Any],
    data_dir: Path,
) -> list[dict[str, Any]]:
    """Probe upstreams concurrently via tools discovery."""
    return list(
        await asyncio.gather(
            *(_probe_one_upstream(upstream, data_dir) for upstream in upstreams)
        )
    )


def probe_upstreams(
    *,
    server: str | None = None,
    all_servers: bool = False,
    data_dir: Path | None = None,
) -> dict[str, Any]:
    """Active probe for configured upstreams."""
    if server and all_servers:
        msg = "--server and --all are mutually exclusive"
        raise ValueError(msg)
    if not server and not all_servers:
        msg = "one of --server or --all is required"
        raise ValueError(msg)

    resolved_data_dir = resolve_upstream_data_dir(data_dir)
    from sift_gateway.config import load_gateway_config

    config = load_gateway_config(data_dir_override=str(resolved_data_dir))
    active = list(config.upstreams)
    if server:
        active = [up for up in active if up.prefix == server]
        if not active:
            raw_items = list_upstreams(data_dir=resolved_data_dir)
            disabled = any(
                item["name"] == server and not item["enabled"]
                for item in raw_items
            )
            if disabled:
                msg = f"upstream {server!r} is disabled"
                raise ValueError(msg)
            msg = f"upstream {server!r} not found"
            raise ValueError(msg)

    results = asyncio.run(
        _probe_upstream_configs(
            upstreams=active,
            data_dir=resolved_data_dir,
        )
    )
    ok_count = sum(1 for item in results if item.get("ok"))
    return {
        "results": results,
        "ok": ok_count == len(results),
        "ok_count": ok_count,
        "total": len(results),
    }
