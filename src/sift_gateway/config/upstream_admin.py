"""Administrative helpers for upstream registration workflows.

These helpers provide ergonomic CRUD-style operations over the
``mcpServers`` section in ``state/config.json`` while preserving
secret externalization via ``state/upstream_secrets`` files.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
import contextlib
import json
import logging
import os
from pathlib import Path
import shutil
import tempfile
from typing import Any
from urllib.parse import urljoin, urlparse

from sift_gateway.auth.config import (
    AUTH_MODE_GOOGLE_ADC,
    AUTH_MODE_OAUTH,
    OAUTH_REGISTRATION_DYNAMIC,
    OAUTH_REGISTRATION_PREREGISTERED,
    OAUTH_STATIC_CLIENT_CALLBACK_PORT,
)
from sift_gateway.auth.config import (
    auth_enabled as _auth_enabled,
)
from sift_gateway.auth.config import (
    auth_mode as _auth_mode,
)
from sift_gateway.auth.config import (
    auth_scope as _auth_scope,
)
from sift_gateway.auth.config import (
    normalize_auth_config as _normalize_oauth_config,
)
from sift_gateway.auth.config import (
    oauth_callback_port as _oauth_config_callback_port,
)
from sift_gateway.auth.config import (
    oauth_login_requires_session_reset as _oauth_login_requires_session_reset,
)
from sift_gateway.auth.config import (
    oauth_registration as _oauth_registration,
)
from sift_gateway.auth.config import (
    uses_oauth_session as _uses_oauth_session,
)
from sift_gateway.auth.oauth_login import (
    oauth_apply_client_config as _oauth_apply_client_config_impl,
)
from sift_gateway.auth.oauth_login import (
    oauth_async_auth_flow_once as _oauth_async_auth_flow_once,
)
from sift_gateway.auth.oauth_login import (
    oauth_client_info_from_config as _oauth_client_info_from_config_impl,
)
from sift_gateway.auth.oauth_login import (
    oauth_login_access_token as _oauth_login_access_token_impl,
)
from sift_gateway.auth.oauth_login import (
    oauth_login_access_token_proactive as _oauth_login_access_token_proactive_impl,
)
from sift_gateway.config.mcp_servers import extract_mcp_servers
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
    clear_oauth_client_registration,
    clear_oauth_session,
    mark_oauth_access_token_stale,
    oauth_cache_dir_path,
    oauth_token_storage,
    read_oauth_access_token,
    read_secret,
    secret_file_path,
    validate_prefix,
    write_secret,
)
from sift_gateway.config.upstream_secrets import (
    delete_oauth_server_auth_config as _delete_oauth_server_auth_config_impl,
)
from sift_gateway.config.upstream_secrets import (
    effective_oauth_server_auth_config as _effective_oauth_server_auth_config,
)
from sift_gateway.config.upstream_secrets import (
    oauth_server_auth_config_path as _oauth_server_auth_config_path_impl,
)
from sift_gateway.config.upstream_secrets import (
    read_oauth_server_auth_config as _read_oauth_server_auth_config_impl,
)
from sift_gateway.config.upstream_secrets import (
    write_oauth_server_auth_config as _write_oauth_server_auth_config_impl,
)
from sift_gateway.constants import DEFAULT_DATA_DIR
from sift_gateway.mcp.upstream import discover_tools
from sift_gateway.mcp.upstream_errors import classify_upstream_exception

_logger = logging.getLogger(__name__)

_OAUTH_REDIRECT_STATUS_CODES = (301, 302, 303, 307, 308)
_OAUTH_REDIRECT_MAX_HOPS = 20


def _oauth_dependency_runtime_error(
    exc: BaseException,
) -> RuntimeError | None:
    """Return a user-facing RuntimeError for known OAuth dependency gaps."""
    if "py-key-value-aio[disk]" not in str(exc):
        return None
    msg = (
        "OAuth login could not initialize token storage because "
        "`py-key-value-aio[disk]` is missing. Install it and retry with: "
        "pip install 'py-key-value-aio[disk]'"
    )
    return RuntimeError(msg)


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


def _delete_oauth_cache_dir(
    *,
    data_dir: Path,
    ref: str | None,
) -> None:
    """Delete per-upstream OAuth cache directory for ``ref`` when present."""
    if not isinstance(ref, str) or not ref:
        return
    path = oauth_cache_dir_path(data_dir, ref)
    if not path.exists():
        return
    with contextlib.suppress(OSError):
        shutil.rmtree(path)


def _oauth_server_config_path(
    *,
    data_dir: Path,
    ref: str,
    server_url: str,
) -> Path:
    """Return the per-server OAuth config path inside one cache directory."""
    return _oauth_server_auth_config_path_impl(
        data_dir,
        ref,
        server_url,
    )


def _read_oauth_server_auth_config(
    *,
    data_dir: Path,
    ref: str,
    server_url: str,
) -> dict[str, Any] | None:
    """Read per-server OAuth login config for shared secret refs."""
    return _read_oauth_server_auth_config_impl(
        data_dir,
        ref,
        server_url=server_url,
    )


def _write_oauth_server_auth_config(
    *,
    data_dir: Path,
    ref: str,
    server_url: str,
    oauth_config: dict[str, Any],
) -> None:
    """Persist per-server OAuth login config for shared secret refs."""
    _write_oauth_server_auth_config_impl(
        data_dir,
        ref,
        server_url=server_url,
        oauth_config=oauth_config,
    )


def _delete_oauth_server_auth_config(
    *,
    data_dir: Path,
    ref: str | None,
    server_url: str | None,
) -> None:
    """Delete per-server OAuth login config for one shared secret ref."""
    if (
        not isinstance(ref, str)
        or not ref
        or not isinstance(server_url, str)
        or not server_url
    ):
        return
    _delete_oauth_server_auth_config_impl(
        data_dir,
        ref,
        server_url=server_url,
    )


def _shared_oauth_secret_config(
    oauth_config: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """Return auth metadata safe to persist in a shared secret file."""
    normalized = _normalize_oauth_config(oauth_config)
    return dict(normalized) if isinstance(normalized, dict) else None


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


def _secret_ref_is_shared(
    *,
    data_dir: Path,
    ref: str,
    server: str,
) -> bool:
    """Return whether another upstream also uses ``ref``."""
    normalized = ref.removesuffix(".json")
    records = load_registry_upstream_records(
        data_dir,
        include_disabled=True,
    )
    if records:
        for record in records:
            prefix = record.get("prefix")
            if not isinstance(prefix, str) or prefix == server:
                continue
            candidate = record.get("secret_ref")
            if (
                isinstance(candidate, str)
                and candidate.removesuffix(".json") == normalized
            ):
                return True
        return False

    raw_config = load_gateway_config_dict(gateway_config_path(data_dir))
    try:
        servers = extract_mcp_servers(raw_config)
    except ValueError:
        return False
    for name, entry in servers.items():
        if name == server or not isinstance(entry, dict):
            continue
        gateway = entry.get("_gateway")
        candidate = (
            gateway.get("secret_ref") if isinstance(gateway, dict) else None
        )
        if (
            isinstance(candidate, str)
            and candidate.removesuffix(".json") == normalized
        ):
            return True
    return False


def _oauth_server_auth_config_is_still_referenced(
    *,
    data_dir: Path,
    ref: str,
    server_url: str,
) -> bool:
    """Return whether any upstream still uses one shared ref + server URL."""
    normalized_ref = ref.removesuffix(".json")
    normalized_url = server_url.rstrip("/")
    for record in load_registry_upstream_records(
        data_dir,
        include_disabled=True,
    ):
        candidate_ref = record.get("secret_ref")
        candidate_url = record.get("url")
        if (
            isinstance(candidate_ref, str)
            and candidate_ref.removesuffix(".json") == normalized_ref
            and isinstance(candidate_url, str)
            and candidate_url.rstrip("/") == normalized_url
        ):
            return True
    return False


def _records_for_secret_ref(
    *,
    data_dir: Path,
    ref: str,
) -> list[dict[str, Any]]:
    """Return registry records that still reference one secret ref."""
    normalized = ref.removesuffix(".json")
    return [
        record
        for record in load_registry_upstream_records(
            data_dir,
            include_disabled=True,
        )
        if isinstance(record.get("secret_ref"), str)
        and record["secret_ref"].removesuffix(".json") == normalized
    ]


def _delete_one_upstream_oauth_state(
    *,
    data_dir: Path,
    ref: str | None,
    server_url: str | None,
) -> None:
    """Delete one server URL's cached OAuth state for a shared secret ref."""
    if (
        not isinstance(ref, str)
        or not ref
        or not isinstance(server_url, str)
        or not server_url
    ):
        return

    _delete_oauth_server_auth_config(
        data_dir=data_dir,
        ref=ref,
        server_url=server_url,
    )
    try:
        asyncio.run(
            clear_oauth_session(
                token_storage=oauth_token_storage(data_dir, ref),
                server_url=server_url,
            )
        )
    except Exception:
        _logger.debug(
            "skipped oauth session cleanup for shared ref %s (%s)",
            ref,
            server_url,
            exc_info=True,
        )


def _effective_oauth_config_for_server(
    *,
    data_dir: Path,
    ref: str | None,
    server: str | None,
    server_url: str | None,
    oauth_config: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """Return effective OAuth config only while a ref is actively shared."""
    normalized = _normalize_oauth_config(oauth_config)
    if (
        not isinstance(ref, str)
        or not ref
        or not isinstance(server, str)
        or not server
        or not isinstance(server_url, str)
        or not server_url
        or not _secret_ref_is_shared(
            data_dir=data_dir,
            ref=ref,
            server=server,
        )
    ):
        return dict(normalized) if isinstance(normalized, dict) else None
    return _effective_oauth_server_auth_config(
        data_dir=data_dir,
        ref=ref,
        server_url=server_url,
        oauth_config=oauth_config,
    )


def _oauth_config_for_inspect(
    *,
    data_dir: Path,
    ref: str | None,
    server: str | None,
    server_url: str | None,
    oauth_config: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """Return the effective OAuth config for inspect output."""
    return _effective_oauth_config_for_server(
        data_dir=data_dir,
        ref=ref,
        server=server,
        server_url=server_url,
        oauth_config=oauth_config,
    )


def _oauth_inspect_metadata(
    oauth_config: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """Return a secret-safe OAuth metadata view for inspect output."""
    normalized = _normalize_oauth_config(oauth_config)
    if not isinstance(normalized, dict):
        return None

    mode = _auth_mode(normalized)
    metadata: dict[str, Any] = {
        "enabled": _auth_enabled(normalized),
        "mode": mode,
        "token_storage": (
            normalized.get("token_storage")
            if isinstance(normalized.get("token_storage"), str)
            else None
        ),
    }
    if mode == AUTH_MODE_OAUTH:
        metadata["registration"] = _oauth_registration(normalized)
        scope = _auth_scope(normalized)
        if isinstance(scope, str):
            metadata["scope"] = scope
        callback_port = _oauth_config_callback_port(normalized)
        if callback_port is not None:
            metadata["callback_port"] = callback_port
        client_id = normalized.get("client_id")
        if isinstance(client_id, str) and client_id.strip():
            metadata["client_id"] = client_id.strip()
        auth_method = normalized.get("token_endpoint_auth_method")
        if isinstance(auth_method, str) and auth_method.strip():
            metadata["token_endpoint_auth_method"] = auth_method.strip()
        metadata_url = normalized.get("client_metadata_url")
        if isinstance(metadata_url, str) and metadata_url.strip():
            metadata["client_metadata_url"] = metadata_url.strip()
    elif mode == AUTH_MODE_GOOGLE_ADC:
        google_scopes = normalized.get("google_scopes")
        if isinstance(google_scopes, list):
            scopes = [
                str(scope).strip()
                for scope in google_scopes
                if str(scope).strip()
            ]
            if scopes:
                metadata["google_scopes"] = scopes

    return metadata


def _collapse_unshared_secret_ref_oauth_state(
    *,
    data_dir: Path,
    ref: str | None,
) -> None:
    """Move surviving sidecar auth back into the secret when a ref unshares."""
    if not isinstance(ref, str) or not ref:
        return

    normalized_ref = ref.removesuffix(".json")
    records = _records_for_secret_ref(
        data_dir=data_dir,
        ref=normalized_ref,
    )
    if len(records) != 1:
        return

    remaining = records[0]
    if remaining.get("transport") != "http":
        return
    server_url = remaining.get("url")
    if not isinstance(server_url, str) or not server_url:
        return

    try:
        secret = read_secret(data_dir, normalized_ref)
    except Exception:
        _logger.debug(
            "skipped oauth sidecar collapse for ref %s",
            normalized_ref,
            exc_info=True,
        )
        return

    raw_oauth = (
        secret.get("oauth") if isinstance(secret.get("oauth"), dict) else None
    )
    effective_oauth = _effective_oauth_server_auth_config(
        data_dir=data_dir,
        ref=normalized_ref,
        server_url=server_url,
        oauth_config=raw_oauth,
    )
    normalized_secret_oauth = _normalize_oauth_config(raw_oauth)
    env = secret.get("env") if isinstance(secret.get("env"), dict) else None
    raw_headers = secret.get("headers")
    headers = dict(raw_headers) if isinstance(raw_headers, dict) else None
    if _uses_oauth_session(effective_oauth):
        try:
            access_token = asyncio.run(
                read_oauth_access_token(
                    oauth_token_storage(data_dir, normalized_ref),
                    server_url=server_url,
                )
            )
        except Exception:
            access_token = None
            _logger.debug(
                "skipped oauth fallback header snapshot for ref %s",
                normalized_ref,
                exc_info=True,
            )
        if access_token:
            if headers is None:
                headers = {}
            headers["Authorization"] = f"Bearer {access_token}"

    current_headers = (
        secret.get("headers")
        if isinstance(secret.get("headers"), dict)
        else None
    )
    if effective_oauth != normalized_secret_oauth or headers != current_headers:
        raw_transport: object = secret.get("transport")
        transport = raw_transport if isinstance(raw_transport, str) else "http"
        write_secret(
            data_dir,
            normalized_ref,
            transport=transport,
            env=env,
            headers=headers,
            oauth=effective_oauth if effective_oauth else None,
        )

    _delete_oauth_server_auth_config(
        data_dir=data_dir,
        ref=normalized_ref,
        server_url=server_url,
    )


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
    server_url = record["url"] if isinstance(record.get("url"), str) else None
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
            oauth = _oauth_config_for_inspect(
                data_dir=resolved_data_dir,
                ref=secret_ref.removesuffix(".json"),
                server=server,
                server_url=server_url,
                oauth_config=secret.get("oauth"),
            )
            oauth_meta = _oauth_inspect_metadata(oauth)
            if isinstance(oauth_meta, dict):
                secret_meta["oauth"] = oauth_meta
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
    server_url = record["url"] if isinstance(record.get("url"), str) else None

    if not dry_run:
        remove_registry_upstream(
            data_dir=resolved_data_dir,
            prefix=server,
        )
        config_path = mirror_registry_to_config(resolved_data_dir)
        if (
            isinstance(secret_ref, str)
            and secret_ref
            and isinstance(server_url, str)
            and server_url
            and not _oauth_server_auth_config_is_still_referenced(
                data_dir=resolved_data_dir,
                ref=secret_ref,
                server_url=server_url,
            )
        ):
            _delete_one_upstream_oauth_state(
                data_dir=resolved_data_dir,
                ref=secret_ref,
                server_url=server_url,
            )
        if (
            isinstance(secret_ref, str)
            and secret_ref
            and not _secret_ref_is_still_referenced(
                data_dir=resolved_data_dir,
                ref=secret_ref,
            )
        ):
            _delete_secret_file(data_dir=resolved_data_dir, ref=secret_ref)
            _delete_oauth_cache_dir(
                data_dir=resolved_data_dir,
                ref=secret_ref,
            )
        elif isinstance(secret_ref, str) and secret_ref:
            _collapse_unshared_secret_ref_oauth_state(
                data_dir=resolved_data_dir,
                ref=secret_ref,
            )
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
    oauth: dict[str, Any] | None = None,
    merge_oauth: bool = False,
    clear_oauth: bool = True,
    data_dir: Path | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Set upstream auth material and externalize to secret file."""
    updates_env = dict(env_updates or {})
    updates_headers = dict(header_updates or {})
    if not updates_env and not updates_headers and oauth is None:
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
    if transport != "http" and oauth is not None:
        msg = "OAuth runtime auth is only supported for http upstreams"
        raise ValueError(msg)

    secret_ref = record["secret_ref"]
    if isinstance(secret_ref, str) and secret_ref:
        target_ref = secret_ref.removesuffix(".json")
    else:
        target_ref = server
    validate_prefix(target_ref)
    server_url = record["url"] if isinstance(record.get("url"), str) else None
    shared_secret_ref = transport == "http" and _secret_ref_is_shared(
        data_dir=resolved_data_dir,
        ref=target_ref,
        server=server,
    )

    merged_env: dict[str, str] = {}
    merged_headers: dict[str, str] = {}
    merged_oauth: dict[str, Any] | None = None
    shared_secret_oauth: dict[str, Any] | None = None
    existing_uses_oauth_session = False
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
        raw_oauth = (
            secret_data.get("oauth")
            if isinstance(secret_data.get("oauth"), dict)
            else None
        )
        shared_secret_oauth = (
            _shared_oauth_secret_config(raw_oauth)
            if transport == "http" and shared_secret_ref
            else None
        )
        merged_oauth = (
            _effective_oauth_server_auth_config(
                data_dir=resolved_data_dir,
                ref=target_ref if shared_secret_ref else None,
                server_url=server_url if shared_secret_ref else None,
                oauth_config=raw_oauth,
            )
            if transport == "http"
            else _normalize_oauth_config(raw_oauth)
        )
        existing_uses_oauth_session = _uses_oauth_session(merged_oauth)

    merged_env.update(updates_env)
    merged_headers.update(updates_headers)
    if oauth is not None:
        if merge_oauth and isinstance(merged_oauth, dict):
            merged_oauth = {**merged_oauth, **oauth}
        else:
            merged_oauth = dict(oauth)
    elif clear_oauth:
        merged_oauth = None
    merged_oauth = _normalize_oauth_config(merged_oauth)
    merged_auth_mode = _auth_mode(merged_oauth)
    merged_uses_oauth_session = _uses_oauth_session(merged_oauth)
    delete_oauth_cache = clear_oauth and not merged_oauth

    if transport == "http" and merged_auth_mode == AUTH_MODE_GOOGLE_ADC:
        if not shared_secret_ref:
            merged_headers = {
                key: value
                for key, value in merged_headers.items()
                if key.lower() != "authorization"
            }
        delete_oauth_cache = True
    elif (
        transport == "http"
        and existing_uses_oauth_session
        and not merged_uses_oauth_session
    ):
        delete_oauth_cache = True

    if not dry_run:
        if delete_oauth_cache:
            if shared_secret_ref:
                _delete_one_upstream_oauth_state(
                    data_dir=resolved_data_dir,
                    ref=target_ref,
                    server_url=server_url,
                )
            else:
                _delete_oauth_cache_dir(
                    data_dir=resolved_data_dir,
                    ref=target_ref,
                )

        desired_sidecar_oauth: dict[str, Any] | None = None
        if transport == "http" and shared_secret_ref:
            if isinstance(shared_secret_oauth, dict):
                if isinstance(merged_oauth, dict):
                    if merged_oauth != shared_secret_oauth:
                        desired_sidecar_oauth = dict(merged_oauth)
                else:
                    desired_sidecar_oauth = {"enabled": False}
            elif isinstance(merged_oauth, dict):
                desired_sidecar_oauth = dict(merged_oauth)

        secret_oauth = (
            shared_secret_oauth
            if transport == "http" and shared_secret_ref
            else (merged_oauth if merged_oauth else None)
        )
        write_secret(
            resolved_data_dir,
            target_ref,
            transport=transport,
            env=merged_env if merged_env else None,
            headers=merged_headers if merged_headers else None,
            oauth=secret_oauth,
        )
        if (
            transport == "http"
            and shared_secret_ref
            and isinstance(server_url, str)
            and server_url
        ):
            if isinstance(desired_sidecar_oauth, dict):
                _write_oauth_server_auth_config(
                    data_dir=resolved_data_dir,
                    ref=target_ref,
                    server_url=server_url,
                    oauth_config=desired_sidecar_oauth,
                )
            else:
                _delete_oauth_server_auth_config(
                    data_dir=resolved_data_dir,
                    ref=target_ref,
                    server_url=server_url,
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
        "oauth_enabled": bool(
            isinstance(merged_oauth, dict) and merged_oauth.get("enabled")
        ),
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


def _oauth_config_provider(oauth_config: dict[str, Any] | None) -> str | None:
    """Return the normalized auth mode from config."""
    return _auth_mode(_normalize_oauth_config(oauth_config))


def _oauth_config_scope(oauth_config: dict[str, Any] | None) -> str | None:
    """Return a normalized scope string from stored auth config."""
    return _auth_scope(_normalize_oauth_config(oauth_config))


async def _oauth_apply_client_config(
    *,
    oauth: Any,
    oauth_config: dict[str, Any] | None,
) -> None:
    """Apply explicit client config using the legacy upstream-admin signature."""
    await _oauth_apply_client_config_impl(
        oauth=oauth,
        auth_config=oauth_config,
    )


def _oauth_client_info_from_config(
    *,
    oauth_config: dict[str, Any] | None,
    redirect_uris: Any,
    client_name: str | None,
) -> Any | None:
    """Build client info using the legacy upstream-admin signature."""
    return _oauth_client_info_from_config_impl(
        auth_config=oauth_config,
        redirect_uris=redirect_uris,
        client_name=client_name,
    )


async def _oauth_login_access_token_proactive(
    *,
    oauth: Any,
    url: str,
    retry_on_stale_client: bool = True,
) -> str:
    """Run proactive OAuth login using the legacy upstream-admin hook."""
    return await _oauth_login_access_token_proactive_impl(
        oauth=oauth,
        url=url,
        retry_on_stale_client=retry_on_stale_client,
    )


@contextlib.contextmanager
def _preserve_oauth_cache_dir(
    *,
    data_dir: Path,
    ref: str,
    existed: bool,
) -> Iterator[None]:
    """Restore the upstream OAuth cache directory if login fails."""
    cache_dir = oauth_cache_dir_path(data_dir, ref)
    with tempfile.TemporaryDirectory() as tmp_dir_str:
        snapshot_dir = Path(tmp_dir_str) / "oauth-cache"
        if existed and cache_dir.is_dir():
            shutil.copytree(cache_dir, snapshot_dir)
        try:
            yield
        except Exception:
            if cache_dir.exists():
                shutil.rmtree(cache_dir)
            if existed and snapshot_dir.is_dir():
                cache_dir.parent.mkdir(parents=True, exist_ok=True)
                shutil.copytree(snapshot_dir, cache_dir)
            raise


async def _oauth_login_access_token(
    *,
    url: str,
    headless: bool = False,
    token_storage: Any | None = None,
    oauth_config: dict[str, Any] | None = None,
) -> str:
    """Run OAuth flow for an HTTP upstream and return an access token."""
    try:
        return await _oauth_login_access_token_impl(
            url=url,
            resolve_callback_url_headless=_resolve_oauth_callback_url_headless,
            auth_flow_once=_oauth_async_auth_flow_once,
            proactive_access_token=_oauth_login_access_token_proactive,
            headless=headless,
            token_storage=token_storage,
            auth_config=oauth_config,
        )
    except TypeError as exc:
        if "token_storage" not in str(exc):
            raise
        msg = (
            "OAuth provider does not support configurable token storage in "
            "this environment."
        )
        raise RuntimeError(msg) from exc


def login_upstream(
    *,
    server: str,
    data_dir: Path | None = None,
    dry_run: bool = False,
    headless: bool = False,
    oauth_client_id: str | None = None,
    oauth_client_secret: str | None = None,
    oauth_auth_method: str | None = None,
    oauth_registration: str | None = None,
    oauth_scopes: list[str] | None = None,
    oauth_callback_port: int | None = None,
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
    secret_ref = record["secret_ref"]
    if isinstance(secret_ref, str) and secret_ref:
        target_ref = secret_ref.removesuffix(".json")
    else:
        target_ref = server
    validate_prefix(target_ref)
    shared_secret_ref = _secret_ref_is_shared(
        data_dir=resolved_data_dir,
        ref=target_ref,
        server=server,
    )
    existing_oauth: dict[str, Any] = {}
    existing_oauth_session: dict[str, Any] = {}
    shared_reusable_oauth: dict[str, Any] = {}
    try:
        secret = (
            _read_secret_from_file(
                data_dir=resolved_data_dir,
                ref=target_ref,
            )
            if dry_run
            else read_secret(resolved_data_dir, target_ref)
        )
        if isinstance(secret, dict):
            raw_oauth = secret.get("oauth")
            if isinstance(raw_oauth, dict):
                normalized_oauth = _normalize_oauth_config(raw_oauth) or {}
                shared_reusable_oauth = (
                    _shared_oauth_secret_config(normalized_oauth) or {}
                    if shared_secret_ref
                    else dict(normalized_oauth)
                )
                existing_oauth_session = (
                    dict(shared_reusable_oauth)
                    if shared_secret_ref
                    else dict(normalized_oauth)
                )
                existing_oauth = (
                    dict(existing_oauth_session)
                    if _uses_oauth_session(existing_oauth_session)
                    else {}
                )
    except (FileNotFoundError, json.JSONDecodeError, ValueError):
        pass
    except Exception:
        _logger.warning(
            "failed to read existing secret %r for oauth login",
            target_ref,
            exc_info=True,
        )

    if shared_secret_ref:
        cached_oauth = _read_oauth_server_auth_config(
            data_dir=resolved_data_dir,
            ref=target_ref,
            server_url=url,
        )
        if isinstance(cached_oauth, dict):
            existing_oauth_session = {
                **existing_oauth_session,
                **cached_oauth,
            }
            if _uses_oauth_session(existing_oauth_session):
                existing_oauth = dict(existing_oauth_session)
            elif _uses_oauth_session(shared_reusable_oauth):
                existing_oauth = dict(shared_reusable_oauth)

    oauth_meta = dict(existing_oauth)
    oauth_meta.update(
        {
            "enabled": True,
            "mode": AUTH_MODE_OAUTH,
            "registration": OAUTH_REGISTRATION_DYNAMIC,
            "token_storage": "disk",
        }
    )
    explicit_registration = (
        oauth_registration.strip()
        if isinstance(oauth_registration, str) and oauth_registration.strip()
        else None
    )
    if explicit_registration not in {
        None,
        OAUTH_REGISTRATION_DYNAMIC,
        OAUTH_REGISTRATION_PREREGISTERED,
    }:
        msg = (
            "OAuth registration must be one of "
            f"{OAUTH_REGISTRATION_DYNAMIC!r} or "
            f"{OAUTH_REGISTRATION_PREREGISTERED!r}."
        )
        raise ValueError(msg)
    explicit_client_id = (
        oauth_client_id.strip()
        if isinstance(oauth_client_id, str) and oauth_client_id.strip()
        else None
    )
    explicit_client_secret = (
        oauth_client_secret.strip()
        if isinstance(oauth_client_secret, str) and oauth_client_secret.strip()
        else None
    )
    explicit_auth_method = (
        oauth_auth_method.strip()
        if isinstance(oauth_auth_method, str) and oauth_auth_method.strip()
        else None
    )
    raw_existing_client_id: object = oauth_meta.get("client_id")
    existing_client_id = (
        raw_existing_client_id.strip()
        if isinstance(raw_existing_client_id, str)
        and raw_existing_client_id.strip()
        else None
    )
    effective_client_id = (
        explicit_client_id
        if explicit_client_id is not None
        else existing_client_id
    )
    if explicit_client_secret is not None and effective_client_id is None:
        msg = (
            "--oauth-client-secret requires --oauth-client-id or an existing "
            "stored client_id"
        )
        raise ValueError(msg)
    if explicit_auth_method is not None and effective_client_id is None:
        msg = (
            "--oauth-auth-method requires --oauth-client-id or an existing "
            "stored client_id"
        )
        raise ValueError(msg)
    if explicit_registration == OAUTH_REGISTRATION_DYNAMIC and (
        explicit_client_id is not None
        or explicit_client_secret is not None
        or explicit_auth_method is not None
    ):
        msg = (
            "--oauth-registration dynamic cannot be combined with "
            "--oauth-client-id, --oauth-client-secret, or "
            "--oauth-auth-method"
        )
        raise ValueError(msg)

    effective_registration = explicit_registration
    if effective_registration is None:
        effective_registration = (
            OAUTH_REGISTRATION_PREREGISTERED
            if effective_client_id is not None
            else OAUTH_REGISTRATION_DYNAMIC
        )
    oauth_meta["registration"] = effective_registration

    if effective_registration == OAUTH_REGISTRATION_PREREGISTERED:
        if effective_client_id is None:
            msg = (
                "--oauth-registration preregistered requires "
                "--oauth-client-id or an existing stored client_id"
            )
            raise ValueError(msg)
        oauth_meta["client_id"] = effective_client_id
        client_id_changed = (
            explicit_client_id is not None
            and explicit_client_id != existing_client_id
        )
        if client_id_changed and explicit_client_secret is None:
            oauth_meta.pop("client_secret", None)
        if client_id_changed and explicit_auth_method is None:
            oauth_meta.pop("token_endpoint_auth_method", None)
        if explicit_client_secret is not None:
            oauth_meta["client_secret"] = explicit_client_secret
            if (
                explicit_auth_method is None
                and oauth_meta.get("token_endpoint_auth_method") == "none"
            ):
                oauth_meta.pop("token_endpoint_auth_method", None)
        if explicit_auth_method is not None:
            oauth_meta["token_endpoint_auth_method"] = explicit_auth_method
            if (
                explicit_auth_method == "none"
                and explicit_client_secret is None
            ):
                oauth_meta.pop("client_secret", None)
    else:
        oauth_meta.pop("client_id", None)
        oauth_meta.pop("client_secret", None)
        oauth_meta.pop("token_endpoint_auth_method", None)
    scopes = [
        scope.strip()
        for scope in (oauth_scopes or [])
        if isinstance(scope, str) and scope.strip()
    ]
    if scopes:
        oauth_meta["scope"] = " ".join(scopes)
    if oauth_callback_port is not None:
        oauth_meta["callback_port"] = oauth_callback_port
    elif (
        effective_registration == OAUTH_REGISTRATION_PREREGISTERED
        and isinstance(oauth_meta.get("client_id"), str)
        and oauth_meta.get("client_id", "").strip()
        and _oauth_config_callback_port(oauth_meta) is None
    ):
        oauth_meta["callback_port"] = OAUTH_STATIC_CLIENT_CALLBACK_PORT
    callback_port = _oauth_config_callback_port(oauth_meta)
    if callback_port is not None:
        oauth_meta["callback_port"] = callback_port

    if (
        effective_registration == OAUTH_REGISTRATION_PREREGISTERED
        and isinstance(oauth_meta.get("client_id"), str)
        and oauth_meta.get("client_id", "").strip()
    ):
        if callback_port is None:
            msg = "OAuth callback port is required for static client login."
            raise RuntimeError(msg)
        redirect_uri = f"http://localhost:{callback_port}/callback"
        _oauth_client_info_from_config(
            oauth_config=oauth_meta,
            redirect_uris=[redirect_uri],
            client_name="FastMCP Client",
        )

    persisted_oauth_meta = dict(oauth_meta)
    persisted_headers = (
        None
        if shared_secret_ref
        else {"Authorization": "Bearer __OAUTH_ACCESS_TOKEN__"}
    )

    if dry_run:
        result = set_upstream_auth(
            server=server,
            env_updates=None,
            header_updates=persisted_headers,
            oauth=persisted_oauth_meta,
            clear_oauth=False,
            data_dir=resolved_data_dir,
            dry_run=True,
        )
        result["login"] = "oauth"
        return result

    reset_oauth_session = _oauth_login_requires_session_reset(
        existing_auth=existing_oauth_session,
        auth_config=oauth_meta,
    )
    try:
        cache_dir = oauth_cache_dir_path(resolved_data_dir, target_ref)
        had_oauth_cache = cache_dir.exists()
        token_storage = oauth_token_storage(resolved_data_dir, target_ref)
        preserve_cache = (
            _preserve_oauth_cache_dir(
                data_dir=resolved_data_dir,
                ref=target_ref,
                existed=had_oauth_cache,
            )
            if reset_oauth_session
            else contextlib.nullcontext()
        )
        with preserve_cache:
            if reset_oauth_session:
                asyncio.run(
                    clear_oauth_session(
                        token_storage=token_storage,
                        server_url=url,
                    )
                )
            else:
                try:
                    asyncio.run(
                        clear_oauth_client_registration(
                            token_storage=token_storage,
                            server_url=url,
                        )
                    )
                except Exception as exc:
                    _logger.debug(
                        "skipped oauth client registration reset for %s: %s",
                        server,
                        exc,
                    )
            access_token = asyncio.run(
                _oauth_login_access_token(
                    url=url,
                    headless=headless,
                    token_storage=token_storage,
                    oauth_config=oauth_meta,
                )
            )
            result = set_upstream_auth(
                server=server,
                env_updates=None,
                header_updates=(
                    None
                    if shared_secret_ref
                    else {"Authorization": f"Bearer {access_token}"}
                ),
                oauth=persisted_oauth_meta,
                clear_oauth=False,
                data_dir=resolved_data_dir,
                dry_run=False,
            )
    except Exception as exc:
        rewritten = _oauth_dependency_runtime_error(exc)
        if rewritten is not None:
            raise rewritten from exc
        raise
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


def _upstream_secret_ref(upstream: Any) -> str | None:
    """Return normalized secret ref for one upstream config."""
    raw_secret_ref = getattr(upstream, "secret_ref", None)
    if isinstance(raw_secret_ref, str) and raw_secret_ref:
        return raw_secret_ref.removesuffix(".json")

    raw_prefix = getattr(upstream, "prefix", None)
    if isinstance(raw_prefix, str) and raw_prefix:
        return raw_prefix
    return None


def _upstream_oauth_enabled(upstream: Any, data_dir: Path) -> bool:
    """Return whether one HTTP upstream uses interactive OAuth sessions."""
    if getattr(upstream, "transport", None) != "http":
        return False
    url = getattr(upstream, "url", None)
    raw_secret_ref = getattr(upstream, "secret_ref", None)
    has_explicit_secret_ref = isinstance(raw_secret_ref, str) and bool(
        raw_secret_ref
    )
    secret_ref = _upstream_secret_ref(upstream)
    if not isinstance(secret_ref, str) or not secret_ref:
        return False
    name = str(getattr(upstream, "prefix", secret_ref))
    try:
        secret = read_secret(data_dir, secret_ref)
    except FileNotFoundError as exc:
        if not has_explicit_secret_ref:
            return False
        msg = f"upstream {name!r} secret file {secret_ref!r} not found"
        raise ValueError(msg) from exc
    except Exception as exc:
        msg = f"upstream {name!r} has invalid secret file {secret_ref!r}: {exc}"
        raise ValueError(msg) from exc
    oauth = _effective_oauth_config_for_server(
        data_dir=data_dir,
        ref=secret_ref,
        server=name,
        server_url=url if isinstance(url, str) else None,
        oauth_config=(
            secret.get("oauth")
            if isinstance(secret.get("oauth"), dict)
            else None
        ),
    )
    return _uses_oauth_session(oauth)


async def _probe_one_oauth_upstream(
    upstream: Any,
    data_dir: Path,
) -> dict[str, Any]:
    """Probe one OAuth-enabled upstream with a forced refresh attempt."""
    name = str(getattr(upstream, "prefix", ""))
    url = getattr(upstream, "url", None)
    if not isinstance(url, str) or not url:
        return {
            "name": name,
            "ok": False,
            "error_code": "UPSTREAM_CONFIG_ERROR",
            "error": "oauth upstream has no HTTP url configured",
        }

    secret_ref = _upstream_secret_ref(upstream)
    if not isinstance(secret_ref, str) or not secret_ref:
        return {
            "name": name,
            "ok": False,
            "error_code": "UPSTREAM_CONFIG_ERROR",
            "error": "oauth upstream has no secret_ref configured",
        }

    forced_refresh = False
    try:
        token_storage = oauth_token_storage(data_dir, secret_ref)
        forced_refresh = await mark_oauth_access_token_stale(
            token_storage=token_storage,
            server_url=url,
        )
        tools = await discover_tools(
            upstream,
            data_dir=str(data_dir),
        )
    except Exception as exc:
        return {
            "name": name,
            "ok": False,
            "error_code": classify_upstream_exception(exc),
            "error": str(exc),
            "forced_refresh": forced_refresh,
        }
    return {
        "name": name,
        "ok": True,
        "tool_count": len(tools),
        "forced_refresh": forced_refresh,
    }


async def _probe_oauth_upstream_configs(
    *,
    upstreams: list[Any],
    data_dir: Path,
) -> list[dict[str, Any]]:
    """Probe OAuth-enabled upstreams concurrently with refresh checks."""
    return list(
        await asyncio.gather(
            *(
                _probe_one_oauth_upstream(upstream, data_dir)
                for upstream in upstreams
            )
        )
    )


def probe_oauth_upstreams(
    *,
    server: str | None = None,
    all_servers: bool = False,
    data_dir: Path | None = None,
) -> dict[str, Any]:
    """Probe OAuth upstream session health with a forced refresh preflight."""
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

    oauth_upstream_indexes: list[int] = []
    oauth_upstreams: list[Any] = []
    preflight_failures: dict[int, dict[str, Any]] = {}
    for idx, upstream in enumerate(active):
        try:
            oauth_enabled = _upstream_oauth_enabled(upstream, resolved_data_dir)
        except ValueError as exc:
            preflight_failures[idx] = {
                "name": str(getattr(upstream, "prefix", "")),
                "ok": False,
                "error_code": "UPSTREAM_CONFIG_ERROR",
                "error": str(exc),
                "forced_refresh": False,
            }
            continue
        if oauth_enabled:
            oauth_upstream_indexes.append(idx)
            oauth_upstreams.append(upstream)

    if server and preflight_failures:
        first_failure = next(iter(preflight_failures.values()))
        return {
            "results": [first_failure],
            "ok": False,
            "ok_count": 0,
            "total": 1,
        }
    if server and not oauth_upstreams:
        msg = f"upstream {server!r} is not OAuth-enabled"
        raise ValueError(msg)

    oauth_results = asyncio.run(
        _probe_oauth_upstream_configs(
            upstreams=oauth_upstreams,
            data_dir=resolved_data_dir,
        )
    )
    oauth_results_by_index = dict(
        zip(
            oauth_upstream_indexes,
            oauth_results,
            strict=True,
        )
    )
    results: list[dict[str, Any]] = []
    for idx, _upstream in enumerate(active):
        if idx in preflight_failures:
            results.append(preflight_failures[idx])
            continue
        oauth_row = oauth_results_by_index.get(idx)
        if oauth_row is not None:
            results.append(oauth_row)

    ok_count = sum(1 for item in results if item.get("ok"))
    return {
        "results": results,
        "ok": ok_count == len(results),
        "ok_count": ok_count,
        "total": len(results),
    }
