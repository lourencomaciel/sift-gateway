"""SQLite-backed upstream registry and config mirror helpers."""

from __future__ import annotations

from collections.abc import Iterator
import contextlib
import json
import os
from pathlib import Path
import tempfile
from typing import Any

from sift_gateway.config.mcp_servers import (
    _infer_transport,
    extract_mcp_servers,
)
from sift_gateway.config.shared import (
    ensure_gateway_config_path,
    gateway_config_path,
)
from sift_gateway.config.upstream_secrets import (
    _validate_prefix,
    write_secret,
)
from sift_gateway.constants import WORKSPACE_ID
from sift_gateway.db.backend import SqliteBackend
from sift_gateway.db.migrate import apply_migrations

_UPSTREAM_REGISTRY_COLUMNS = (
    "prefix",
    "transport",
    "command",
    "args_json",
    "url",
    "pagination_json",
    "auto_paginate_max_pages",
    "auto_paginate_max_records",
    "auto_paginate_timeout_seconds",
    "passthrough_allowed",
    "semantic_salt_env_keys_json",
    "semantic_salt_headers_json",
    "inherit_parent_env",
    "external_user_id",
    "secret_ref",
    "enabled",
    "source_kind",
    "source_ref",
)
_VALID_SOURCE_KINDS = frozenset({"manual", "init_sync", "snippet_add"})


def _migrations_dir() -> Path:
    """Return the SQLite migrations directory."""
    return Path(__file__).resolve().parents[1] / "db" / "migrations_sqlite"


def _db_path(data_dir: Path) -> Path:
    """Return the SQLite DB path for a data dir."""
    return data_dir / "state" / "gateway.db"


def _secret_file_path(data_dir: Path, secret_ref: str) -> Path:
    """Return the filesystem path for a secret reference."""
    prefix = secret_ref.removesuffix(".json")
    _validate_prefix(prefix)
    return data_dir / "state" / "upstream_secrets" / f"{prefix}.json"


def _snapshot_secret_files(
    *,
    data_dir: Path,
    pending_secret_writes: list[
        tuple[
            str,
            str,
            dict[str, str] | None,
            dict[str, str] | None,
        ]
    ],
) -> dict[str, bytes | None]:
    """Capture current secret-file bytes for rollback.

    Returns a mapping of ``secret_ref -> prior_bytes`` where ``None`` means the
    file did not exist before this operation.
    """
    snapshots: dict[str, bytes | None] = {}
    for secret_ref, _transport, _env, _headers in pending_secret_writes:
        if secret_ref in snapshots:
            continue
        path = _secret_file_path(data_dir, secret_ref)
        if path.is_file():
            snapshots[secret_ref] = path.read_bytes()
        else:
            snapshots[secret_ref] = None
    return snapshots


def _restore_secret_snapshots(
    *,
    data_dir: Path,
    snapshots: dict[str, bytes | None],
) -> None:
    """Restore secret files to their pre-write state."""
    for secret_ref, prior_bytes in snapshots.items():
        path = _secret_file_path(data_dir, secret_ref)
        if prior_bytes is None:
            with contextlib.suppress(FileNotFoundError):
                path.unlink()
            continue

        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_path_raw = tempfile.mkstemp(
            dir=str(path.parent),
            suffix=".tmp",
        )
        tmp_path = Path(tmp_path_raw)
        try:
            os.write(fd, prior_bytes)
            os.fchmod(fd, 0o600)
            os.close(fd)
            fd = -1
            tmp_path.replace(path)
        except BaseException:
            if fd >= 0:
                os.close(fd)
            with contextlib.suppress(OSError):
                tmp_path.unlink(missing_ok=True)
            raise


def _write_secrets_and_commit(
    *,
    conn: Any,
    data_dir: Path,
    pending_secret_writes: list[
        tuple[
            str,
            str,
            dict[str, str] | None,
            dict[str, str] | None,
        ]
    ],
) -> None:
    """Write pending secret files and commit DB transaction atomically."""
    snapshots: dict[str, bytes | None] | None = None
    try:
        snapshots = _snapshot_secret_files(
            data_dir=data_dir,
            pending_secret_writes=pending_secret_writes,
        )
        for secret_ref, transport, env, headers in pending_secret_writes:
            write_secret(
                data_dir,
                secret_ref,
                transport=transport,
                env=env,
                headers=headers,
            )
        conn.commit()
    except Exception:
        conn.rollback()
        if snapshots is not None:
            # Best effort: preserve original error if rollback restoration fails.
            with contextlib.suppress(Exception):
                _restore_secret_snapshots(
                    data_dir=data_dir,
                    snapshots=snapshots,
                )
        raise


@contextlib.contextmanager
def _connect_migrated(data_dir: Path) -> Iterator[Any]:
    """Yield migrated SQLite connection for registry operations."""
    db_path = _db_path(data_dir)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    backend = SqliteBackend(db_path=db_path)
    try:
        with backend.connection() as conn:
            apply_migrations(conn, _migrations_dir())
            yield conn
    finally:
        backend.close()


def _write_json(path: Path, data: dict[str, Any]) -> None:
    """Atomically write a JSON object with stable formatting."""
    content = json.dumps(data, indent=2, ensure_ascii=False) + "\n"
    fd, tmp_path_raw = tempfile.mkstemp(
        dir=str(path.parent),
        suffix=".tmp",
    )
    tmp_path = Path(tmp_path_raw)
    try:
        os.write(fd, content.encode("utf-8"))
        os.close(fd)
        fd = -1
        tmp_path.replace(path)
    except BaseException:
        if fd >= 0:
            os.close(fd)
        with contextlib.suppress(OSError):
            tmp_path.unlink(missing_ok=True)
        raise


def _load_gateway_config_dict(config_path: Path) -> dict[str, Any]:
    """Load existing gateway config file as a dict."""
    if not config_path.exists():
        return {}
    raw = json.loads(config_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        return {}
    return raw


def _coerce_str_list(value: Any) -> list[str]:
    """Coerce unknown JSON-like list values to list[str]."""
    if not isinstance(value, list):
        return []
    return [str(item) for item in value]


def _decode_json_value(raw: Any, *, default: Any) -> Any:
    """Decode JSON text from DB columns with a fallback default."""
    if not isinstance(raw, str):
        return default
    try:
        decoded = json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        return default
    return decoded


def _registry_rows_to_records(
    rows: list[tuple[Any, ...]],
) -> list[dict[str, Any]]:
    """Convert SQL row tuples to normalized registry record dicts."""
    records: list[dict[str, Any]] = []
    for row in rows:
        (
            prefix,
            transport,
            command,
            args_json,
            url,
            pagination_json,
            auto_paginate_max_pages,
            auto_paginate_max_records,
            auto_paginate_timeout_seconds,
            passthrough_allowed,
            semantic_salt_env_keys_json,
            semantic_salt_headers_json,
            inherit_parent_env,
            external_user_id,
            secret_ref,
            enabled,
            source_kind,
            source_ref,
        ) = row
        args = _decode_json_value(args_json, default=[])
        pagination = _decode_json_value(pagination_json, default=None)
        semantic_salt_env_keys = _decode_json_value(
            semantic_salt_env_keys_json, default=[]
        )
        semantic_salt_headers = _decode_json_value(
            semantic_salt_headers_json, default=[]
        )
        records.append(
            {
                "prefix": str(prefix),
                "transport": str(transport),
                "command": command if isinstance(command, str) else None,
                "args": _coerce_str_list(args),
                "url": url if isinstance(url, str) else None,
                "pagination": pagination
                if isinstance(pagination, dict)
                else None,
                "auto_paginate_max_pages": auto_paginate_max_pages,
                "auto_paginate_max_records": auto_paginate_max_records,
                "auto_paginate_timeout_seconds": auto_paginate_timeout_seconds,
                "passthrough_allowed": bool(passthrough_allowed),
                "semantic_salt_env_keys": _coerce_str_list(
                    semantic_salt_env_keys
                ),
                "semantic_salt_headers": _coerce_str_list(
                    semantic_salt_headers
                ),
                "inherit_parent_env": bool(inherit_parent_env),
                "external_user_id": external_user_id
                if isinstance(external_user_id, str)
                else None,
                "secret_ref": secret_ref
                if isinstance(secret_ref, str)
                else None,
                "enabled": bool(enabled),
                "source_kind": source_kind
                if isinstance(source_kind, str)
                else None,
                "source_ref": source_ref
                if isinstance(source_ref, str)
                else None,
            }
        )
    return records


def _record_to_upstream_dict(record: dict[str, Any]) -> dict[str, Any]:
    """Convert one registry record to UpstreamConfig-compatible dict."""
    config: dict[str, Any] = {
        "prefix": record["prefix"],
        "transport": record["transport"],
    }
    if record["transport"] == "stdio":
        config["command"] = record["command"]
        config["args"] = list(record["args"])
    else:
        config["url"] = record["url"]

    if record["pagination"] is not None:
        config["pagination"] = record["pagination"]
    if record["auto_paginate_max_pages"] is not None:
        config["auto_paginate_max_pages"] = record["auto_paginate_max_pages"]
    if record["auto_paginate_max_records"] is not None:
        config["auto_paginate_max_records"] = record[
            "auto_paginate_max_records"
        ]
    if record["auto_paginate_timeout_seconds"] is not None:
        config["auto_paginate_timeout_seconds"] = record[
            "auto_paginate_timeout_seconds"
        ]
    if not record["passthrough_allowed"]:
        config["passthrough_allowed"] = False
    if record["semantic_salt_env_keys"]:
        config["semantic_salt_env_keys"] = list(
            record["semantic_salt_env_keys"]
        )
    if record["semantic_salt_headers"]:
        config["semantic_salt_headers"] = list(record["semantic_salt_headers"])
    if record["inherit_parent_env"]:
        config["inherit_parent_env"] = True
    if isinstance(record["external_user_id"], str):
        config["external_user_id"] = record["external_user_id"]
    if isinstance(record["secret_ref"], str):
        config["secret_ref"] = record["secret_ref"]
    return config


def _record_to_mcp_server_entry(record: dict[str, Any]) -> dict[str, Any]:
    """Convert one registry record to mcpServers entry shape."""
    entry: dict[str, Any] = {}
    if record["transport"] == "stdio":
        entry["command"] = record["command"]
        entry["args"] = list(record["args"])
    else:
        entry["url"] = record["url"]

    gateway_ext: dict[str, Any] = {}
    if record["pagination"] is not None:
        gateway_ext["pagination"] = record["pagination"]
    if record["auto_paginate_max_pages"] is not None:
        gateway_ext["auto_paginate_max_pages"] = record[
            "auto_paginate_max_pages"
        ]
    if record["auto_paginate_max_records"] is not None:
        gateway_ext["auto_paginate_max_records"] = record[
            "auto_paginate_max_records"
        ]
    if record["auto_paginate_timeout_seconds"] is not None:
        gateway_ext["auto_paginate_timeout_seconds"] = record[
            "auto_paginate_timeout_seconds"
        ]
    if not record["passthrough_allowed"]:
        gateway_ext["passthrough_allowed"] = False
    if record["semantic_salt_env_keys"]:
        gateway_ext["semantic_salt_env_keys"] = list(
            record["semantic_salt_env_keys"]
        )
    if record["semantic_salt_headers"]:
        gateway_ext["semantic_salt_headers"] = list(
            record["semantic_salt_headers"]
        )
    if record["inherit_parent_env"]:
        gateway_ext["inherit_parent_env"] = True
    if isinstance(record["external_user_id"], str):
        gateway_ext["external_user_id"] = record["external_user_id"]
    if isinstance(record["secret_ref"], str):
        gateway_ext["secret_ref"] = record["secret_ref"]
    if not record["enabled"]:
        gateway_ext["enabled"] = False
    if gateway_ext:
        entry["_gateway"] = gateway_ext
    return entry


def _gateway_bool_field(
    *,
    prefix: str,
    gateway_ext: dict[str, Any],
    field: str,
    default: bool,
) -> bool:
    """Read an optional boolean field from ``_gateway``."""
    if field not in gateway_ext:
        return default
    raw = gateway_ext[field]
    if not isinstance(raw, bool):
        msg = f"server '{prefix}' _gateway.{field} must be a boolean"
        raise ValueError(msg)
    return raw


def _gateway_pagination_field(
    *,
    prefix: str,
    gateway_ext: dict[str, Any],
) -> dict[str, Any] | None:
    """Read optional ``_gateway.pagination`` with shape validation."""
    if "pagination" not in gateway_ext:
        return None
    raw = gateway_ext["pagination"]
    if raw is None:
        return None
    if not isinstance(raw, dict):
        msg = f"server '{prefix}' _gateway.pagination must be a JSON object"
        raise ValueError(msg)
    return raw


def _gateway_optional_int_field(
    *,
    prefix: str,
    gateway_ext: dict[str, Any],
    field: str,
    minimum: int,
) -> int | None:
    """Read optional integer from ``_gateway`` with range validation."""
    if field not in gateway_ext:
        return None
    raw = gateway_ext[field]
    if raw is None:
        return None
    if isinstance(raw, bool) or not isinstance(raw, int):
        msg = f"server '{prefix}' _gateway.{field} must be an integer"
        raise ValueError(msg)
    if raw < minimum:
        msg = f"server '{prefix}' _gateway.{field} must be >= {minimum}"
        raise ValueError(msg)
    return raw


def _gateway_optional_float_field(
    *,
    prefix: str,
    gateway_ext: dict[str, Any],
    field: str,
) -> float | None:
    """Read optional positive float from ``_gateway``."""
    if field not in gateway_ext:
        return None
    raw = gateway_ext[field]
    if raw is None:
        return None
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        msg = f"server '{prefix}' _gateway.{field} must be a number"
        raise ValueError(msg)
    value = float(raw)
    if value <= 0:
        msg = f"server '{prefix}' _gateway.{field} must be > 0"
        raise ValueError(msg)
    return value


def _gateway_optional_string_list_field(
    *,
    prefix: str,
    gateway_ext: dict[str, Any],
    field: str,
) -> list[str]:
    """Read optional list[str]-like field from ``_gateway``.

    Values must be JSON arrays when provided.
    """
    if field not in gateway_ext:
        return []
    raw = gateway_ext[field]
    if not isinstance(raw, list):
        msg = f"server '{prefix}' _gateway.{field} must be a JSON array"
        raise ValueError(msg)
    return [str(item) for item in raw]


def _entry_to_registry_payload(
    *,
    data_dir: Path,
    prefix: str,
    entry: dict[str, Any],
    source_kind: str,
    source_ref: str | None,
    pending_secret_writes: list[
        tuple[
            str,
            str,
            dict[str, str] | None,
            dict[str, str] | None,
        ]
    ]
    | None = None,
) -> dict[str, Any]:
    """Convert one mcpServers entry to registry row payload."""
    _validate_prefix(prefix)
    if source_kind not in _VALID_SOURCE_KINDS:
        msg = f"invalid source_kind: {source_kind!r}"
        raise ValueError(msg)
    transport = _infer_transport(prefix, entry)
    gateway_ext = entry.get("_gateway", {})
    if not isinstance(gateway_ext, dict):
        msg = f"server '{prefix}' _gateway must be a JSON object"
        raise ValueError(msg)
    pagination = _gateway_pagination_field(
        prefix=prefix,
        gateway_ext=gateway_ext,
    )
    auto_paginate_max_pages = _gateway_optional_int_field(
        prefix=prefix,
        gateway_ext=gateway_ext,
        field="auto_paginate_max_pages",
        minimum=0,
    )
    auto_paginate_max_records = _gateway_optional_int_field(
        prefix=prefix,
        gateway_ext=gateway_ext,
        field="auto_paginate_max_records",
        minimum=0,
    )
    auto_paginate_timeout_seconds = _gateway_optional_float_field(
        prefix=prefix,
        gateway_ext=gateway_ext,
        field="auto_paginate_timeout_seconds",
    )

    if transport == "stdio":
        command = entry.get("command")
        if not isinstance(command, str) or not command:
            msg = f"server '{prefix}' command must be a non-empty string"
            raise ValueError(msg)
        raw_args = entry.get("args")
        if raw_args is None:
            args = []
        elif not isinstance(raw_args, list):
            msg = f"server '{prefix}' args must be a JSON array"
            raise ValueError(msg)
        else:
            args = [str(item) for item in raw_args]
        url = None
    else:
        url = entry.get("url")
        if not isinstance(url, str) or not url:
            msg = f"server '{prefix}' url must be a non-empty string"
            raise ValueError(msg)
        command = None
        args = []

    raw_env = entry.get("env")
    raw_headers = entry.get("headers")
    if raw_env is not None and not isinstance(raw_env, dict):
        msg = f"server '{prefix}' env must be a JSON object"
        raise ValueError(msg)
    if raw_headers is not None and not isinstance(raw_headers, dict):
        msg = f"server '{prefix}' headers must be a JSON object"
        raise ValueError(msg)
    env = (
        {str(key): str(value) for key, value in raw_env.items()}
        if isinstance(raw_env, dict)
        else None
    )
    headers = (
        {str(key): str(value) for key, value in raw_headers.items()}
        if isinstance(raw_headers, dict)
        else None
    )

    has_secret_ref_field = "secret_ref" in gateway_ext
    secret_ref_raw = gateway_ext.get("secret_ref")
    if (
        has_secret_ref_field
        and secret_ref_raw is not None
        and not isinstance(secret_ref_raw, str)
    ):
        msg = (
            f"server '{prefix}' _gateway.secret_ref must be a non-empty string"
        )
        raise ValueError(msg)
    explicit_secret_ref = isinstance(secret_ref_raw, str)
    secret_ref: str | None
    if explicit_secret_ref:
        secret_ref = secret_ref_raw.removesuffix(".json")
        if not secret_ref:
            msg = (
                f"server '{prefix}' _gateway.secret_ref must be a "
                "non-empty string"
            )
            raise ValueError(msg)
    else:
        secret_ref = None

    if explicit_secret_ref and (env or headers):
        msg = (
            "Cannot specify both inline env/headers and "
            "secret_ref for upstream. Use one or the other."
        )
        raise ValueError(msg)

    if not secret_ref:
        if transport == "stdio" and env:
            secret_ref = prefix
        if transport == "http" and headers:
            secret_ref = prefix
    if secret_ref:
        _validate_prefix(secret_ref)
        secret_env = env if transport == "stdio" and env else None
        secret_headers = headers if transport == "http" and headers else None
        if (secret_env or secret_headers) and pending_secret_writes is not None:
            pending_secret_writes.append(
                (
                    secret_ref,
                    transport,
                    secret_env,
                    secret_headers,
                )
            )
        elif secret_env or secret_headers:
            write_secret(
                data_dir,
                secret_ref,
                transport=transport,
                env=secret_env,
                headers=secret_headers,
            )

    return {
        "workspace_id": WORKSPACE_ID,
        "prefix": prefix,
        "transport": transport,
        "command": command,
        "args_json": json.dumps(args, ensure_ascii=False),
        "url": url,
        "pagination_json": (
            json.dumps(pagination, ensure_ascii=False)
            if isinstance(pagination, dict)
            else None
        ),
        "auto_paginate_max_pages": auto_paginate_max_pages,
        "auto_paginate_max_records": auto_paginate_max_records,
        "auto_paginate_timeout_seconds": auto_paginate_timeout_seconds,
        "passthrough_allowed": int(
            _gateway_bool_field(
                prefix=prefix,
                gateway_ext=gateway_ext,
                field="passthrough_allowed",
                default=True,
            )
        ),
        "semantic_salt_env_keys_json": json.dumps(
            _gateway_optional_string_list_field(
                prefix=prefix,
                gateway_ext=gateway_ext,
                field="semantic_salt_env_keys",
            ),
            ensure_ascii=False,
        ),
        "semantic_salt_headers_json": json.dumps(
            _gateway_optional_string_list_field(
                prefix=prefix,
                gateway_ext=gateway_ext,
                field="semantic_salt_headers",
            ),
            ensure_ascii=False,
        ),
        "inherit_parent_env": int(
            _gateway_bool_field(
                prefix=prefix,
                gateway_ext=gateway_ext,
                field="inherit_parent_env",
                default=False,
            )
        ),
        "external_user_id": (
            gateway_ext.get("external_user_id")
            if isinstance(gateway_ext.get("external_user_id"), str)
            else None
        ),
        "secret_ref": secret_ref,
        "enabled": int(
            _gateway_bool_field(
                prefix=prefix,
                gateway_ext=gateway_ext,
                field="enabled",
                default=True,
            )
        ),
        "source_kind": source_kind,
        "source_ref": source_ref,
    }


def _upsert_payload(conn: Any, payload: dict[str, Any]) -> None:
    """Upsert one registry payload row."""
    conn.execute(
        """
        INSERT INTO upstream_registry (
            workspace_id,
            prefix,
            transport,
            command,
            args_json,
            url,
            pagination_json,
            auto_paginate_max_pages,
            auto_paginate_max_records,
            auto_paginate_timeout_seconds,
            passthrough_allowed,
            semantic_salt_env_keys_json,
            semantic_salt_headers_json,
            inherit_parent_env,
            external_user_id,
            secret_ref,
            enabled,
            source_kind,
            source_ref
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (workspace_id, prefix)
        DO UPDATE SET
            transport = EXCLUDED.transport,
            command = EXCLUDED.command,
            args_json = EXCLUDED.args_json,
            url = EXCLUDED.url,
            pagination_json = EXCLUDED.pagination_json,
            auto_paginate_max_pages = EXCLUDED.auto_paginate_max_pages,
            auto_paginate_max_records = EXCLUDED.auto_paginate_max_records,
            auto_paginate_timeout_seconds =
                EXCLUDED.auto_paginate_timeout_seconds,
            passthrough_allowed = EXCLUDED.passthrough_allowed,
            semantic_salt_env_keys_json =
                EXCLUDED.semantic_salt_env_keys_json,
            semantic_salt_headers_json = EXCLUDED.semantic_salt_headers_json,
            inherit_parent_env = EXCLUDED.inherit_parent_env,
            external_user_id = EXCLUDED.external_user_id,
            secret_ref = EXCLUDED.secret_ref,
            enabled = EXCLUDED.enabled,
            source_kind = EXCLUDED.source_kind,
            source_ref = EXCLUDED.source_ref,
            updated_at = datetime('now')
        """,
        tuple(
            payload[column]
            for column in ("workspace_id", *_UPSTREAM_REGISTRY_COLUMNS)
        ),
    )


def load_registry_upstream_records(
    data_dir: Path,
    *,
    include_disabled: bool = True,
) -> list[dict[str, Any]]:
    """Load registry rows as normalized record dicts."""
    db_path = _db_path(data_dir)
    if not db_path.exists():
        return []

    with _connect_migrated(data_dir) as conn:
        sql = (
            "SELECT "
            + ", ".join(_UPSTREAM_REGISTRY_COLUMNS)
            + " FROM upstream_registry WHERE workspace_id = %s"
        )
        params: tuple[Any, ...] = (WORKSPACE_ID,)
        if not include_disabled:
            sql += " AND enabled = 1"
        # Preserve insertion order so index-based env overrides
        # (e.g. SIFT_GATEWAY_UPSTREAMS__0__...) remain stable.
        sql += " ORDER BY rowid"
        rows = conn.execute(sql, params).fetchall()
    return _registry_rows_to_records(rows)


def load_registry_upstream_dicts(
    data_dir: Path,
    *,
    enabled_only: bool,
) -> list[dict[str, Any]]:
    """Load registry rows as UpstreamConfig-compatible dicts."""
    records = load_registry_upstream_records(
        data_dir,
        include_disabled=not enabled_only,
    )
    return [_record_to_upstream_dict(record) for record in records]


def load_registry_mcp_servers(data_dir: Path) -> dict[str, dict[str, Any]]:
    """Load registry rows as mcpServers config map (includes disabled)."""
    records = load_registry_upstream_records(data_dir, include_disabled=True)
    servers: dict[str, dict[str, Any]] = {}
    for record in records:
        servers[record["prefix"]] = _record_to_mcp_server_entry(record)
    return servers


def upsert_registry_from_mcp_servers(
    *,
    data_dir: Path,
    servers: dict[str, dict[str, Any]],
    merge_missing: bool,
    source_kind: str,
    source_ref: str | None = None,
) -> int:
    """Upsert registry rows from mcpServers map."""
    if not servers:
        return 0

    pending_secret_writes: list[
        tuple[
            str,
            str,
            dict[str, str] | None,
            dict[str, str] | None,
        ]
    ] = []
    with _connect_migrated(data_dir) as conn:
        existing_prefixes: set[str] = set()
        if merge_missing:
            rows = conn.execute(
                (
                    "SELECT prefix FROM upstream_registry "
                    "WHERE workspace_id = %s"
                ),
                (WORKSPACE_ID,),
            ).fetchall()
            existing_prefixes = {
                str(row[0]) for row in rows if row and isinstance(row[0], str)
            }

        changed = 0
        for prefix, entry in servers.items():
            if merge_missing and prefix in existing_prefixes:
                continue
            if not isinstance(entry, dict):
                msg = f"server '{prefix}' config must be a JSON object"
                raise ValueError(msg)
            payload = _entry_to_registry_payload(
                data_dir=data_dir,
                prefix=prefix,
                entry=entry,
                source_kind=source_kind,
                source_ref=source_ref,
                pending_secret_writes=pending_secret_writes,
            )
            _upsert_payload(conn, payload)
            changed += 1

        _write_secrets_and_commit(
            conn=conn,
            data_dir=data_dir,
            pending_secret_writes=pending_secret_writes,
        )
    return changed


def replace_registry_from_mcp_servers(
    *,
    data_dir: Path,
    servers: dict[str, dict[str, Any]],
    source_kind: str,
    source_ref: str | None = None,
) -> int:
    """Replace full registry snapshot from mcpServers map."""
    pending_secret_writes: list[
        tuple[
            str,
            str,
            dict[str, str] | None,
            dict[str, str] | None,
        ]
    ] = []
    with _connect_migrated(data_dir) as conn:
        conn.execute(
            "DELETE FROM upstream_registry WHERE workspace_id = %s",
            (WORKSPACE_ID,),
        )
        changed = 0
        for prefix, entry in servers.items():
            if not isinstance(entry, dict):
                msg = f"server '{prefix}' config must be a JSON object"
                raise ValueError(msg)
            payload = _entry_to_registry_payload(
                data_dir=data_dir,
                prefix=prefix,
                entry=entry,
                source_kind=source_kind,
                source_ref=source_ref,
                pending_secret_writes=pending_secret_writes,
            )
            _upsert_payload(conn, payload)
            changed += 1

        _write_secrets_and_commit(
            conn=conn,
            data_dir=data_dir,
            pending_secret_writes=pending_secret_writes,
        )
    return changed


def _load_config_mcp_servers(data_dir: Path) -> dict[str, dict[str, Any]]:
    """Load mcpServers-compatible map from state/config.json."""
    config_path = gateway_config_path(data_dir)
    raw_config = _load_gateway_config_dict(config_path)
    servers = extract_mcp_servers(raw_config)
    normalized: dict[str, dict[str, Any]] = {}
    for prefix, entry in servers.items():
        if not isinstance(entry, dict):
            msg = f"server '{prefix}' config must be a JSON object"
            raise ValueError(msg)
        normalized[str(prefix)] = entry
    return normalized


def bootstrap_registry_from_config(data_dir: Path) -> int:
    """Bootstrap registry from config when the registry is empty."""
    existing = load_registry_upstream_records(data_dir, include_disabled=True)
    if existing:
        return 0
    servers = _load_config_mcp_servers(data_dir)
    if not servers:
        return 0
    changed = upsert_registry_from_mcp_servers(
        data_dir=data_dir,
        servers=servers,
        merge_missing=False,
        source_kind="init_sync",
    )
    if changed > 0:
        mirror_registry_to_config(data_dir)
    return changed


def merge_missing_registry_from_config(data_dir: Path) -> int:
    """Merge config-defined servers that are missing in the registry."""
    existing = load_registry_upstream_records(data_dir, include_disabled=True)
    try:
        servers = _load_config_mcp_servers(data_dir)
    except ValueError:
        # If canonical rows already exist, avoid blocking runtime on
        # compatibility-mirror drift and keep the registry authoritative.
        if existing:
            return 0
        raise
    if not servers:
        return 0
    try:
        changed = upsert_registry_from_mcp_servers(
            data_dir=data_dir,
            servers=servers,
            merge_missing=True,
            source_kind="init_sync",
        )
    except ValueError:
        # If canonical rows already exist, avoid blocking runtime on
        # compatibility-mirror validation drift and keep registry-first reads.
        if existing:
            return 0
        raise
    if changed > 0:
        mirror_registry_to_config(data_dir)
    return changed


def mirror_registry_to_config(data_dir: Path) -> Path:
    """Mirror registry snapshot into ``state/config.json`` mcpServers."""
    config_path = gateway_config_path(data_dir)
    raw_config = _load_gateway_config_dict(config_path)
    raw_config["mcpServers"] = load_registry_mcp_servers(data_dir)
    raw_config.pop("upstreams", None)
    ensure_gateway_config_path(data_dir)
    _write_json(config_path, raw_config)
    return config_path


def get_registry_upstream_record(
    *,
    data_dir: Path,
    prefix: str,
) -> dict[str, Any] | None:
    """Load one registry row by prefix."""
    db_path = _db_path(data_dir)
    if not db_path.exists():
        return None

    with _connect_migrated(data_dir) as conn:
        rows = conn.execute(
            (
                "SELECT "
                + ", ".join(_UPSTREAM_REGISTRY_COLUMNS)
                + " FROM upstream_registry "
                + "WHERE workspace_id = %s AND prefix = %s"
            ),
            (WORKSPACE_ID, prefix),
        ).fetchall()
    if not rows:
        return None
    return _registry_rows_to_records(rows)[0]


def remove_registry_upstream(
    *,
    data_dir: Path,
    prefix: str,
) -> bool:
    """Remove one registry row by prefix."""
    db_path = _db_path(data_dir)
    if not db_path.exists():
        return False
    with _connect_migrated(data_dir) as conn:
        cursor = conn.execute(
            (
                "DELETE FROM upstream_registry "
                "WHERE workspace_id = %s AND prefix = %s"
            ),
            (WORKSPACE_ID, prefix),
        )
        conn.commit()
    return cursor.rowcount > 0


def set_registry_upstream_enabled(
    *,
    data_dir: Path,
    prefix: str,
    enabled: bool,
) -> bool:
    """Set ``enabled`` state for one registry row."""
    db_path = _db_path(data_dir)
    if not db_path.exists():
        return False
    with _connect_migrated(data_dir) as conn:
        cursor = conn.execute(
            (
                "UPDATE upstream_registry "
                "SET enabled = %s, updated_at = datetime('now') "
                "WHERE workspace_id = %s AND prefix = %s"
            ),
            (int(enabled), WORKSPACE_ID, prefix),
        )
        conn.commit()
    return cursor.rowcount > 0


def set_registry_upstream_secret_ref(
    *,
    data_dir: Path,
    prefix: str,
    secret_ref: str,
) -> bool:
    """Set ``secret_ref`` for one registry row."""
    _validate_prefix(secret_ref)
    db_path = _db_path(data_dir)
    if not db_path.exists():
        return False
    with _connect_migrated(data_dir) as conn:
        cursor = conn.execute(
            (
                "UPDATE upstream_registry "
                "SET secret_ref = %s, updated_at = datetime('now') "
                "WHERE workspace_id = %s AND prefix = %s"
            ),
            (secret_ref, WORKSPACE_ID, prefix),
        )
        conn.commit()
    return cursor.rowcount > 0
