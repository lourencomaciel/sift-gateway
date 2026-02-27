"""Shape conversion and field validation for upstream registry records."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from sift_gateway.config.mcp_servers import infer_transport
from sift_gateway.config.upstream_secrets import (
    validate_prefix,
    write_secret,
)
from sift_gateway.constants import WORKSPACE_ID

_VALID_SOURCE_KINDS = frozenset({"manual", "init_sync", "snippet_add"})


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


def registry_rows_to_records(
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
                "auto_paginate_max_records": (auto_paginate_max_records),
                "auto_paginate_timeout_seconds": (
                    auto_paginate_timeout_seconds
                ),
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


def _extract_gateway_fields(
    record: dict[str, Any],
) -> dict[str, Any]:
    """Extract gateway extension fields from a registry record."""
    fields: dict[str, Any] = {}
    if record["pagination"] is not None:
        fields["pagination"] = record["pagination"]
    if record["auto_paginate_max_pages"] is not None:
        fields["auto_paginate_max_pages"] = record["auto_paginate_max_pages"]
    if record["auto_paginate_max_records"] is not None:
        fields["auto_paginate_max_records"] = record[
            "auto_paginate_max_records"
        ]
    if record["auto_paginate_timeout_seconds"] is not None:
        fields["auto_paginate_timeout_seconds"] = record[
            "auto_paginate_timeout_seconds"
        ]
    if not record["passthrough_allowed"]:
        fields["passthrough_allowed"] = False
    if record["semantic_salt_env_keys"]:
        fields["semantic_salt_env_keys"] = list(
            record["semantic_salt_env_keys"]
        )
    if record["semantic_salt_headers"]:
        fields["semantic_salt_headers"] = list(record["semantic_salt_headers"])
    if record["inherit_parent_env"]:
        fields["inherit_parent_env"] = True
    if isinstance(record["external_user_id"], str):
        fields["external_user_id"] = record["external_user_id"]
    if isinstance(record["secret_ref"], str):
        fields["secret_ref"] = record["secret_ref"]
    return fields


def record_to_upstream_dict(
    record: dict[str, Any],
) -> dict[str, Any]:
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

    config.update(_extract_gateway_fields(record))
    return config


def record_to_mcp_server_entry(
    record: dict[str, Any],
) -> dict[str, Any]:
    """Convert one registry record to mcpServers entry shape."""
    entry: dict[str, Any] = {}
    if record["transport"] == "stdio":
        entry["command"] = record["command"]
        entry["args"] = list(record["args"])
    else:
        entry["url"] = record["url"]

    gateway_ext = _extract_gateway_fields(record)
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
    from sift_gateway.config.settings import PaginationConfig

    if "pagination" not in gateway_ext:
        return None
    raw = gateway_ext["pagination"]
    if raw is None:
        return None
    if not isinstance(raw, dict):
        msg = f"server '{prefix}' _gateway.pagination must be a JSON object"
        raise ValueError(msg)
    try:
        PaginationConfig(**raw)
    except Exception as exc:
        msg = f"server '{prefix}' _gateway.pagination is invalid: {exc}"
        raise ValueError(msg) from exc
    return raw


def _gateway_optional_int_field(
    *,
    prefix: str,
    gateway_ext: dict[str, Any],
    field: str,
    minimum: int,
) -> int | None:
    """Read optional integer from ``_gateway`` with range check."""
    if field not in gateway_ext:
        return None
    raw_value = gateway_ext[field]
    if raw_value is None:
        return None
    if isinstance(raw_value, bool) or not isinstance(raw_value, int):
        msg = f"server '{prefix}' _gateway.{field} must be an integer"
        raise ValueError(msg)
    value = int(raw_value)
    if value < minimum:
        msg = f"server '{prefix}' _gateway.{field} must be >= {minimum}"
        raise ValueError(msg)
    return value


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
    """Read optional list[str]-like field from ``_gateway``."""
    if field not in gateway_ext:
        return []
    raw = gateway_ext[field]
    if not isinstance(raw, list):
        msg = f"server '{prefix}' _gateway.{field} must be a JSON array"
        raise ValueError(msg)
    return [str(item) for item in raw]


def _gateway_optional_string_field(
    *,
    prefix: str,
    gateway_ext: dict[str, Any],
    field: str,
) -> str | None:
    """Read optional string field from ``_gateway``."""
    if field not in gateway_ext:
        return None
    raw = gateway_ext[field]
    if raw is None:
        return None
    if not isinstance(raw, str):
        msg = f"server '{prefix}' _gateway.{field} must be a string"
        raise ValueError(msg)
    return raw


def entry_to_registry_payload(
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
    validate_prefix(prefix)
    if source_kind not in _VALID_SOURCE_KINDS:
        msg = f"invalid source_kind: {source_kind!r}"
        raise ValueError(msg)
    transport = infer_transport(prefix, entry)
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
    secret_ref_text = (
        secret_ref_raw if isinstance(secret_ref_raw, str) else None
    )
    secret_ref: str | None
    if isinstance(secret_ref_text, str):
        secret_ref = secret_ref_text.removesuffix(".json")
        if not secret_ref:
            msg = (
                f"server '{prefix}' _gateway.secret_ref must be a "
                "non-empty string"
            )
            raise ValueError(msg)
    else:
        secret_ref = None

    if isinstance(secret_ref_text, str) and (env or headers):
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
        validate_prefix(secret_ref)
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
        "auto_paginate_timeout_seconds": (auto_paginate_timeout_seconds),
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
        "external_user_id": _gateway_optional_string_field(
            prefix=prefix,
            gateway_ext=gateway_ext,
            field="external_user_id",
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
