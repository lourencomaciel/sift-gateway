"""Gateway configuration models — loaded from env / config.json / defaults.

Spec references: §2, §3, §16, §17, §18, Addendum A.3, Addendum D.3.
"""

from __future__ import annotations

import json
import os
from enum import Enum
from pathlib import Path
from typing import Literal, Mapping

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from mcp_artifact_gateway.constants import CONFIG_FILENAME, DEFAULT_DATA_DIR, STATE_SUBDIR


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------
class EnvelopeJsonbMode(str, Enum):
    full = "full"
    minimal_for_large = "minimal_for_large"
    none = "none"


class CanonicalEncoding(str, Enum):
    zstd = "zstd"
    gzip = "gzip"
    none = "none"


class MappingMode(str, Enum):
    async_ = "async"
    hybrid = "hybrid"
    sync = "sync"


class WhereCanonicalizationMode(str, Enum):
    raw_string = "raw_string"
    canonical_ast = "canonical_ast"


# ---------------------------------------------------------------------------
# Upstream config (§4)
# ---------------------------------------------------------------------------
class UpstreamConfig(BaseSettings):
    """Configuration for a single upstream MCP server."""

    model_config = SettingsConfigDict(extra="forbid")

    prefix: str = Field(..., description="Tool namespace prefix (e.g. 'github')")
    transport: Literal["stdio", "http"] = Field(..., description="Transport type")

    # stdio fields
    command: str | None = Field(None, description="Command path for stdio transport")
    args: list[str] = Field(default_factory=list, description="CLI args for stdio transport")
    env: dict[str, str] = Field(default_factory=dict, description="Env vars for stdio transport")

    # http fields
    url: str | None = Field(None, description="URL for http transport")
    headers: dict[str, str] = Field(default_factory=dict, description="Headers for http transport")

    # Semantic salt for upstream_instance_id (§4.3)
    semantic_salt_headers: list[str] = Field(
        default_factory=list,
        description="Stable non-secret header names that affect semantics",
    )
    semantic_salt_env_keys: list[str] = Field(
        default_factory=list,
        description="Stable non-secret env key/value pairs for identity",
    )

    # Tool-level configuration
    strict_schema_reuse: bool = Field(
        True, description="Require schema hash match for reuse (§11.2)"
    )
    inline_allowed: bool = Field(True, description="Allow inline envelope in response (Add. A.1.2)")
    dedupe_exclusions: list[str] = Field(
        default_factory=list,
        description="JSONPath subset exclusions for dedupe hash (§7.2)",
    )

    @field_validator("command")
    @classmethod
    def _validate_stdio_command(cls, value: str | None, info) -> str | None:
        transport = info.data.get("transport")
        if transport == "stdio" and not value:
            msg = "stdio upstream requires command"
            raise ValueError(msg)
        return value

    @field_validator("url")
    @classmethod
    def _validate_http_url(cls, value: str | None, info) -> str | None:
        transport = info.data.get("transport")
        if transport == "http" and not value:
            msg = "http upstream requires url"
            raise ValueError(msg)
        return value


# ---------------------------------------------------------------------------
# Main gateway config
# ---------------------------------------------------------------------------
class GatewayConfig(BaseSettings):
    """Root configuration for the MCP Artifact Gateway."""

    model_config = SettingsConfigDict(
        env_prefix="MCP_GATEWAY_",
        env_nested_delimiter="__",
        extra="forbid",
    )

    # --------------- Filesystem (§17) ---------------
    data_dir: Path = Field(
        Path(DEFAULT_DATA_DIR),
        description="Root data directory (default .mcp_gateway/)",
    )

    # --------------- Database backend ---------------
    db_backend: Literal["sqlite", "postgres"] = Field(
        "sqlite",
        description="Database backend: 'sqlite' (default, zero-config) or 'postgres'",
    )

    # --------------- Postgres ---------------
    postgres_dsn: str = Field(
        "postgresql://localhost:5432/mcp_gateway",
        description="Postgres connection string",
    )
    postgres_pool_min: int = Field(2, ge=1)
    postgres_pool_max: int = Field(10, ge=1)
    postgres_statement_timeout_ms: int = Field(30_000, ge=1000)

    # --------------- SQLite ---------------
    sqlite_busy_timeout_ms: int = Field(5000, ge=100)

    # --------------- Upstreams (§4) ---------------
    upstreams: list[UpstreamConfig] = Field(default_factory=list)

    # --------------- Envelope storage (§7.3, §8.4) ---------------
    envelope_jsonb_mode: EnvelopeJsonbMode = Field(EnvelopeJsonbMode.full)
    envelope_jsonb_minimize_threshold_bytes: int = Field(1_000_000, ge=0)
    envelope_canonical_encoding: CanonicalEncoding = Field(CanonicalEncoding.zstd)

    # --------------- Ingest caps (§16.1) ---------------
    max_inbound_request_bytes: int = Field(10_000_000, ge=1)
    max_upstream_error_capture_bytes: int = Field(100_000, ge=1)
    max_json_part_parse_bytes: int = Field(50_000_000, ge=1)

    # --------------- Storage caps (§16.2) ---------------
    max_binary_blob_bytes: int = Field(500_000_000, ge=1)
    max_payload_total_bytes: int = Field(1_000_000_000, ge=1)
    max_total_storage_bytes: int = Field(10_000_000_000, ge=1)

    # --------------- Full mapping (§13.3) ---------------
    max_full_map_bytes: int = Field(10_000_000, ge=1)
    max_root_discovery_k: int = Field(3, ge=1)

    # --------------- Partial mapping budgets (§13.5.2) ---------------
    max_bytes_read_partial_map: int = Field(50_000_000, ge=1)
    max_compute_steps_partial_map: int = Field(5_000_000, ge=1)
    max_depth_partial_map: int = Field(64, ge=1)
    max_records_sampled_partial: int = Field(100, ge=1)
    max_record_bytes_partial: int = Field(100_000, ge=1)
    max_leaf_paths_partial: int = Field(500, ge=1)
    max_root_discovery_depth: int = Field(5, ge=1)

    # --------------- Mapping mode (§13.1) ---------------
    mapping_mode: MappingMode = Field(MappingMode.hybrid)

    # --------------- Retrieval budgets (§16.3) ---------------
    max_items: int = Field(1000, ge=1)
    max_bytes_out: int = Field(5_000_000, ge=1)
    max_wildcards: int = Field(10_000, ge=1)
    max_compute_steps: int = Field(1_000_000, ge=1)

    # --------------- JSONPath caps (§12.3) ---------------
    max_jsonpath_length: int = Field(4096, ge=1)
    max_path_segments: int = Field(64, ge=1)
    max_wildcard_expansion_total: int = Field(10_000, ge=1)

    # --------------- Search (Addendum B) ---------------
    artifact_search_max_limit: int = Field(200, ge=1)

    # --------------- Inline thresholds (Addendum A.3) ---------------
    inline_envelope_max_json_bytes: int = Field(32_768, ge=0)
    inline_envelope_max_total_bytes: int = Field(65_536, ge=0)

    # --------------- Cursor (§14, Addendum D) ---------------
    cursor_ttl_minutes: int = Field(60, ge=1)
    where_canonicalization_mode: WhereCanonicalizationMode = Field(
        WhereCanonicalizationMode.raw_string
    )

    # --------------- Binary probe (§6.3) ---------------
    binary_probe_bytes: int = Field(65_536, ge=0)

    # --------------- Select behavior (Addendum F.1) ---------------
    select_missing_as_null: bool = Field(False)

    # --------------- Advisory lock (§9.1) ---------------
    advisory_lock_timeout_ms: int = Field(5000, ge=100)

    # --------------- Derived paths ---------------
    @property
    def state_dir(self) -> Path:
        return self.data_dir / "state"

    @property
    def resources_dir(self) -> Path:
        return self.data_dir / "resources"

    @property
    def blobs_bin_dir(self) -> Path:
        return self.data_dir / "blobs" / "bin"

    @property
    def tmp_dir(self) -> Path:
        return self.data_dir / "tmp"

    @property
    def logs_dir(self) -> Path:
        return self.data_dir / "logs"

    @property
    def secrets_path(self) -> Path:
        return self.state_dir / "secrets.json"

    @property
    def sqlite_path(self) -> Path:
        return self.state_dir / "gateway.db"

    @property
    def config_json_path(self) -> Path:
        return self.state_dir / "config.json"

    @field_validator("data_dir", mode="before")
    @classmethod
    def _resolve_data_dir(cls, v: str | Path) -> Path:
        return Path(v).resolve()


_JSON_DECODE_TOP_LEVEL_FIELDS = {"upstreams"}
_JSON_DECODE_UPSTREAM_FIELDS = {
    "args",
    "dedupe_exclusions",
    "env",
    "headers",
    "semantic_salt_env_keys",
    "semantic_salt_headers",
}


def _should_decode_json(parts: tuple[str, ...]) -> bool:
    if len(parts) == 1:
        return parts[0] in _JSON_DECODE_TOP_LEVEL_FIELDS

    if len(parts) == 3 and parts[0] == "upstreams" and parts[1].isdigit():
        return parts[2] in _JSON_DECODE_UPSTREAM_FIELDS

    return False


def _coerce_env_value(raw: str, parts: tuple[str, ...]) -> object:
    if not _should_decode_json(parts):
        return raw

    stripped = raw.strip()
    if not stripped:
        return raw
    if stripped[0] not in "[{":
        return raw
    try:
        return json.loads(stripped)
    except json.JSONDecodeError:
        return raw


def _env_path_segments(env_key: str) -> tuple[str, ...] | None:
    prefix = "MCP_GATEWAY_"
    if not env_key.startswith(prefix):
        return None
    suffix = env_key[len(prefix) :]
    if not suffix:
        return None
    parts = tuple(part for part in suffix.split("__") if part)
    if not parts:
        return None
    return parts


def _match_model_field(raw_key: str, fields: Mapping[str, object]) -> str | None:
    for field_name in fields:
        if field_name.lower() == raw_key.lower():
            return field_name
    return None


def _normalize_env_parts(parts: tuple[str, ...]) -> tuple[str, ...] | None:
    top_level = _match_model_field(parts[0], GatewayConfig.model_fields)
    if top_level is None:
        return None

    normalized: list[str] = [top_level]
    if top_level == "upstreams" and len(parts) >= 2:
        normalized.append(parts[1])
        if len(parts) >= 3:
            upstream_field = _match_model_field(parts[2], UpstreamConfig.model_fields)
            if upstream_field is None:
                normalized.append(parts[2].lower())
            else:
                normalized.append(upstream_field)
            if upstream_field in {"env", "headers"}:
                normalized.extend(parts[3:])
            else:
                normalized.extend(part.lower() if not part.isdigit() else part for part in parts[3:])
        return tuple(normalized)

    normalized.extend(part.lower() if not part.isdigit() else part for part in parts[1:])
    return tuple(normalized)


class _SparseList(list):
    """List built from indexed env overrides; merged by index."""


def _set_nested_env_value(container: object | None, parts: tuple[str, ...], value: object) -> object:
    head, *tail = parts
    is_index = head.isdigit()

    if is_index:
        idx = int(head)
        if isinstance(container, _SparseList):
            out: list[object | None] = _SparseList(container)
        elif isinstance(container, list):
            out = list(container)
        else:
            out = _SparseList()
        while len(out) <= idx:
            out.append(None)
        if tail:
            out[idx] = _set_nested_env_value(out[idx], tuple(tail), value)
        else:
            out[idx] = value
        return out

    out = {} if not isinstance(container, dict) else dict(container)
    if tail:
        out[head] = _set_nested_env_value(out.get(head), tuple(tail), value)
    else:
        out[head] = value
    return out


def _deep_merge(base: object, override: object) -> object:
    if isinstance(base, dict) and isinstance(override, dict):
        merged = dict(base)
        for key, value in override.items():
            if key in merged:
                merged[key] = _deep_merge(merged[key], value)
            else:
                merged[key] = _deep_merge(None, value)
        return merged

    if isinstance(base, list) and isinstance(override, list):
        if not isinstance(override, _SparseList):
            return list(override)

        merged = list(base)
        for index, value in enumerate(override):
            if value is None:
                continue
            if index >= len(merged):
                while len(merged) <= index:
                    merged.append(None)
            merged[index] = _deep_merge(merged[index], value)
        return merged

    if isinstance(override, _SparseList):
        return list(override)

    return override


def _load_env_overrides() -> dict[str, object]:
    overrides: object = {}
    env_items = sorted(os.environ.items(), key=lambda item: len(item[0].split("__")))
    for env_key, raw_value in env_items:
        parts = _env_path_segments(env_key)
        if parts is None:
            continue
        normalized_parts = _normalize_env_parts(parts)
        if normalized_parts is None:
            continue
        value = _coerce_env_value(raw_value, normalized_parts)
        overrides = _set_nested_env_value(overrides, normalized_parts, value)
    if not isinstance(overrides, dict):
        return {}
    return overrides


def _load_state_config(data_dir: Path) -> dict[str, object]:
    config_path = data_dir / STATE_SUBDIR / CONFIG_FILENAME
    if not config_path.exists():
        return {}
    raw = json.loads(config_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        msg = "config.json must contain a JSON object"
        raise ValueError(msg)
    return raw


def load_gateway_config(*, data_dir_override: str | None = None) -> GatewayConfig:
    """Load config with precedence env > config.json > defaults."""
    env_overrides = _load_env_overrides()
    env_data_dir = env_overrides.get("data_dir")

    if data_dir_override is not None:
        data_dir = Path(data_dir_override).resolve()
    elif isinstance(env_data_dir, str):
        data_dir = Path(env_data_dir).resolve()
    else:
        data_dir = Path(DEFAULT_DATA_DIR).resolve()

    from_file = _load_state_config(data_dir)
    merged = _deep_merge(from_file, env_overrides)
    if not isinstance(merged, dict):
        msg = "invalid merged gateway config shape"
        raise ValueError(msg)
    merged["data_dir"] = str(data_dir)

    config = GatewayConfig(**merged)

    prefixes = [upstream.prefix for upstream in config.upstreams]
    if len(prefixes) != len(set(prefixes)):
        msg = "upstream prefixes must be unique"
        raise ValueError(msg)

    return config
