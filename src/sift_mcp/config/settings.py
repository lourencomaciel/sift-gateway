"""Define gateway configuration models with layered loading.

Merge environment variables (``SIFT_MCP_*`` prefix), an optional
``state/config.json`` file, and built-in defaults via Pydantic
settings.  Exports ``GatewayConfig``, ``UpstreamConfig``, and
``load_gateway_config``.

Typical usage example::

    config = load_gateway_config(data_dir_override="./data")
    print(config.db_backend, config.upstreams)
"""

from __future__ import annotations

from enum import Enum
import json
import os
from pathlib import Path
from typing import Literal, Mapping

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)
from pydantic_settings import BaseSettings, SettingsConfigDict

from sift_mcp.constants import (
    CONFIG_FILENAME,
    DEFAULT_DATA_DIR,
    STATE_SUBDIR,
)


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------
class EnvelopeJsonbMode(str, Enum):
    """Control how envelope JSONB is stored in the database.

    Attributes:
        full: Store complete envelope as JSONB.
        minimal_for_large: Store minimal JSONB for large payloads.
        none: Do not store envelope JSONB at all.
    """

    full = "full"
    minimal_for_large = "minimal_for_large"
    none = "none"


class CanonicalEncoding(str, Enum):
    """Compression algorithm for canonical envelope bytes.

    Attributes:
        zstd: Compress with Zstandard.
        gzip: Compress with gzip.
        none: Store uncompressed.
    """

    zstd = "zstd"
    gzip = "gzip"
    none = "none"


class MappingMode(str, Enum):
    """Control when artifact mapping runs after persistence.

    Attributes:
        async_: Run mapping in a background task.
        hybrid: Run mapping inline, fall back to background.
        sync: Run mapping inline within the request.
    """

    async_ = "async"
    hybrid = "hybrid"
    sync = "sync"


class WhereCanonicalizationMode(str, Enum):
    """Control how ``where`` filter expressions are canonicalized.

    Attributes:
        raw_string: Hash the raw filter string as-is.
        canonical_ast: Parse and canonicalize the AST first.
    """

    raw_string = "raw_string"
    canonical_ast = "canonical_ast"


# ---------------------------------------------------------------------------
# Pagination config
# ---------------------------------------------------------------------------
class PaginationConfig(BaseModel):
    """Pagination behavior for an upstream MCP server.

    Configures how the gateway detects pagination metadata in
    upstream responses and constructs follow-up requests.

    Attributes:
        strategy: Pagination scheme: ``"cursor"``, ``"offset"``,
            or ``"page_number"``.
        cursor_response_path: JSONPath to the cursor value in
            the upstream response (cursor strategy).
        cursor_param_name: Argument name to inject the cursor
            value into the next upstream call (cursor strategy).
        offset_param_name: Argument name for the offset value
            (offset strategy).
        page_size_param_name: Argument name to read the page
            size from original args (offset strategy).
        page_param_name: Argument name for the page number
            (page_number strategy).
        has_more_response_path: JSONPath that, when non-null
            and non-empty, signals more pages exist.
    """

    model_config = ConfigDict(extra="forbid")

    strategy: Literal["cursor", "offset", "page_number"]

    # Cursor strategy
    cursor_response_path: str | None = Field(
        None, description="JSONPath to cursor value in response"
    )
    cursor_param_name: str | None = Field(
        None, description="Arg name to inject cursor into next call"
    )

    # Offset strategy
    offset_param_name: str | None = Field(
        None, description="Arg name for offset value"
    )
    page_size_param_name: str | None = Field(
        None, description="Arg name to read page size from"
    )

    # Page number strategy
    page_param_name: str | None = Field(
        None, description="Arg name for page number"
    )

    # Common
    has_more_response_path: str | None = Field(
        None, description="JSONPath for has-more signal"
    )

    @model_validator(mode="after")
    def _check_strategy_fields(self) -> PaginationConfig:
        """Validate that required fields are set for the strategy.

        Returns:
            Self after validation.

        Raises:
            ValueError: When strategy-specific fields are missing.
        """
        if self.strategy == "cursor":
            if not self.cursor_response_path:
                msg = "cursor strategy requires cursor_response_path"
                raise ValueError(msg)
            if not self.cursor_param_name:
                msg = "cursor strategy requires cursor_param_name"
                raise ValueError(msg)
        elif self.strategy == "offset":
            if not self.offset_param_name:
                msg = "offset strategy requires offset_param_name"
                raise ValueError(msg)
            if not self.page_size_param_name:
                msg = "offset strategy requires page_size_param_name"
                raise ValueError(msg)
            if not self.has_more_response_path:
                msg = "offset strategy requires has_more_response_path"
                raise ValueError(msg)
        elif self.strategy == "page_number":
            if not self.page_param_name:
                msg = "page_number strategy requires page_param_name"
                raise ValueError(msg)
            if not self.has_more_response_path:
                msg = "page_number strategy requires has_more_response_path"
                raise ValueError(msg)
        return self


# ---------------------------------------------------------------------------
# Upstream config (§4)
# ---------------------------------------------------------------------------
class UpstreamConfig(BaseSettings):
    """Configuration for a single upstream MCP server.

    Define transport, authentication, and behavioral settings
    for one upstream.  Validated at load time via Pydantic.

    Attributes:
        prefix: Tool namespace prefix (e.g. ``"github"``).
        transport: Transport type, ``"stdio"`` or ``"http"``.
        command: Executable path for stdio transport.
        args: CLI arguments for stdio transport.
        env: Environment variables for stdio transport.
        url: Endpoint URL for http transport.
        headers: HTTP headers for http transport.
        semantic_salt_headers: Stable header names for identity.
        semantic_salt_env_keys: Stable env keys for identity.
        strict_schema_reuse: Require schema hash match for reuse.
        passthrough_allowed: Allow small-result passthrough.
        dedupe_exclusions: JSONPath exclusions for dedupe hash.
        secret_ref: Reference to an external secret store entry.
        inherit_parent_env: Inherit parent process env vars.
    """

    model_config = SettingsConfigDict(extra="forbid")

    prefix: str = Field(
        ..., description="Tool namespace prefix (e.g. 'github')"
    )
    transport: Literal["stdio", "http"] = Field(
        ..., description="Transport type"
    )

    # stdio fields
    command: str | None = Field(
        None, description="Command path for stdio transport"
    )
    args: list[str] = Field(
        default_factory=list, description="CLI args for stdio transport"
    )
    env: dict[str, str] = Field(
        default_factory=dict, description="Env vars for stdio transport"
    )

    # http fields
    url: str | None = Field(None, description="URL for http transport")
    headers: dict[str, str] = Field(
        default_factory=dict, description="Headers for http transport"
    )

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
    passthrough_allowed: bool = Field(
        True, description="Allow passthrough for small results (§ passthrough)"
    )
    dedupe_exclusions: list[str] = Field(
        default_factory=list,
        description="JSONPath subset exclusions for dedupe hash (§7.2)",
    )

    # Pagination
    pagination: PaginationConfig | None = Field(
        None,
        description="Pagination detection and follow-up config",
    )

    # Secret and environment inheritance
    secret_ref: str | None = Field(
        default=None,
        description="Reference to an external secret store entry",
    )
    inherit_parent_env: bool = Field(
        default=False,
        description="Inherit parent process environment variables",
    )

    @field_validator("secret_ref")
    @classmethod
    def _validate_secret_ref(
        cls,
        value: str | None,
    ) -> str | None:
        r"""Reject secret_ref values with path traversal.

        Args:
            value: Candidate secret_ref string.

        Returns:
            The validated secret_ref or None.

        Raises:
            ValueError: If *value* contains ``..``, ``/``,
                ``\\``, or is an absolute path.
        """
        if value is None:
            return None
        if not value.strip():
            msg = "secret_ref must not be empty"
            raise ValueError(msg)
        if ".." in value:
            msg = f"secret_ref {value!r}: must not contain '..'"
            raise ValueError(msg)
        if "/" in value or "\\" in value:
            msg = f"secret_ref {value!r}: must not contain path separators"
            raise ValueError(msg)
        if Path(value).is_absolute():
            msg = f"secret_ref {value!r}: must not be an absolute path"
            raise ValueError(msg)
        return value

    @field_validator("command")
    @classmethod
    def _validate_stdio_command(cls, value: str | None, info) -> str | None:
        """Require command for stdio transport.

        Args:
            value: Candidate command value.
            info: Pydantic validation context.

        Returns:
            The validated command string or None.

        Raises:
            ValueError: If transport is stdio and command
                is empty.
        """
        transport = info.data.get("transport")
        if transport == "stdio" and not value:
            msg = "stdio upstream requires command"
            raise ValueError(msg)
        return value

    @field_validator("url")
    @classmethod
    def _validate_http_url(cls, value: str | None, info) -> str | None:
        """Require url for http transport.

        Args:
            value: Candidate URL value.
            info: Pydantic validation context.

        Returns:
            The validated URL string or None.

        Raises:
            ValueError: If transport is http and url is
                empty.
        """
        transport = info.data.get("transport")
        if transport == "http" and not value:
            msg = "http upstream requires url"
            raise ValueError(msg)
        return value


# ---------------------------------------------------------------------------
# Main gateway config
# ---------------------------------------------------------------------------
class GatewayConfig(BaseSettings):
    """Root configuration for the Sift.

    Merge environment variables (``SIFT_MCP_*`` prefix), an
    optional ``state/config.json`` file, and built-in defaults.
    Nested env vars use ``__`` as a delimiter.

    Attributes:
        data_dir: Root directory for all persistent state.
        db_backend: Database backend (``"sqlite"`` or ``"postgres"``).
        upstreams: List of upstream server configurations.
        mapping_mode: When to run artifact mapping.
        passthrough_max_bytes: Max bytes for passthrough mode.
        cursor_ttl_minutes: Cursor token time-to-live.
    """

    model_config = SettingsConfigDict(
        env_prefix="SIFT_MCP_",
        env_nested_delimiter="__",
        extra="forbid",
    )

    # --------------- Filesystem (§17) ---------------
    data_dir: Path = Field(
        Path(DEFAULT_DATA_DIR),
        description="Root data directory (default .sift-mcp/)",
    )

    # --------------- Database backend ---------------
    db_backend: Literal["sqlite", "postgres"] = Field(
        "sqlite",
        description=("Database backend: 'sqlite' (default) or 'postgres'"),
    )

    # --------------- Postgres ---------------
    postgres_dsn: str = Field(
        "postgresql://localhost:5432/sift",
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
    envelope_canonical_encoding: CanonicalEncoding = Field(
        CanonicalEncoding.zstd
    )

    # --------------- Ingest caps (§16.1) ---------------
    max_inbound_request_bytes: int = Field(10_000_000, ge=1)
    max_upstream_error_capture_bytes: int = Field(100_000, ge=1)
    max_json_part_parse_bytes: int = Field(50_000_000, ge=1)

    # --------------- Storage caps (§16.2) ---------------
    max_binary_blob_bytes: int = Field(500_000_000, ge=1)
    max_payload_total_bytes: int = Field(1_000_000_000, ge=1)
    max_total_storage_bytes: int = Field(10_000_000_000, ge=1)

    # --------------- Quota enforcement (§16.3) ---------------
    quota_enforcement_enabled: bool = Field(True)
    quota_prune_batch_size: int = Field(100, ge=1)
    quota_max_prune_rounds: int = Field(5, ge=1)
    quota_hard_delete_grace_seconds: int = Field(0, ge=0)

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

    # --------------- Passthrough (small result bypass) ---------------
    passthrough_max_bytes: int = Field(8192, ge=0)

    # --------------- Cursor (§14, Addendum D) ---------------
    cursor_ttl_minutes: int = Field(60, ge=1)
    cursor_secret_max_active_keys: int = Field(5, ge=1)
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
        """The persistent state directory."""
        return self.data_dir / "state"

    @property
    def resources_dir(self) -> Path:
        """The resource store directory."""
        return self.data_dir / "resources"

    @property
    def blobs_bin_dir(self) -> Path:
        """The binary blob store directory."""
        return self.data_dir / "blobs" / "bin"

    @property
    def tmp_dir(self) -> Path:
        """The temporary scratch directory."""
        return self.data_dir / "tmp"

    @property
    def logs_dir(self) -> Path:
        """The log output directory."""
        return self.data_dir / "logs"

    @property
    def secrets_path(self) -> Path:
        """The cursor secrets JSON file path."""
        return self.state_dir / "secrets.json"

    @property
    def sqlite_path(self) -> Path:
        """The SQLite database file path."""
        return self.state_dir / "gateway.db"

    @property
    def config_json_path(self) -> Path:
        """The gateway config JSON file path."""
        return self.state_dir / "config.json"

    @field_validator("data_dir", mode="before")
    @classmethod
    def _resolve_data_dir(cls, v: str | Path) -> Path:
        """Resolve data_dir to an absolute path.

        Args:
            v: Raw data directory value (str or Path).

        Returns:
            Resolved absolute Path.
        """
        return Path(v).resolve()


_JSON_DECODE_TOP_LEVEL_FIELDS = {"upstreams"}
_JSON_DECODE_UPSTREAM_FIELDS = {
    "args",
    "dedupe_exclusions",
    "env",
    "headers",
    "pagination",
    "semantic_salt_env_keys",
    "semantic_salt_headers",
}


def _should_decode_json(parts: tuple[str, ...]) -> bool:
    """Determine if an env var value should be JSON-decoded.

    Top-level list/dict fields and upstream sub-fields
    that accept structured values need JSON decoding when
    the raw string starts with ``[`` or ``{``.

    Args:
        parts: Normalized env var path segments.

    Returns:
        True if the value should be JSON-decoded.
    """
    if len(parts) == 1:
        return parts[0] in _JSON_DECODE_TOP_LEVEL_FIELDS

    if len(parts) == 3 and parts[0] == "upstreams" and parts[1].isdigit():
        return parts[2] in _JSON_DECODE_UPSTREAM_FIELDS

    return False


def _coerce_env_value(raw: str, parts: tuple[str, ...]) -> object:
    """Coerce a raw env var string to its typed value.

    Attempt JSON decoding for fields that expect structured
    values when the string starts with ``[`` or ``{``.
    Return the raw string otherwise.

    Args:
        raw: Raw environment variable value.
        parts: Normalized env var path segments.

    Returns:
        Decoded JSON value or the raw string.
    """
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
    """Extract path segments from an SIFT_MCP_ env key.

    Strip the ``SIFT_MCP_`` prefix and split on ``__``
    delimiters to produce nested path segments.

    Args:
        env_key: Full environment variable name.

    Returns:
        Tuple of path segment strings, or None if the key
        does not match the gateway prefix.
    """
    prefix = "SIFT_MCP_"
    if not env_key.startswith(prefix):
        return None
    suffix = env_key[len(prefix) :]
    if not suffix:
        return None
    parts = tuple(part for part in suffix.split("__") if part)
    if not parts:
        return None
    return parts


def _match_model_field(
    raw_key: str, fields: Mapping[str, object]
) -> str | None:
    """Case-insensitively match a key to a model field name.

    Args:
        raw_key: Env var segment to match.
        fields: Model field name mapping.

    Returns:
        The matching field name, or None if no match.
    """
    for field_name in fields:
        if field_name.lower() == raw_key.lower():
            return field_name
    return None


def _normalize_env_parts(parts: tuple[str, ...]) -> tuple[str, ...] | None:
    """Normalize env var path segments to model field names.

    Map each segment to its canonical field name via
    case-insensitive matching against the Pydantic model.
    Preserve case for dict sub-keys (env, headers).

    Args:
        parts: Raw path segments from ``_env_path_segments``.

    Returns:
        Normalized segment tuple, or None if the top-level
        field is unrecognized.
    """
    top_level = _match_model_field(parts[0], GatewayConfig.model_fields)
    if top_level is None:
        return None

    normalized: list[str] = [top_level]
    if top_level == "upstreams" and len(parts) >= 2:
        normalized.append(parts[1])
        if len(parts) >= 3:
            upstream_field = _match_model_field(
                parts[2], UpstreamConfig.model_fields
            )
            if upstream_field is None:
                normalized.append(parts[2].lower())
            else:
                normalized.append(upstream_field)
            if upstream_field in {"env", "headers"}:
                normalized.extend(parts[3:])
            else:
                normalized.extend(
                    part.lower() if not part.isdigit() else part
                    for part in parts[3:]
                )
        return tuple(normalized)

    normalized.extend(
        part.lower() if not part.isdigit() else part for part in parts[1:]
    )
    return tuple(normalized)


class _SparseList(list):
    """List subclass for sparse index-based env override merging.

    Used internally during config loading to distinguish indexed
    environment variable overrides from fully-specified lists
    during deep merge.
    """


def _set_nested_env_value(
    container: object | None, parts: tuple[str, ...], value: object
) -> object:
    """Set a deeply nested value in a dict/list structure.

    Build intermediate dicts or sparse lists as needed to
    reach the target path, then assign the value.

    Args:
        container: Existing container or None.
        parts: Remaining path segments to descend.
        value: Value to assign at the leaf.

    Returns:
        Updated container with the value set.
    """
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
    """Recursively merge override values into a base structure.

    Dicts are merged key-by-key.  Lists are replaced unless
    the override is a ``_SparseList``, in which case only
    non-None indexed entries are patched into the base.

    Args:
        base: Base value (dict, list, or scalar).
        override: Override value to merge in.

    Returns:
        Merged result.
    """
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
    """Collect and normalize SIFT_MCP_* env var overrides.

    Iterate all environment variables, filter for the
    ``SIFT_MCP_`` prefix, normalize segments, coerce
    values, and build a nested dict structure.

    Returns:
        Nested dict of env var overrides keyed by
        normalized field paths.
    """
    overrides: object = {}
    env_items = sorted(
        os.environ.items(), key=lambda item: len(item[0].split("__"))
    )
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
    """Load the config.json file from the state directory.

    Args:
        data_dir: Root data directory path.

    Returns:
        Parsed config dict, or empty dict if the file
        does not exist.

    Raises:
        ValueError: If the file does not contain a JSON
            object.
    """
    config_path = data_dir / STATE_SUBDIR / CONFIG_FILENAME
    if not config_path.exists():
        return {}
    raw = json.loads(config_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        msg = "config.json must contain a JSON object"
        raise ValueError(msg)
    return raw


def _resolve_mcp_servers_format(
    merged: dict[str, object],
    env_overrides: dict[str, object] | None = None,
) -> dict[str, object]:
    """Convert mcpServers format to the legacy upstreams array.

    Detect ``mcpServers`` or VS Code ``mcp`` keys and
    translate them into the ``upstreams`` list that
    ``GatewayConfig`` expects.  Mutate and return the
    merged dict, stripping format-specific keys.

    When *env_overrides* contains ``upstreams``, those are
    deep-merged on top of the converted result so that env
    overrides retain higher precedence than the file.

    Args:
        merged: Merged config dict (file + env overrides).
        env_overrides: Raw env var overrides (before deep
            merge) so that ``_SparseList`` type info is
            preserved for correct index-by-index patching.

    Returns:
        The same dict with upstreams populated and
        format-specific keys removed.
    """
    from sift_mcp.config.mcp_servers import (
        resolve_mcp_servers_config,
    )

    has_mcp_servers = "mcpServers" in merged
    has_vscode = isinstance(merged.get("mcp"), dict)

    uses_new_format = has_mcp_servers or has_vscode

    if not uses_new_format:
        return merged

    upstream_dicts = resolve_mcp_servers_config(merged)
    if upstream_dicts is not None:
        merged["upstreams"] = upstream_dicts

    # Re-apply env upstream overrides on top so that
    # env > file precedence is honoured.  We use the raw
    # env_overrides (not merged) because _deep_merge strips
    # _SparseList to plain list, losing index-patch semantics.
    raw_env_upstreams = (
        env_overrides.get("upstreams") if env_overrides is not None else None
    )
    if raw_env_upstreams is not None and merged.get("upstreams") is not None:
        merged["upstreams"] = _deep_merge(
            merged["upstreams"],
            raw_env_upstreams,
        )

    # Strip keys that GatewayConfig doesn't know about (extra="forbid")
    merged.pop("mcpServers", None)
    merged.pop("mcp", None)
    merged.pop("_gateway_sync", None)

    return merged


def _check_legacy_upstreams(from_file: dict[str, object]) -> None:
    """Reject legacy ``upstreams`` config format from a file.

    Inspect the raw config dict loaded from the state file
    and reject configurations that use the legacy ``upstreams``
    key without the ``mcpServers`` format.

    Args:
        from_file: Raw config dict from the state config file.

    Raises:
        ValueError: If the config uses the legacy ``upstreams``
            format, or mixes both ``mcpServers`` and ``upstreams``.
    """
    has_upstreams = "upstreams" in from_file
    has_mcp = "mcpServers" in from_file
    has_vscode = isinstance(from_file.get("mcp"), dict)
    uses_new = has_mcp or has_vscode

    if has_upstreams and uses_new:
        msg = (
            "config contains both 'mcpServers' and legacy "
            "'upstreams'; use one format or the other, not both"
        )
        raise ValueError(msg)

    if has_upstreams and not uses_new:
        msg = (
            "Legacy 'upstreams' config format is no longer "
            "supported. Use 'mcpServers' format instead. "
            "Run 'sift-mcp init --from <config>' "
            "to migrate."
        )
        raise ValueError(msg)


def load_gateway_config(
    *, data_dir_override: str | None = None
) -> GatewayConfig:
    """Load gateway config with layered precedence.

    Merge environment variables (highest priority), the
    ``state/config.json`` file, and built-in defaults.
    Validate upstream prefix uniqueness before returning.

    Args:
        data_dir_override: Optional explicit data directory
            path, overriding env and default values.

    Returns:
        Fully resolved GatewayConfig instance.

    Raises:
        ValueError: If the merged config is invalid or
            upstream prefixes are not unique.
    """
    env_overrides = _load_env_overrides()
    env_data_dir = env_overrides.get("data_dir")

    if data_dir_override is not None:
        data_dir = Path(data_dir_override).resolve()
    elif isinstance(env_data_dir, str):
        data_dir = Path(env_data_dir).resolve()
    else:
        data_dir = Path(DEFAULT_DATA_DIR).resolve()

    from_file = _load_state_config(data_dir)
    _check_legacy_upstreams(from_file)
    merged = _deep_merge(from_file, env_overrides)
    if not isinstance(merged, dict):
        msg = "invalid merged gateway config shape"
        raise ValueError(msg)
    merged["data_dir"] = str(data_dir)

    # Convert mcpServers format to legacy upstreams if needed
    merged = _resolve_mcp_servers_format(merged, env_overrides)

    config = GatewayConfig(**merged)

    prefixes = [upstream.prefix for upstream in config.upstreams]
    if len(prefixes) != len(set(prefixes)):
        msg = "upstream prefixes must be unique"
        raise ValueError(msg)

    # Reject configs that specify both inline secrets and
    # secret_ref for any upstream.
    from sift_mcp.config.upstream_secrets import (
        validate_no_secret_conflict,
    )

    for upstream in config.upstreams:
        validate_no_secret_conflict(
            upstream.env or None,
            upstream.headers or None,
            upstream.secret_ref,
        )

    return config
