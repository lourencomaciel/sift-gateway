"""Define gateway configuration models with layered loading.

Merge environment variables (``SIFT_MCP_*`` prefix), an optional
``state/config.json`` file, and built-in defaults via Pydantic
settings.  Exports ``GatewayConfig``, ``UpstreamConfig``, and
``load_gateway_config``.

Typical usage example::

    config = load_gateway_config(data_dir_override="./data")
    print(config.upstreams)
"""

from __future__ import annotations

from collections.abc import Mapping
from enum import StrEnum
import json
import os
from pathlib import Path
from typing import Literal, cast

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
    model_validator,
)
from pydantic_settings import BaseSettings, SettingsConfigDict

from sift_mcp.constants import (
    CONFIG_FILENAME,
    DEFAULT_DATA_DIR,
    STATE_SUBDIR,
)

_CGROUP_MEMORY_LIMIT_SENTINEL_BYTES = 1 << 60
_IN_MEMORY_MAPPING_BUDGET_NUMERATOR = 1
_IN_MEMORY_MAPPING_BUDGET_DENOMINATOR = 16
_IN_MEMORY_MAPPING_BUDGET_MIN_BYTES = 50_000_000
_IN_MEMORY_MAPPING_BUDGET_MAX_BYTES = 512_000_000


def _read_int_file(path: Path) -> int | None:
    """Read an integer value from a file path.

    Args:
        path: File path to read.

    Returns:
        Parsed positive integer value, or ``None`` when
        unavailable/unparseable.
    """
    try:
        raw = path.read_text(encoding="utf-8").strip()
    except OSError:
        return None
    if not raw or raw.lower() == "max":
        return None
    try:
        value = int(raw)
    except ValueError:
        return None
    if value <= 0:
        return None
    return value


def _detect_memory_capacity_bytes() -> int | None:
    """Detect stable process memory capacity in bytes.

    Prefer cgroup limits when present (container-aware),
    otherwise fall back to host physical memory.
    """
    cgroup_candidates: list[int] = []
    for path in (
        Path("/sys/fs/cgroup/memory.max"),
        Path("/sys/fs/cgroup/memory/memory.limit_in_bytes"),
    ):
        value = _read_int_file(path)
        if value is None:
            continue
        if value >= _CGROUP_MEMORY_LIMIT_SENTINEL_BYTES:
            continue
        cgroup_candidates.append(value)
    if cgroup_candidates:
        return min(cgroup_candidates)

    try:
        pages = os.sysconf("SC_PHYS_PAGES")
        page_size = os.sysconf("SC_PAGE_SIZE")
    except (OSError, ValueError, AttributeError):
        return None
    if not isinstance(pages, int) or not isinstance(page_size, int):
        return None
    if pages <= 0 or page_size <= 0:
        return None
    return pages * page_size


def _default_max_in_memory_mapping_bytes() -> int:
    """Compute startup in-memory mapping budget from capacity."""
    capacity = _detect_memory_capacity_bytes()
    if capacity is None:
        return _IN_MEMORY_MAPPING_BUDGET_MIN_BYTES
    derived = (
        capacity * _IN_MEMORY_MAPPING_BUDGET_NUMERATOR
    ) // _IN_MEMORY_MAPPING_BUDGET_DENOMINATOR
    return max(
        _IN_MEMORY_MAPPING_BUDGET_MIN_BYTES,
        min(_IN_MEMORY_MAPPING_BUDGET_MAX_BYTES, derived),
    )


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------
class EnvelopeJsonbMode(StrEnum):
    """Control how envelope JSONB is stored in the database.

    Attributes:
        full: Store complete envelope as JSONB.
        minimal_for_large: Store minimal JSONB for large payloads.
        none: Do not store envelope JSONB at all.
    """

    full = "full"
    minimal_for_large = "minimal_for_large"
    none = "none"


class CanonicalEncoding(StrEnum):
    """Compression algorithm for canonical envelope bytes.

    Attributes:
        gzip: Compress with gzip.
        none: Store uncompressed.
    """

    gzip = "gzip"
    none = "none"


# ---------------------------------------------------------------------------
# Pagination config
# ---------------------------------------------------------------------------
class PaginationConfig(BaseModel):
    """Pagination behavior for an upstream MCP server.

    Configures how the gateway detects pagination metadata in
    upstream responses and constructs follow-up requests.

    Attributes:
        strategy: Pagination scheme: ``"cursor"``, ``"offset"``,
            ``"page_number"``, or ``"param_map"``.
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
        next_params_response_paths: Mapping of next-call argument
            names to JSONPath expressions that read values from
            the upstream response (param_map strategy).
        has_more_response_path: JSONPath that, when non-null
            and non-empty, signals more pages exist.
    """

    model_config = ConfigDict(extra="forbid")

    strategy: Literal["cursor", "offset", "page_number", "param_map"]

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

    # Param-map strategy
    next_params_response_paths: dict[str, str] | None = Field(
        None,
        description=("Map of next-call argument name -> JSONPath in response"),
    )

    # Common
    has_more_response_path: str | None = Field(
        None, description="JSONPath for has-more signal"
    )

    @field_validator("next_params_response_paths")
    @classmethod
    def _validate_next_params_response_paths(
        cls,
        value: dict[str, str] | None,
    ) -> dict[str, str] | None:
        """Validate ``next_params_response_paths`` map entries.

        Args:
            value: Optional map of argument names to JSONPath strings.

        Returns:
            The original map when valid, else ``None``.

        Raises:
            ValueError: If any key/path is empty or non-string.
        """
        if value is None:
            return None
        if not value:
            msg = (
                "param_map strategy requires non-empty "
                "next_params_response_paths"
            )
            raise ValueError(msg)
        for key, path in value.items():
            if not isinstance(key, str) or not key.strip():
                msg = (
                    "next_params_response_paths keys must be non-empty strings"
                )
                raise ValueError(msg)
            if not isinstance(path, str) or not path.strip():
                msg = (
                    "next_params_response_paths values must be "
                    "non-empty JSONPath strings"
                )
                raise ValueError(msg)
        return value

    @model_validator(mode="after")
    def _check_strategy_fields(self) -> PaginationConfig:
        """Validate that required fields are set for the strategy.

        Returns:
            Self after validation.

        Raises:
            ValueError: When strategy-specific fields are missing.
        """
        required_fields_by_strategy: dict[str, tuple[str, ...]] = {
            "cursor": ("cursor_response_path", "cursor_param_name"),
            "offset": (
                "offset_param_name",
                "page_size_param_name",
                "has_more_response_path",
            ),
            "page_number": ("page_param_name", "has_more_response_path"),
            "param_map": ("next_params_response_paths",),
        }
        for field_name in required_fields_by_strategy.get(
            self.strategy, ()
        ):
            if not getattr(self, field_name):
                msg = (
                    f"{self.strategy} strategy requires {field_name}"
                )
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
        secret_ref: Reference to an external secret store entry.
        inherit_parent_env: Inherit parent process env vars.
        external_user_id: Stable user identity for upstream
            auth persistence.  ``"auto"`` generates and persists
            a UUID; any other value is used verbatim.
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

    # Pagination
    pagination: PaginationConfig | None = Field(
        None,
        description="Pagination detection and follow-up config",
    )

    # Auto-pagination (per-upstream overrides; None inherits gateway)
    auto_paginate_max_pages: int | None = Field(
        None,
        ge=0,
        description=(
            "Max pages to auto-fetch (overrides gateway "
            "default). 0 or 1 disables."
        ),
    )
    auto_paginate_max_records: int | None = Field(
        None,
        ge=0,
        description=(
            "Approximate record budget for auto-pagination; "
            "stop fetching more pages once reached "
            "(overrides gateway default)."
        ),
    )
    auto_paginate_timeout_seconds: float | None = Field(
        None,
        gt=0,
        description=(
            "Timeout in seconds for auto-pagination "
            "loop (overrides gateway default)."
        ),
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

    # Persistent identity for upstream auth
    external_user_id: str | None = Field(
        default=None,
        description=(
            "Stable user identity for upstream auth persistence. "
            "Set to 'auto' to generate and persist a UUID. "
            "Any other value is used verbatim. When set, "
            "'--external-user-id <value>' is appended to "
            "stdio args at launch time."
        ),
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
    def _validate_stdio_command(
        cls, value: str | None, info: ValidationInfo
    ) -> str | None:
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
    def _validate_http_url(
        cls, value: str | None, info: ValidationInfo
    ) -> str | None:
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
        sqlite_busy_timeout_ms: SQLite busy timeout in milliseconds.
        upstreams: List of upstream server configurations.
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

    # --------------- SQLite ---------------
    sqlite_busy_timeout_ms: int = Field(5000, ge=100)

    # --------------- Upstreams (§4) ---------------
    upstreams: list[UpstreamConfig] = Field(default_factory=list)

    # --------------- Envelope storage (§7.3, §8.4) ---------------
    envelope_jsonb_mode: EnvelopeJsonbMode = Field(EnvelopeJsonbMode.full)
    envelope_jsonb_minimize_threshold_bytes: int = Field(1_000_000, ge=0)
    envelope_canonical_encoding: CanonicalEncoding = Field(
        CanonicalEncoding.gzip
    )

    @field_validator("envelope_canonical_encoding", mode="before")
    @classmethod
    def _coerce_legacy_zstd_encoding(
        cls, value: object
    ) -> object:
        """Accept legacy ``zstd`` config values gracefully.

        Existing installations may have ``envelope_canonical_encoding:
        "zstd"`` persisted in config or env vars.  Silently coerce to
        ``gzip`` so upgrades don't hard-fail at startup.

        Args:
            value: Raw config value.

        Returns:
            ``"gzip"`` when the input was ``"zstd"``, otherwise
            the original value.
        """
        if isinstance(value, str) and value.strip().lower() == "zstd":
            return "gzip"
        return value

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
    max_in_memory_mapping_bytes: int = Field(
        default_factory=_default_max_in_memory_mapping_bytes,
        ge=1,
    )
    max_root_discovery_k: int = Field(3, ge=1)

    # --------------- Partial mapping budgets (§13.5.2) ---------------
    max_bytes_read_partial_map: int = Field(50_000_000, ge=1)
    max_compute_steps_partial_map: int = Field(5_000_000, ge=1)
    max_depth_partial_map: int = Field(64, ge=1)
    max_records_sampled_partial: int = Field(100, ge=1)
    max_record_bytes_partial: int = Field(100_000, ge=1)
    max_leaf_paths_partial: int = Field(500, ge=1)
    max_root_discovery_depth: int = Field(5, ge=1)

    # --------------- Retrieval budgets (§16.3) ---------------
    max_items: int = Field(1000, ge=1)
    max_bytes_out: int = Field(5_000_000, ge=1)
    max_wildcards: int = Field(10_000, ge=1)
    max_compute_steps: int = Field(1_000_000, ge=1)

    # --------------- Code query runtime ---------------
    code_query_enabled: bool = Field(True)
    code_query_allowed_import_roots: list[str] | None = Field(
        None,
        description=(
            "Explicit import root allowlist for query_kind=code. "
            "When null, use the built-in import allowlist."
        ),
    )
    code_query_timeout_seconds: float = Field(8.0, gt=0)
    code_query_max_memory_mb: int = Field(512, ge=32)
    code_query_max_input_records: int = Field(100_000, ge=1)
    code_query_max_input_bytes: int = Field(50_000_000, ge=1)

    # --------------- JSONPath caps (§12.3) ---------------
    max_jsonpath_length: int = Field(4096, ge=1)
    max_path_segments: int = Field(64, ge=1)
    max_wildcard_expansion_total: int = Field(10_000, ge=1)

    # --------------- Search (Addendum B) ---------------
    artifact_search_max_limit: int = Field(200, ge=1)
    related_query_max_artifacts: int = Field(
        256,
        ge=1,
        description=(
            "Maximum related artifacts allowed in a lineage-scoped query."
        ),
    )

    # --------------- Cursor (§14, Addendum D) ---------------
    cursor_ttl_minutes: int = Field(60, ge=1)

    # --------------- Binary probe (§6.3) ---------------
    binary_probe_bytes: int = Field(65_536, ge=0)

    # --------------- Select behavior (Addendum F.1) ---------------
    select_missing_as_null: bool = Field(False)

    # --------------- Auto-pagination ---------------
    auto_paginate_max_pages: int = Field(
        10,
        ge=0,
        description=(
            "Max pages to auto-fetch before returning a merged "
            "artifact. 0 disables auto-pagination."
        ),
    )
    auto_paginate_max_records: int = Field(
        1000,
        ge=0,
        description=(
            "Approximate record budget for auto-pagination; "
            "stop fetching more pages once reached."
        ),
    )
    auto_paginate_timeout_seconds: float = Field(
        30.0,
        gt=0,
        description="Timeout in seconds for auto-pagination loop.",
    )

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

    @field_validator("code_query_allowed_import_roots")
    @classmethod
    def _validate_code_query_allowed_import_roots(
        cls,
        value: list[str] | None,
    ) -> list[str] | None:
        """Validate configured import roots for code queries.

        Each entry must be a non-empty Python identifier representing
        a top-level import root (for example ``math`` or ``jmespath``).
        Duplicates are removed while preserving order.
        """
        if value is None:
            return None
        cleaned: list[str] = []
        seen: set[str] = set()
        for raw in value:
            root = raw.strip()
            if not root:
                msg = (
                    "code_query_allowed_import_roots entries must be non-empty"
                )
                raise ValueError(msg)
            if not root.isidentifier():
                msg = (
                    "code_query_allowed_import_roots entries must be "
                    "top-level Python module names"
                )
                raise ValueError(msg)
            if root in seen:
                continue
            seen.add(root)
            cleaned.append(root)
        return cleaned


_JSON_DECODE_TOP_LEVEL_FIELDS = {
    "upstreams",
    "code_query_allowed_import_roots",
}
_JSON_DECODE_UPSTREAM_FIELDS = {
    "args",
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


class _SparseList(list[object | None]):
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
            out_list: list[object | None] = _SparseList(container)
        elif isinstance(container, list):
            out_list = list(container)
        else:
            out_list = _SparseList()
        while len(out_list) <= idx:
            out_list.append(None)
        if tail:
            out_list[idx] = _set_nested_env_value(
                out_list[idx], tuple(tail), value
            )
        else:
            out_list[idx] = value
        return out_list

    out_dict: dict[str, object]
    if isinstance(container, dict):
        out_dict = cast(dict[str, object], dict(container))
    else:
        out_dict = {}
    if tail:
        out_dict[head] = _set_nested_env_value(
            out_dict.get(head), tuple(tail), value
        )
    else:
        out_dict[head] = value
    return out_dict


def _merge_dict_values(
    base: dict[str, object], override: dict[str, object]
) -> dict[str, object]:
    """Merge dictionary override values recursively into base."""
    merged_dict = dict(base)
    for key, value in override.items():
        merged_dict[key] = _deep_merge(merged_dict.get(key), value)
    return merged_dict


def _merge_list_values(base: list[object], override: list[object]) -> list[object]:
    """Merge list override into base, supporting sparse index updates."""
    if not isinstance(override, _SparseList):
        return list(override)

    merged_list = list(base)
    for index, value in enumerate(override):
        if value is None:
            continue
        if index >= len(merged_list):
            merged_list.extend([None] * (index + 1 - len(merged_list)))
        merged_list[index] = _deep_merge(merged_list[index], value)
    return merged_list


def _deep_merge(base: object, override: object) -> object:
    """Recursively merge override values into a base structure."""
    if isinstance(base, dict) and isinstance(override, dict):
        return _merge_dict_values(base, override)
    if isinstance(base, list) and isinstance(override, list):
        return _merge_list_values(base, override)
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
    merged.pop("provider", None)
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

    config = GatewayConfig.model_validate(merged)

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
