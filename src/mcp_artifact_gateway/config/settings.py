"""Gateway configuration models — loaded from env / config.json / defaults.

Spec references: §2, §3, §16, §17, §18, Addendum A.3, Addendum D.3.
"""

from __future__ import annotations

import os
from enum import Enum
from pathlib import Path
from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic_settings.sources import JsonConfigSettingsSource, PydanticBaseSettingsSource

from mcp_artifact_gateway.constants import DEFAULT_DATA_DIR


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

    # --------------- Postgres ---------------
    postgres_dsn: str = Field(
        "postgresql://localhost:5432/mcp_gateway",
        description="Postgres connection string",
    )
    postgres_pool_min: int = Field(2, ge=1)
    postgres_pool_max: int = Field(10, ge=1)
    postgres_statement_timeout_ms: int = Field(30_000, ge=1000)

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
    def config_json_path(self) -> Path:
        return self.state_dir / "config.json"

    @field_validator("data_dir", mode="before")
    @classmethod
    def _resolve_data_dir(cls, v: str | Path) -> Path:
        return Path(v).resolve()

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        """Load config from env vars -> config.json -> defaults.

        The config.json file is resolved under DATA_DIR/state/config.json where
        DATA_DIR comes from an explicit init override (if provided), else the
        MCP_GATEWAY_DATA_DIR environment variable, else the default.
        """
        init_kwargs = getattr(init_settings, "init_kwargs", {})
        data_dir_override = init_kwargs.get("data_dir")
        if data_dir_override is not None:
            base_dir = Path(data_dir_override).resolve()
        else:
            env_data_dir = os.getenv("MCP_GATEWAY_DATA_DIR")
            base_dir = Path(env_data_dir or DEFAULT_DATA_DIR).resolve()

        config_path = base_dir / "state" / "config.json"
        json_settings = JsonConfigSettingsSource(settings_cls, json_file=config_path)

        # Priority: init (CLI overrides) -> env -> config.json -> defaults
        return init_settings, env_settings, json_settings, dotenv_settings, file_secret_settings
