"""Runtime contracts for core services.

Core services depend on abstract runtime hooks so interface-specific layers
can provide cursor/session/DB behavior without coupling service logic to MCP
or CLI implementations.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Protocol


class ConnectionFactory(Protocol):
    """Minimal DB pool protocol used by core services."""

    def connection(self) -> Any:
        """Return a context manager that yields a DB connection."""


class ArtifactSearchRuntime(Protocol):
    """Runtime hooks required by artifact search execution."""

    @property
    def db_pool(self) -> ConnectionFactory | None:
        """Database pool used for artifact search queries."""

    @property
    def artifact_search_max_limit(self) -> int:
        """Maximum allowed page size for artifact search queries."""

    def cursor_session_artifact_id(self, session_id: str, order_by: str) -> str:
        """Build a synthetic cursor artifact binding for search queries."""

    def verify_cursor(
        self,
        *,
        token: str,
        tool: str,
        artifact_id: str,
    ) -> dict[str, Any]:
        """Verify cursor token and return decoded position state."""

    def issue_cursor(
        self,
        *,
        tool: str,
        artifact_id: str,
        position_state: dict[str, Any],
    ) -> str:
        """Issue an encoded cursor token."""

    def cursor_error(self, token_error: Exception) -> dict[str, Any]:
        """Map cursor decoding/binding failures to a response payload."""

    def safe_touch_for_search(
        self,
        connection: Any,
        *,
        session_id: str,
        artifact_ids: Sequence[str],
    ) -> bool:
        """Best-effort session touch performed after successful search."""

    def not_implemented(self, tool_name: str) -> dict[str, Any]:
        """Return a NOT_IMPLEMENTED response for unavailable capabilities."""


class ArtifactCaptureRuntime(Protocol):
    """Runtime hooks required by artifact capture execution."""

    @property
    def db_pool(self) -> ConnectionFactory | None:
        """Database pool used for capture persistence and reuse."""

    @property
    def config(self) -> Any:
        """Gateway/config object consumed by artifact persistence."""

    def run_mapping_inline(
        self,
        connection: Any,
        *,
        handle: Any,
        envelope: Any,
    ) -> bool:
        """Run mapping synchronously after capture persistence."""

    def safe_touch_for_retrieval(
        self,
        connection: Any,
        *,
        session_id: str,
        artifact_id: str,
    ) -> bool:
        """Best-effort touch for capture reuse recency/session activity."""

    def not_implemented(self, tool_name: str) -> dict[str, Any]:
        """Return a NOT_IMPLEMENTED response for unavailable capabilities."""


class ArtifactGetRuntime(Protocol):
    """Runtime hooks required by artifact get execution."""

    @property
    def db_pool(self) -> ConnectionFactory | None:
        """Database pool used by get queries."""

    @property
    def max_jsonpath_length(self) -> int:
        """Maximum JSONPath string length."""

    @property
    def max_path_segments(self) -> int:
        """Maximum JSONPath segment count."""

    @property
    def max_wildcard_expansion_total(self) -> int:
        """Maximum wildcard expansion budget."""

    @property
    def related_query_max_artifacts(self) -> int:
        """Maximum related artifacts allowed for lineage queries."""

    @property
    def max_bytes_out(self) -> int:
        """Maximum output byte budget for retrieval responses."""

    @property
    def blobs_payload_dir(self) -> Any:
        """Blob payload directory used for envelope reconstruction."""

    def bounded_limit(self, limit_value: Any) -> int:
        """Normalize and bound caller-provided limit."""

    def verify_cursor_payload(
        self,
        *,
        token: str,
        tool: str,
        artifact_id: str,
    ) -> dict[str, Any]:
        """Decode and validate cursor payload bindings."""

    def cursor_position(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Extract cursor position state from decoded payload."""

    def issue_cursor(
        self,
        *,
        tool: str,
        artifact_id: str,
        position_state: dict[str, Any],
        extra: dict[str, Any] | None = None,
    ) -> str:
        """Issue an encoded cursor token."""

    def cursor_error(self, token_error: Exception) -> dict[str, Any]:
        """Map cursor decoding/binding failures to a response payload."""

    def assert_cursor_field(
        self,
        payload: Mapping[str, Any],
        *,
        field: str,
        expected: object,
    ) -> None:
        """Assert a cursor payload field matches expected value."""

    def artifact_visible(
        self,
        connection: Any,
        *,
        session_id: str,
        artifact_id: str,
    ) -> bool:
        """Check whether an artifact is visible for a session."""

    def safe_touch_for_retrieval(
        self,
        connection: Any,
        *,
        session_id: str,
        artifact_id: str,
    ) -> bool:
        """Best-effort touch for retrieval recency/session activity."""

    def resolve_related_artifacts(
        self,
        connection: Any,
        *,
        session_id: str,
        anchor_artifact_id: str,
    ) -> list[dict[str, Any]]:
        """Resolve related artifacts for lineage-scoped queries."""

    def compute_related_set_hash(
        self,
        artifacts: list[dict[str, Any]],
    ) -> str:
        """Compute deterministic lineage freshness binding hash."""

    def build_lineage_root_catalog(
        self,
        entries: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Aggregate mapped roots across lineage artifacts."""

    def not_implemented(self, tool_name: str) -> dict[str, Any]:
        """Return a NOT_IMPLEMENTED response for unavailable capabilities."""


class ArtifactSelectRuntime(ArtifactGetRuntime, Protocol):
    """Runtime hooks required by artifact select execution."""

    @property
    def select_missing_as_null(self) -> bool:
        """Whether projected missing paths should be emitted as null."""

    def persist_select_derived(
        self,
        *,
        parent_artifact_ids: list[str],
        arguments: dict[str, Any],
        result_data: dict[str, Any] | list[Any],
    ) -> tuple[str | None, dict[str, Any] | None]:
        """Persist derived artifact for select outputs."""


class ArtifactCodeRuntime(ArtifactGetRuntime, Protocol):
    """Runtime hooks required by artifact code execution."""

    @property
    def code_query_enabled(self) -> bool:
        """Whether code-query execution is enabled."""

    @property
    def code_query_max_input_records(self) -> int:
        """Maximum number of input records permitted for code queries."""

    @property
    def code_query_max_input_bytes(self) -> int:
        """Maximum serialized input bytes permitted for code queries."""

    @property
    def code_query_timeout_seconds(self) -> float:
        """Maximum wall time allowed for code runtime execution."""

    @property
    def code_query_max_memory_mb(self) -> int:
        """Maximum memory allowed for code runtime execution."""

    @property
    def code_query_allowed_import_roots(self) -> list[str] | None:
        """Configured runtime import allowlist roots."""

    def safe_touch_for_retrieval_many(
        self,
        connection: Any,
        *,
        session_id: str,
        artifact_ids: Sequence[str],
    ) -> bool:
        """Best-effort touch for multiple retrieval artifacts."""

    def check_sample_corruption(
        self,
        root_row: dict[str, Any],
        sample_rows: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        """Validate sampled rows against mapping-root sample metadata."""

    def increment_metric(self, attr: str, amount: int = 1) -> None:
        """Increment a runtime metric counter."""

    def observe_metric(self, attr: str, value: float) -> None:
        """Record a runtime metric observation."""

    def persist_code_derived(
        self,
        *,
        parent_artifact_ids: list[str],
        requested_root_paths: dict[str, str],
        root_path: str,
        code_hash: str,
        params_hash: str,
        result_items: list[Any],
    ) -> tuple[str | None, dict[str, Any] | None]:
        """Persist derived artifact for code outputs."""


class ArtifactNextPageRuntime(Protocol):
    """Runtime hooks required by artifact next-page execution."""

    @property
    def db_pool(self) -> ConnectionFactory | None:
        """Database pool used by next-page queries."""

    @property
    def blobs_payload_dir(self) -> Any:
        """Blob payload directory used for envelope reconstruction."""

    def artifact_visible(
        self,
        connection: Any,
        *,
        session_id: str,
        artifact_id: str,
    ) -> bool:
        """Check whether an artifact is visible for a session."""

    def get_mirrored_tool(self, qualified_name: str) -> Any | None:
        """Resolve a mirrored tool by its fully qualified name."""

    async def call_mirrored_tool(
        self,
        mirrored: Any,
        arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Forward a reconstructed next-page call through mirrored tool flow."""

    def not_implemented(self, tool_name: str) -> dict[str, Any]:
        """Return a NOT_IMPLEMENTED response for unavailable capabilities."""
