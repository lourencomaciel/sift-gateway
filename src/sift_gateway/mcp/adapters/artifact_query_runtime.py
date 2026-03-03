"""MCP runtime adapters for core artifact query services."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from sift_gateway.artifacts.derive import create_derived_artifact
from sift_gateway.constants import (
    KIND_DERIVED_CODEGEN,
    KIND_DERIVED_QUERY,
    WORKSPACE_ID,
)
from sift_gateway.envelope.responses import gateway_error
from sift_gateway.mcp.lineage import (
    build_lineage_root_catalog,
    compute_related_set_hash,
    resolve_related_artifacts,
)

if TYPE_CHECKING:
    from sift_gateway.mcp.server import GatewayServer


@dataclass(frozen=True)
class GatewayArtifactQueryRuntime:
    """Expose core query runtime hooks from ``GatewayServer``."""

    gateway: GatewayServer

    @property
    def db_pool(self) -> Any:
        """Return the gateway DB pool used by search queries."""
        return self.gateway.db_pool

    @property
    def config(self) -> Any:
        """Return gateway configuration used by capture persistence."""
        return self.gateway.config

    @property
    def artifact_search_max_limit(self) -> int:
        """Return configured maximum search page size."""
        return self.gateway.config.artifact_search_max_limit

    @property
    def max_jsonpath_length(self) -> int:
        """Return maximum allowed JSONPath string length."""
        return self.gateway.config.max_jsonpath_length

    @property
    def max_path_segments(self) -> int:
        """Return maximum allowed JSONPath segment count."""
        return self.gateway.config.max_path_segments

    @property
    def max_wildcard_expansion_total(self) -> int:
        """Return wildcard expansion cap for JSONPath evaluation."""
        return self.gateway.config.max_wildcard_expansion_total

    @property
    def related_query_max_artifacts(self) -> int:
        """Return lineage query related-artifact cap."""
        return self.gateway.config.related_query_max_artifacts

    @property
    def max_bytes_out(self) -> int:
        """Return maximum serialized response byte budget."""
        return self.gateway.config.max_bytes_out

    @property
    def passthrough_max_bytes(self) -> int:
        """Return inline response cap used for full/schema_ref selection."""
        return self.gateway.config.passthrough_max_bytes

    @property
    def blobs_payload_dir(self) -> Any:
        """Return payload blob directory for envelope reconstruction."""
        return self.gateway.config.blobs_payload_dir

    @property
    def select_missing_as_null(self) -> bool:
        """Return select projection missing-path behavior."""
        return self.gateway.config.select_missing_as_null

    @property
    def code_query_max_input_records(self) -> int:
        """Return maximum code-query input record budget."""
        return self.gateway.config.code_query_max_input_records

    @property
    def code_query_max_input_bytes(self) -> int:
        """Return maximum code-query input byte budget."""
        return self.gateway.config.code_query_max_input_bytes

    @property
    def code_query_max_bytes_out(self) -> int:
        """Return maximum code-query response bytes before schema_ref mode."""
        return self.gateway.config.code_query_max_bytes_out

    @property
    def code_query_timeout_seconds(self) -> float:
        """Return runtime timeout for code-query subprocess execution."""
        return self.gateway.config.code_query_timeout_seconds

    @property
    def code_query_max_memory_mb(self) -> int:
        """Return runtime memory limit for code-query subprocess execution."""
        return self.gateway.config.code_query_max_memory_mb

    @property
    def code_query_allowed_import_roots(self) -> list[str] | None:
        """Return configured import allowlist roots for code-query runtime."""
        return self.gateway.config.code_query_allowed_import_roots

    def bounded_limit(self, limit_value: Any) -> int:
        """Bound caller-provided limit using gateway defaults."""
        return self.gateway._bounded_limit(limit_value)

    def cursor_session_artifact_id(self, session_id: str, order_by: str) -> str:
        """Build session-scoped cursor binding for a search order key."""
        return self.gateway._cursor_session_artifact_id(session_id, order_by)

    def verify_cursor(
        self,
        *,
        token: str,
        tool: str,
        artifact_id: str,
    ) -> dict[str, Any]:
        """Validate cursor token and return decoded position state."""
        return self.gateway._verify_cursor(
            token=token,
            tool=tool,
            artifact_id=artifact_id,
        )

    def verify_cursor_payload(
        self,
        *,
        token: str,
        tool: str,
        artifact_id: str,
    ) -> dict[str, Any]:
        """Validate cursor token and return full decoded payload."""
        return self.gateway._verify_cursor_payload(
            token=token,
            tool=tool,
            artifact_id=artifact_id,
        )

    def cursor_position(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Return cursor position state from decoded payload."""
        return self.gateway._cursor_position(payload)

    def issue_cursor(
        self,
        *,
        tool: str,
        artifact_id: str,
        position_state: dict[str, Any],
        extra: dict[str, Any] | None = None,
    ) -> str:
        """Issue cursor token for a subsequent retrieval page."""
        return self.gateway._issue_cursor(
            tool=tool,
            artifact_id=artifact_id,
            position_state=position_state,
            extra=extra,
        )

    def cursor_error(self, token_error: Exception) -> dict[str, Any]:
        """Map cursor exceptions into gateway error payloads."""
        return self.gateway._cursor_error(token_error)

    def assert_cursor_field(
        self,
        payload: Mapping[str, Any],
        *,
        field: str,
        expected: object,
    ) -> None:
        """Assert a cursor payload field matches expected value."""
        self.gateway._assert_cursor_field(
            payload,
            field=field,
            expected=expected,
        )

    def artifact_visible(
        self,
        connection: Any,
        *,
        session_id: str,
        artifact_id: str,
    ) -> bool:
        """Check artifact visibility for a session."""
        return self.gateway._artifact_visible(
            connection,
            session_id=session_id,
            artifact_id=artifact_id,
        )

    def safe_touch_for_retrieval(
        self,
        connection: Any,
        *,
        session_id: str,
        artifact_id: str,
    ) -> bool:
        """Update retrieval recency/session activity."""
        return self.gateway._safe_touch_for_retrieval(
            connection,
            session_id=session_id,
            artifact_id=artifact_id,
        )

    def safe_touch_for_retrieval_many(
        self,
        connection: Any,
        *,
        session_id: str,
        artifact_ids: Sequence[str],
    ) -> bool:
        """Update retrieval recency/session activity for multiple artifacts."""
        return self.gateway._safe_touch_for_retrieval_many(
            connection,
            session_id=session_id,
            artifact_ids=artifact_ids,
        )

    def safe_touch_for_search(
        self,
        connection: Any,
        *,
        session_id: str,
        artifact_ids: Sequence[str],
    ) -> bool:
        """Update session activity for search calls without touching LRU."""
        return self.gateway._safe_touch_for_search(
            connection,
            session_id=session_id,
            artifact_ids=artifact_ids,
        )

    def resolve_related_artifacts(
        self,
        connection: Any,
        *,
        session_id: str,
        anchor_artifact_id: str,
    ) -> list[dict[str, Any]]:
        """Resolve lineage-connected artifacts for an anchor."""
        return resolve_related_artifacts(
            connection,
            session_id=session_id,
            anchor_artifact_id=anchor_artifact_id,
        )

    def compute_related_set_hash(
        self,
        artifacts: list[dict[str, Any]],
    ) -> str:
        """Compute deterministic hash of related artifacts."""
        return compute_related_set_hash(artifacts)

    def build_lineage_root_catalog(
        self,
        entries: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Aggregate root metadata across lineage artifacts."""
        return build_lineage_root_catalog(entries)

    def get_mirrored_tool(self, qualified_name: str) -> Any | None:
        """Resolve mirrored tool metadata by qualified name."""
        return self.gateway.mirrored_tools.get(qualified_name)

    async def call_mirrored_tool(
        self,
        mirrored: Any,
        arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Forward a mirrored tool call through the gateway handler."""
        from sift_gateway.mcp.handlers.mirrored_tool import handle_mirrored_tool

        return await handle_mirrored_tool(self.gateway, mirrored, arguments)

    def not_implemented(self, tool_name: str) -> dict[str, Any]:
        """Return standardized not-implemented error for tool calls."""
        return self.gateway._not_implemented(tool_name)

    def check_sample_corruption(
        self,
        root_row: dict[str, Any],
        sample_rows: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        """Validate sampled rows against mapping root sample metadata."""
        return self.gateway._check_sample_corruption(root_row, sample_rows)

    def run_mapping_inline(
        self,
        connection: Any,
        *,
        handle: Any,
        envelope: Any,
    ) -> bool:
        """Run mapping synchronously for newly captured artifacts."""
        return self.gateway._run_mapping_inline(
            connection,
            handle=handle,
            envelope=envelope,
        )

    def increment_metric(self, attr: str, amount: int = 1) -> None:
        """Increment a gateway counter metric."""
        self.gateway._increment_metric(attr, amount)

    def observe_metric(self, attr: str, value: float) -> None:
        """Record a gateway histogram observation."""
        self.gateway._observe_metric(attr, value)

    def _soft_delete_derived_artifact(
        self,
        artifact_id: str,
    ) -> None:
        """Best-effort cleanup for failed derived persistence flows."""
        if self.gateway.db_pool is None:
            return
        try:
            with self.gateway.db_pool.connection() as cleanup_conn:
                cleanup_conn.execute(
                    """
                    UPDATE artifacts
                    SET deleted_at = datetime('now'),
                        generation = generation + 1
                    WHERE workspace_id = %s
                      AND artifact_id = %s
                      AND deleted_at IS NULL
                    """,
                    (WORKSPACE_ID, artifact_id),
                )
                cleanup_conn.commit()
        except Exception:
            return

    def persist_select_derived(
        self,
        *,
        parent_artifact_ids: list[str],
        arguments: dict[str, Any],
        result_data: dict[str, Any] | list[Any],
    ) -> tuple[str | None, dict[str, Any] | None]:
        """Persist and map derived select output with strict success semantics."""
        if self.gateway.db_pool is None:
            return None, gateway_error(
                "DERIVED_PERSISTENCE_FAILED",
                "derived artifact persistence requires database backend",
                details={"stage": "db_pool_missing"},
            )

        derivation_expression: dict[str, Any] = {
            "root_path": arguments.get("root_path"),
            "select_paths": arguments.get("select_paths"),
            "where": arguments.get("where"),
            "distinct": arguments.get("distinct") is True,
            "order_by": arguments.get("order_by"),
            "count_only": arguments.get("count_only") is True,
            "scope": arguments.get("scope"),
        }

        stage = "create"
        derived_artifact_id: str | None = None
        map_status: str | None = None
        try:
            with self.gateway.db_pool.connection() as connection:
                created = create_derived_artifact(
                    connection=connection,
                    config=self.gateway.config,
                    parent_artifact_ids=parent_artifact_ids,
                    result_data=result_data,
                    derivation_expression=derivation_expression,
                    kind=KIND_DERIVED_QUERY,
                    query_kind="select",
                )
                derived_artifact_id = created.handle.artifact_id

                stage = "mapping"
                mapped = self.gateway._run_mapping_inline(
                    connection,
                    handle=created.handle,
                    envelope=created.envelope,
                )
                if not mapped:
                    self._soft_delete_derived_artifact(
                        created.handle.artifact_id
                    )
                    return None, gateway_error(
                        "DERIVED_PERSISTENCE_FAILED",
                        "derived artifact persistence failed",
                        details={
                            "stage": stage,
                            "artifact_id": created.handle.artifact_id,
                        },
                    )

                stage = "verify_ready"
                status_row = connection.execute(
                    """
                    SELECT map_status
                    FROM artifacts
                    WHERE workspace_id = %s AND artifact_id = %s
                    """,
                    (WORKSPACE_ID, created.handle.artifact_id),
                ).fetchone()
                map_status = (
                    str(status_row[0])
                    if status_row is not None and status_row[0] is not None
                    else None
                )
                if map_status != "ready":
                    self._soft_delete_derived_artifact(
                        created.handle.artifact_id
                    )
                    return None, gateway_error(
                        "DERIVED_PERSISTENCE_FAILED",
                        "derived artifact mapping did not reach ready status",
                        details={
                            "stage": stage,
                            "artifact_id": created.handle.artifact_id,
                            "map_status": map_status,
                        },
                    )
                return created.handle.artifact_id, None
        except Exception as exc:
            if derived_artifact_id is not None:
                self._soft_delete_derived_artifact(derived_artifact_id)
            return None, gateway_error(
                "DERIVED_PERSISTENCE_FAILED",
                "derived artifact persistence failed",
                details={
                    "stage": stage,
                    "error_type": type(exc).__name__,
                    "artifact_id": derived_artifact_id,
                    "map_status": map_status,
                },
            )

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
        """Persist and map derived code output with strict success semantics."""
        if self.gateway.db_pool is None:
            return None, gateway_error(
                "DERIVED_PERSISTENCE_FAILED",
                "derived artifact persistence requires database backend",
                details={"stage": "db_pool_missing"},
            )

        derivation_expression: dict[str, Any] = {
            "artifact_ids": parent_artifact_ids,
            "root_paths": requested_root_paths,
            "root_path": root_path,
            "code_hash": code_hash,
            "params_hash": params_hash,
        }

        stage = "create"
        derived_artifact_id: str | None = None
        map_status: str | None = None
        try:
            with self.gateway.db_pool.connection() as connection:
                created = create_derived_artifact(
                    connection=connection,
                    config=self.gateway.config,
                    parent_artifact_ids=parent_artifact_ids,
                    result_data=result_items,
                    derivation_expression=derivation_expression,
                    kind=KIND_DERIVED_CODEGEN,
                    query_kind="code",
                )
                derived_artifact_id = created.handle.artifact_id

                stage = "mapping"
                mapped = self.gateway._run_mapping_inline(
                    connection,
                    handle=created.handle,
                    envelope=created.envelope,
                )
                if not mapped:
                    self._soft_delete_derived_artifact(
                        created.handle.artifact_id
                    )
                    return None, gateway_error(
                        "DERIVED_PERSISTENCE_FAILED",
                        "derived artifact persistence failed",
                        details={
                            "stage": stage,
                            "artifact_id": created.handle.artifact_id,
                        },
                    )

                stage = "verify_ready"
                status_row = connection.execute(
                    """
                    SELECT map_status
                    FROM artifacts
                    WHERE workspace_id = %s AND artifact_id = %s
                    """,
                    (WORKSPACE_ID, created.handle.artifact_id),
                ).fetchone()
                map_status = (
                    str(status_row[0])
                    if status_row is not None and status_row[0] is not None
                    else None
                )
                if map_status != "ready":
                    self._soft_delete_derived_artifact(
                        created.handle.artifact_id
                    )
                    return None, gateway_error(
                        "DERIVED_PERSISTENCE_FAILED",
                        "derived artifact mapping did not reach ready status",
                        details={
                            "stage": stage,
                            "artifact_id": created.handle.artifact_id,
                            "map_status": map_status,
                        },
                    )
                return created.handle.artifact_id, None
        except Exception as exc:
            if derived_artifact_id is not None:
                self._soft_delete_derived_artifact(derived_artifact_id)
            return None, gateway_error(
                "DERIVED_PERSISTENCE_FAILED",
                "derived artifact persistence failed",
                details={
                    "stage": stage,
                    "error_type": type(exc).__name__,
                    "artifact_id": derived_artifact_id,
                    "map_status": map_status,
                },
            )


# Backward-compatible alias used by existing imports.
GatewayArtifactSearchRuntime = GatewayArtifactQueryRuntime
