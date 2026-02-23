"""Parsing helpers for artifact code-query arguments."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from sift_gateway.core.query_scope import resolve_scope
from sift_gateway.envelope.responses import gateway_error


def _normalize_code_artifact_ids(
    arguments: dict[str, Any],
) -> tuple[list[str], dict[str, Any] | None]:
    raw_artifact_id = arguments.get("artifact_id")
    raw_artifact_ids = arguments.get("artifact_ids")

    if raw_artifact_ids is not None:
        if raw_artifact_id is not None:
            return [], gateway_error(
                "INVALID_ARGUMENT",
                "provide either artifact_id or artifact_ids, not both",
            )
        if not isinstance(raw_artifact_ids, list):
            return [], gateway_error(
                "INVALID_ARGUMENT",
                "artifact_ids must be a list",
            )
        if not raw_artifact_ids:
            return [], gateway_error(
                "INVALID_ARGUMENT",
                "artifact_ids cannot be empty",
            )
        normalized: list[str] = []
        seen: set[str] = set()
        for artifact_id in raw_artifact_ids:
            if not isinstance(artifact_id, str) or not artifact_id.strip():
                return [], gateway_error(
                    "INVALID_ARGUMENT",
                    "artifact_ids items must be non-empty strings",
                )
            if artifact_id not in seen:
                normalized.append(artifact_id)
                seen.add(artifact_id)
        return normalized, None

    if not isinstance(raw_artifact_id, str) or not raw_artifact_id.strip():
        return [], gateway_error(
            "INVALID_ARGUMENT",
            "missing artifact_id or artifact_ids",
        )
    return [raw_artifact_id], None


def _normalize_code_root_paths(
    arguments: dict[str, Any],
    *,
    artifact_ids: list[str],
) -> tuple[dict[str, str], dict[str, Any] | None]:
    raw_root_path = arguments.get("root_path")
    raw_root_paths = arguments.get("root_paths")

    if raw_root_paths is not None:
        if raw_root_path is not None:
            return {}, gateway_error(
                "INVALID_ARGUMENT",
                "provide either root_path or root_paths, not both",
            )
        if not isinstance(raw_root_paths, Mapping):
            return {}, gateway_error(
                "INVALID_ARGUMENT",
                "root_paths must be an object keyed by artifact id",
                details={
                    "code": "ROOT_PATHS_SHAPE_INVALID",
                    "hint": (
                        "Provide root_paths as an object: "
                        "{artifact_id: jsonpath}."
                    ),
                },
            )
        normalized: dict[str, str] = {}
        missing_keys: list[str] = []
        expected_keys = sorted(dict.fromkeys(artifact_ids))
        provided_keys = sorted(str(key) for key in raw_root_paths)
        for artifact_id in artifact_ids:
            value = raw_root_paths.get(artifact_id)
            if not isinstance(value, str) or not value.strip():
                missing_keys.append(artifact_id)
                continue
            normalized[artifact_id] = value.strip()
        expected_key_set = set(artifact_ids)
        extra_keys = [
            str(key)
            for key in raw_root_paths
            if not isinstance(key, str) or key not in expected_key_set
        ]
        if missing_keys or extra_keys:
            return {}, gateway_error(
                "INVALID_ARGUMENT",
                "root_paths keys do not match artifact_ids",
                details={
                    "code": "ROOT_PATH_KEYS_MISMATCH",
                    "expected_artifact_ids": expected_keys,
                    "provided_root_paths_keys": provided_keys,
                    "missing_keys": sorted(missing_keys),
                    "extra_keys": sorted(extra_keys),
                    "hint": (
                        "Provide one non-empty root path for each "
                        "artifact_id, or use shared root_path."
                    ),
                },
            )
        return normalized, None

    if not isinstance(raw_root_path, str) or not raw_root_path.strip():
        return {}, gateway_error(
            "INVALID_ARGUMENT",
            "missing root_path or root_paths",
            details={
                "code": "ROOT_PATH_REQUIRED",
                "hint": (
                    "Provide root_path for single/shared queries, "
                    "or root_paths keyed by artifact_id for "
                    "multi-artifact queries."
                ),
            },
        )
    root_path = raw_root_path.strip()
    return dict.fromkeys(artifact_ids, root_path), None


@dataclass(frozen=True)
class _CodeStep:
    """A single step in a multi-step code pipeline."""

    code: str
    params: dict[str, Any]
    name: str | None = None


@dataclass(frozen=True)
class _ParsedCodeArgs:
    """Normalized and validated inputs for code queries."""

    session_id: str
    scope: str
    artifact_ids: list[str]
    root_paths: dict[str, str]
    code: str
    params: dict[str, Any]
    steps: list[_CodeStep] | None = None


def _parse_steps(
    raw_steps: Any,
) -> tuple[list[_CodeStep] | None, dict[str, Any] | None]:
    """Validate and normalize ``steps`` pipeline entries."""
    if raw_steps is None:
        return None, None
    if not isinstance(raw_steps, list):
        return None, gateway_error(
            "INVALID_ARGUMENT",
            "steps must be an array",
        )
    if not raw_steps:
        return None, gateway_error(
            "INVALID_ARGUMENT",
            "steps cannot be empty",
        )
    parsed: list[_CodeStep] = []
    for idx, entry in enumerate(raw_steps):
        if not isinstance(entry, Mapping):
            return None, gateway_error(
                "INVALID_ARGUMENT",
                f"steps[{idx}] must be an object",
            )
        step_code = entry.get("code")
        if not isinstance(step_code, str) or not step_code.strip():
            return None, gateway_error(
                "INVALID_ARGUMENT",
                f"steps[{idx}] missing code",
            )
        step_params = entry.get("params")
        if step_params is not None and not isinstance(step_params, Mapping):
            return None, gateway_error(
                "INVALID_ARGUMENT",
                f"steps[{idx}] params must be an object",
            )
        normalized_params: dict[str, Any] = (
            dict(step_params) if isinstance(step_params, Mapping) else {}
        )
        step_name = entry.get("name")
        if step_name is not None:
            if not isinstance(step_name, str) or not step_name.strip():
                return None, gateway_error(
                    "INVALID_ARGUMENT",
                    f"steps[{idx}] name must be a non-empty string",
                )
            step_name = step_name.strip()
            if len(step_name) > 128:
                return None, gateway_error(
                    "INVALID_ARGUMENT",
                    f"steps[{idx}] name exceeds 128 characters",
                )
        parsed.append(
            _CodeStep(
                code=step_code,
                params=normalized_params,
                name=step_name,
            )
        )
    return parsed, None


def _parse_code_args(
    arguments: dict[str, Any],
) -> tuple[_ParsedCodeArgs | None, dict[str, Any] | None]:
    """Validate and normalize user-provided code-query arguments."""
    ctx = arguments.get("_gateway_context")
    if not isinstance(ctx, dict) or not ctx.get("session_id"):
        return None, gateway_error(
            "INVALID_ARGUMENT", "missing _gateway_context.session_id"
        )
    scope, scope_err = resolve_scope(raw_scope=arguments.get("scope"))
    if scope_err is not None:
        return None, scope_err

    artifact_ids, artifact_ids_err = _normalize_code_artifact_ids(
        arguments,
    )
    if artifact_ids_err is not None:
        return None, artifact_ids_err
    root_paths, root_paths_err = _normalize_code_root_paths(
        arguments,
        artifact_ids=artifact_ids,
    )
    if root_paths_err is not None:
        return None, root_paths_err

    steps, steps_err = _parse_steps(arguments.get("steps"))
    if steps_err is not None:
        return None, steps_err

    code = arguments.get("code")
    has_code = isinstance(code, str) and bool(code.strip())

    if steps is not None:
        # When steps are present, top-level code is optional.
        # Use first step's code as fallback for backward compat.
        effective_code: str = str(code) if has_code else steps[0].code
    elif isinstance(code, str) and code.strip():
        effective_code = code
    else:
        return None, gateway_error("INVALID_ARGUMENT", "missing code")

    params = arguments.get("params")
    if params is not None and not isinstance(params, Mapping):
        return None, gateway_error(
            "INVALID_ARGUMENT", "params must be an object"
        )
    normalized_params: dict[str, Any] = (
        dict(params) if isinstance(params, Mapping) else {}
    )
    return (
        _ParsedCodeArgs(
            session_id=str(ctx["session_id"]),
            scope=scope,
            artifact_ids=artifact_ids,
            root_paths=root_paths,
            code=effective_code,
            params=normalized_params,
            steps=steps,
        ),
        None,
    )
