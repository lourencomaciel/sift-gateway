"""Argument and value parsing helpers for the Sift CLI."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
from typing import Any

_DEFAULT_TTL_RAW = "24h"
_TTL_PATTERN = re.compile(r"^([1-9][0-9]*)([smhd]?)$")
_INT_PATTERN = re.compile(r"^[+-]?[0-9]+$")


def parse_json_object(raw_value: str, *, flag: str) -> dict[str, Any]:
    """Parse a JSON object flag payload."""
    try:
        parsed = json.loads(raw_value)
    except json.JSONDecodeError as exc:
        msg = f"invalid {flag} JSON: {exc}"
        raise ValueError(msg) from exc
    if not isinstance(parsed, dict):
        msg = f"{flag} must decode to a JSON object"
        raise ValueError(msg)
    return dict(parsed)


def parse_params_json(raw_params: str | None) -> dict[str, Any]:
    """Parse optional ``--params`` JSON object."""
    if raw_params is None:
        return {}
    return parse_json_object(raw_params, flag="--params")


def normalize_code_flag_values(
    raw_values: list[str] | None,
    *,
    flag: str,
) -> list[str]:
    """Normalize repeatable ``code`` flags and enforce non-empty values."""
    values = raw_values or []
    normalized: list[str] = []
    for raw_value in values:
        value = raw_value.strip()
        if not value:
            msg = f"{flag} values must be non-empty strings"
            raise ValueError(msg)
        normalized.append(value)
    return normalized


def resolve_code_target_arguments(
    args: argparse.Namespace,
) -> dict[str, Any]:
    """Resolve code-target args into single-artifact or multi-artifact shape."""
    raw_positional_artifact_id = getattr(args, "artifact_id", None)
    positional_artifact_id = (
        raw_positional_artifact_id.strip()
        if isinstance(raw_positional_artifact_id, str)
        and raw_positional_artifact_id.strip()
        else None
    )
    raw_positional_root_path = getattr(args, "root_path", None)
    positional_root_path = (
        raw_positional_root_path.strip()
        if isinstance(raw_positional_root_path, str)
        and raw_positional_root_path.strip()
        else None
    )
    raw_flag_artifact_ids = getattr(args, "artifact_ids", None)
    raw_flag_root_paths = getattr(args, "root_paths", None)
    flag_artifact_ids = normalize_code_flag_values(
        raw_flag_artifact_ids
        if isinstance(raw_flag_artifact_ids, list)
        else None,
        flag="--artifact-id",
    )
    flag_root_paths = normalize_code_flag_values(
        raw_flag_root_paths if isinstance(raw_flag_root_paths, list) else None,
        flag="--root-path",
    )

    has_positional_artifact = positional_artifact_id is not None
    has_positional_root = positional_root_path is not None
    uses_positionals = has_positional_artifact or has_positional_root
    uses_flags = bool(flag_artifact_ids or flag_root_paths)

    if uses_positionals and uses_flags:
        msg = (
            "cannot mix positional artifact_id/root_path with "
            "--artifact-id/--root-path"
        )
        raise ValueError(msg)

    if uses_positionals:
        if not (has_positional_artifact and has_positional_root):
            msg = "code positional mode requires both artifact_id and root_path"
            raise ValueError(msg)
        return {
            "artifact_id": positional_artifact_id,
            "root_path": positional_root_path,
        }

    if not flag_artifact_ids:
        msg = (
            "missing artifact target; provide positional artifact_id/root_path "
            "or --artifact-id/--root-path"
        )
        raise ValueError(msg)
    if not flag_root_paths:
        msg = (
            "missing root path; provide positional artifact_id/root_path "
            "or --root-path"
        )
        raise ValueError(msg)
    if len(set(flag_artifact_ids)) != len(flag_artifact_ids):
        msg = "duplicate --artifact-id values are not supported"
        raise ValueError(msg)

    root_paths: dict[str, str]
    if len(flag_root_paths) == 1:
        root_paths = dict.fromkeys(flag_artifact_ids, flag_root_paths[0])
    elif len(flag_root_paths) == len(flag_artifact_ids):
        root_paths = dict(
            zip(
                flag_artifact_ids,
                flag_root_paths,
                strict=True,
            )
        )
    else:
        msg = (
            "when using multiple --artifact-id values, provide one --root-path "
            "or repeat --root-path once per --artifact-id"
        )
        raise ValueError(msg)

    return {
        "artifact_ids": flag_artifact_ids,
        "root_paths": root_paths,
    }


def load_code_source(args: argparse.Namespace) -> str:
    """Load Python source from ``--code`` or ``--file``."""
    if args.code_file is not None:
        code_path = Path(args.code_file)
        if not code_path.exists():
            msg = f"code file not found: {args.code_file}"
            raise ValueError(msg)
        try:
            return code_path.read_text(encoding="utf-8")
        except OSError as exc:
            msg = f"unable to read code file: {args.code_file}"
            raise ValueError(msg) from exc
    code_inline = args.code_inline
    if isinstance(code_inline, str) and code_inline.strip():
        return code_inline
    msg = "missing code source; provide --code or --file"
    raise ValueError(msg)


def parse_ttl_seconds(raw_ttl: str | None) -> int | None:
    """Parse CLI TTL values (e.g., ``30m``, ``24h``, ``7d``)."""
    env_ttl = os.environ.get("SIFT_GATEWAY_TTL")
    if env_ttl is None:
        env_ttl = os.environ.get("SIFT_TTL", _DEFAULT_TTL_RAW)
    candidate = (
        raw_ttl.strip().lower()
        if isinstance(raw_ttl, str) and raw_ttl.strip()
        else env_ttl.strip().lower()
    )
    if candidate in {"none", "off", "0"}:
        return None
    match = _TTL_PATTERN.fullmatch(candidate)
    if match is None:
        msg = f"invalid --ttl value: {candidate}"
        raise ValueError(msg)
    value = int(match.group(1))
    suffix = match.group(2) or "s"
    multiplier = {
        "s": 1,
        "m": 60,
        "h": 60 * 60,
        "d": 24 * 60 * 60,
    }[suffix]
    return value * multiplier


def parse_json_or_text_payload(text: str) -> tuple[Any, bool]:
    """Return parsed JSON when possible, otherwise the raw text."""
    if not text.strip():
        return "", False
    try:
        return json.loads(text), True
    except (json.JSONDecodeError, ValueError):
        return text, False


def normalize_tags(raw_tags: list[str] | None) -> list[str]:
    """Normalize repeated/comma-delimited tag values."""
    tags = raw_tags or []
    out: list[str] = []
    seen: set[str] = set()
    for raw in tags:
        for segment in raw.split(","):
            tag = segment.strip()
            if not tag or tag in seen:
                continue
            seen.add(tag)
            out.append(tag)
    return out


def environment_fingerprint() -> str:
    """Return stable hash of visible environment keys."""
    keys = sorted(os.environ.keys())
    payload = "\n".join(keys).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def normalize_command_argv(raw_argv: list[str]) -> list[str]:
    """Normalize remainder argv for ``sift-gateway run -- <cmd>``."""
    argv = list(raw_argv)
    if argv and argv[0] == "--":
        return argv[1:]
    return argv


def _coerce_cli_flag_value(raw_value: str) -> Any:
    """Coerce a CLI flag value into stable JSON-friendly scalar types."""
    value = raw_value.strip()
    lowered = value.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    if _INT_PATTERN.fullmatch(value):
        unsigned = value[1:] if value and value[0] in {"+", "-"} else value
        # Preserve string cursor/token values that rely on leading zeroes.
        if len(unsigned) > 1 and unsigned.startswith("0"):
            return value
        try:
            return int(value)
        except ValueError:
            return value
    return value


def _is_cli_flag_token(token: str) -> bool:
    """Return whether token should be interpreted as a CLI flag token."""
    return bool(token) and token != "-" and token.startswith("-")


def _raw_cli_flag_key(token: str) -> str | None:
    """Extract raw key segment from one short/long option token."""
    raw_key = token[2:] if token.startswith("--") else token[1:]
    return raw_key if raw_key else None


def _apply_inline_cli_flag_assignment(
    raw_key: str,
    parsed: dict[str, Any],
) -> bool:
    """Apply ``--key=value`` assignment when present."""
    if "=" not in raw_key:
        return False
    key, raw_value = raw_key.split("=", 1)
    key = key.strip()
    if key:
        parsed[key] = _coerce_cli_flag_value(raw_value)
    return True


def _is_cli_flag_value_token(token: str | None) -> bool:
    """Return whether token can be consumed as a positional flag value."""
    if not isinstance(token, str):
        return False
    return bool(token) and token != "--" and not token.startswith("-")


def _consume_cli_flag_token(
    *,
    tokens: list[str],
    index: int,
    parsed: dict[str, Any],
) -> int:
    """Consume one flag token and return number of consumed argv entries."""
    raw_key = _raw_cli_flag_key(tokens[index])
    if raw_key is None:
        return 1
    if _apply_inline_cli_flag_assignment(raw_key, parsed):
        return 1

    key = raw_key.strip()
    if not key:
        return 1
    if key.startswith("no-") and len(key) > 3:
        parsed[key[3:]] = False
        return 1

    next_token = tokens[index + 1] if index + 1 < len(tokens) else None
    if _is_cli_flag_value_token(next_token):
        assert next_token is not None
        parsed[key] = _coerce_cli_flag_value(next_token)
        return 2

    parsed[key] = True
    return 1


def extract_cli_flag_args(command_argv: list[str]) -> dict[str, Any]:
    """Best-effort parse of CLI-style flags from command argv."""
    if len(command_argv) <= 1:
        return {}

    parsed: dict[str, Any] = {}
    tokens = command_argv[1:]
    index = 0
    while index < len(tokens):
        token = tokens[index]
        if token == "--":
            break
        if not _is_cli_flag_token(token):
            index += 1
            continue
        index += _consume_cli_flag_token(
            tokens=tokens,
            index=index,
            parsed=parsed,
        )
    return parsed

