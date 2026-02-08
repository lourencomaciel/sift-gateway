"""Cursor signing secret management."""

from __future__ import annotations

import base64
import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class CursorSecrets:
    """Active secrets and current signing version."""

    active: dict[str, str]
    signing_version: str

    def current_secret(self) -> str:
        return self.secret_for(self.signing_version)

    def secret_for(self, version: str) -> str:
        secret = self.active.get(version)
        if secret is None:
            msg = f"unknown cursor secret version: {version}"
            raise KeyError(msg)
        return secret

    def to_dict(self) -> dict[str, object]:
        return {
            "active": self.active,
            "signing_version": self.signing_version,
        }

    @classmethod
    def from_dict(cls, raw: dict[str, object]) -> "CursorSecrets":
        active_raw = raw.get("active")
        signing_version_raw = raw.get("signing_version")
        if not isinstance(active_raw, dict) or not isinstance(signing_version_raw, str):
            msg = "invalid cursor secrets payload"
            raise ValueError(msg)
        active = {str(key): str(value) for key, value in active_raw.items()}
        if signing_version_raw not in active:
            msg = "signing_version must be present in active secrets"
            raise ValueError(msg)
        return cls(active=active, signing_version=signing_version_raw)


def _new_secret() -> str:
    # URL-safe secret encoding; deterministic length and JSON-friendly.
    return base64.urlsafe_b64encode(os.urandom(32)).decode("ascii").rstrip("=")


def load_cursor_secrets(path: Path) -> CursorSecrets:
    return CursorSecrets.from_dict(json.loads(path.read_text(encoding="utf-8")))


def save_cursor_secrets(path: Path, secrets: CursorSecrets) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(secrets.to_dict(), indent=2, sort_keys=True)
    tmp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=".tmp-secrets-",
            delete=False,
        ) as handle:
            tmp_path = Path(handle.name)
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
        os.chmod(path, 0o600)
    finally:
        if tmp_path is not None and tmp_path.exists():
            try:
                tmp_path.unlink()
            except FileNotFoundError:
                pass


def load_or_create_cursor_secrets(path: Path, *, initial_version: str = "v1") -> CursorSecrets:
    if path.exists():
        return load_cursor_secrets(path)

    secrets = CursorSecrets(active={initial_version: _new_secret()}, signing_version=initial_version)
    save_cursor_secrets(path, secrets)
    return secrets
