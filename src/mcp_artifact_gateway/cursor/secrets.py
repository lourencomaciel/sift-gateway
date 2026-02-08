"""Cursor signing secrets management per Addendum D.3.

Secrets are loaded from DATA_DIR/state/secrets.json and contain one or more
HMAC-SHA256 keys used for cursor signing and verification.  Multiple active
secrets allow key rotation without immediately invalidating outstanding cursors.
"""

from __future__ import annotations

import base64
import json
import os
from pathlib import Path
from typing import Self

from pydantic import BaseModel, model_validator


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class SecretEntry(BaseModel):
    """A single HMAC signing secret."""

    version: str
    hmac_sha256_key_b64: str

    @property
    def key_bytes(self) -> bytes:
        """Decode the base64-encoded key to raw bytes."""
        return base64.b64decode(self.hmac_sha256_key_b64)


class SecretsConfig(BaseModel):
    """Top-level secrets.json schema."""

    cursor_ttl_minutes: int
    active_secrets: list[SecretEntry]
    signing_secret_version: str

    @model_validator(mode="after")
    def _validate_config(self) -> Self:
        # signing_secret_version must reference an existing active secret.
        versions = {s.version for s in self.active_secrets}
        if self.signing_secret_version not in versions:
            raise ValueError(
                f"signing_secret_version {self.signing_secret_version!r} "
                f"is not among active_secrets versions: {sorted(versions)}"
            )

        # Every key must be at least 32 bytes when decoded.
        for entry in self.active_secrets:
            key_len = len(entry.key_bytes)
            if key_len < 32:
                raise ValueError(
                    f"Secret {entry.version!r} key is only {key_len} bytes; "
                    f"minimum is 32 bytes"
                )

        return self


# ---------------------------------------------------------------------------
# SecretStore
# ---------------------------------------------------------------------------

class SecretStore:
    """Manages loading and querying of cursor signing secrets."""

    def __init__(self, secrets_path: Path) -> None:
        self._secrets_path = secrets_path
        self._config: SecretsConfig | None = None

    def load(self) -> SecretsConfig:
        """Load and validate secrets from the JSON file.

        Raises:
            FileNotFoundError: If the secrets file does not exist.
            ValueError: If the file content is invalid.
        """
        if not self._secrets_path.exists():
            raise FileNotFoundError(
                f"Secrets file not found: {self._secrets_path}"
            )

        raw = self._secrets_path.read_text(encoding="utf-8")
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"Secrets file is not valid JSON: {self._secrets_path}"
            ) from exc

        self._config = SecretsConfig.model_validate(data)
        return self._config

    @property
    def _cfg(self) -> SecretsConfig:
        """Return the loaded config, loading if necessary."""
        if self._config is None:
            self.load()
        assert self._config is not None
        return self._config

    def signing_secret(self) -> SecretEntry:
        """Return the current signing secret entry."""
        cfg = self._cfg
        for entry in cfg.active_secrets:
            if entry.version == cfg.signing_secret_version:
                return entry
        # Should never reach here due to model validation, but guard anyway.
        raise ValueError(
            f"Signing secret version {cfg.signing_secret_version!r} "
            f"not found in active secrets"
        )

    def get_secret(self, version: str) -> SecretEntry | None:
        """Look up a secret by version string.  Returns None if not found."""
        cfg = self._cfg
        for entry in cfg.active_secrets:
            if entry.version == version:
                return entry
        return None

    def active_versions(self) -> list[str]:
        """Return all active secret version strings."""
        return [entry.version for entry in self._cfg.active_secrets]

    def cursor_ttl_minutes(self) -> int:
        """Return the configured cursor TTL in minutes."""
        return self._cfg.cursor_ttl_minutes


# ---------------------------------------------------------------------------
# Helper: generate a fresh secrets file
# ---------------------------------------------------------------------------

def generate_secrets_file(path: Path, num_secrets: int = 1) -> None:
    """Create a new secrets.json with freshly generated random keys.

    Each key is 32 cryptographically random bytes, base64-encoded.
    Version names follow the pattern "v1", "v2", etc.
    The signing_secret_version is set to the last (highest) version.

    Parameters:
        path: Destination file path for secrets.json.
        num_secrets: Number of secret entries to generate (default 1).
    """
    if num_secrets < 1:
        raise ValueError("num_secrets must be >= 1")

    secrets: list[dict[str, str]] = []
    for i in range(1, num_secrets + 1):
        key_bytes = os.urandom(32)
        key_b64 = base64.b64encode(key_bytes).decode("ascii")
        secrets.append({
            "version": f"v{i}",
            "hmac_sha256_key_b64": key_b64,
        })

    config = {
        "cursor_ttl_minutes": 60,
        "active_secrets": secrets,
        "signing_secret_version": f"v{num_secrets}",
    }

    # Ensure parent directory exists.
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(config, indent=2) + "\n",
        encoding="utf-8",
    )
