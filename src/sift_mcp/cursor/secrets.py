"""Manage cursor signing secrets on disk.

Provide the ``CursorSecrets`` dataclass for versioned HMAC
secrets and file-based persistence with atomic writes and
restricted permissions.  Key exports are ``CursorSecrets``,
``load_or_create_cursor_secrets``, ``load_cursor_secrets``,
and ``save_cursor_secrets``.
"""

from __future__ import annotations

import base64
import contextlib
from dataclasses import dataclass
import json
import os
from pathlib import Path
import tempfile


@dataclass(frozen=True)
class CursorSecrets:
    """Immutable set of versioned HMAC signing secrets.

    Support secret rotation by mapping version labels to
    secret values, with one version designated as current
    for signing.

    Attributes:
        active: Map of version label to base64 secret string.
        signing_version: Version label used for new signatures.
    """

    active: dict[str, str]
    signing_version: str

    def current_secret(self) -> str:
        """Return the secret value for the active signing version.

        Returns:
            The base64-encoded secret string.

        Raises:
            KeyError: If signing_version is not in active.
        """
        return self.secret_for(self.signing_version)

    def secret_for(self, version: str) -> str:
        """Look up the secret value for a given version label.

        Args:
            version: Version label to resolve.

        Returns:
            The base64-encoded secret string.

        Raises:
            KeyError: If version is not in the active map.
        """
        secret = self.active.get(version)
        if secret is None:
            msg = f"unknown cursor secret version: {version}"
            raise KeyError(msg)
        return secret

    def to_dict(self) -> dict[str, object]:
        """Serialize secrets to a JSON-compatible dict.

        Returns:
            A dict with ``active`` and ``signing_version`` keys.
        """
        return {
            "active": self.active,
            "signing_version": self.signing_version,
        }

    @classmethod
    def from_dict(cls, raw: dict[str, object]) -> CursorSecrets:
        """Deserialize a CursorSecrets from a raw dict.

        Args:
            raw: Dict with ``active`` (version->secret map)
                and ``signing_version`` (str) keys.

        Returns:
            A validated CursorSecrets instance.

        Raises:
            ValueError: If the payload structure is invalid or
                signing_version is not present in active.
        """
        active_raw = raw.get("active")
        signing_version_raw = raw.get("signing_version")
        if not isinstance(active_raw, dict) or not isinstance(
            signing_version_raw, str
        ):
            msg = "invalid cursor secrets payload"
            raise ValueError(msg)
        active = {str(key): str(value) for key, value in active_raw.items()}
        if signing_version_raw not in active:
            msg = "signing_version must be present in active secrets"
            raise ValueError(msg)
        return cls(active=active, signing_version=signing_version_raw)


def _new_secret() -> str:
    """Generate a fresh 256-bit URL-safe base64 secret.

    Returns:
        An unpadded URL-safe base64 string of 32 random bytes.
    """
    return base64.urlsafe_b64encode(os.urandom(32)).decode("ascii").rstrip("=")


def load_cursor_secrets(path: Path) -> CursorSecrets:
    """Load cursor secrets from a JSON file.

    Args:
        path: Path to the secrets JSON file.

    Returns:
        A CursorSecrets parsed from the file contents.

    Raises:
        ValueError: If the file content is invalid.
    """
    return CursorSecrets.from_dict(json.loads(path.read_text(encoding="utf-8")))


def save_cursor_secrets(path: Path, secrets: CursorSecrets) -> None:
    """Atomically write cursor secrets to a JSON file.

    Write to a temporary file, fsync, then atomic rename.
    Set file permissions to 0600 (owner read/write only).

    Args:
        path: Destination file path for the secrets JSON.
        secrets: CursorSecrets to persist.
    """
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
            with contextlib.suppress(FileNotFoundError):
                tmp_path.unlink()


def load_or_create_cursor_secrets(
    path: Path, *, initial_version: str = "v1"
) -> CursorSecrets:
    """Load existing cursor secrets or generate and save new ones.

    If the file exists, load and return it.  Otherwise generate
    a fresh secret, save it atomically, and return it.

    Args:
        path: File path for the secrets JSON.
        initial_version: Version label for the generated secret.

    Returns:
        The loaded or newly created CursorSecrets.
    """
    if path.exists():
        return load_cursor_secrets(path)

    secrets = CursorSecrets(
        active={initial_version: _new_secret()}, signing_version=initial_version
    )
    save_cursor_secrets(path, secrets)
    return secrets
