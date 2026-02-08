"""Cursor signing, verification, and staleness detection.

Public API
----------
Secrets management:
    :class:`SecretStore`, :class:`SecretsConfig`, :func:`generate_secrets_file`

Signing / verification:
    :func:`sign_cursor`, :func:`verify_cursor`

Payload construction / binding checks:
    :func:`build_cursor_payload`, :func:`verify_cursor_bindings`

Sample set hashing:
    :func:`compute_sample_set_hash`

Exceptions:
    :class:`CursorError`, :class:`CursorInvalidError`,
    :class:`CursorExpiredError`, :class:`CursorStaleError`
"""

from mcp_artifact_gateway.cursor.hmac import (
    CursorError,
    CursorExpiredError,
    CursorInvalidError,
    CursorStaleError,
    sign_cursor,
    verify_cursor,
)
from mcp_artifact_gateway.cursor.payload import (
    build_cursor_payload,
    verify_cursor_bindings,
)
from mcp_artifact_gateway.cursor.sample_set_hash import compute_sample_set_hash
from mcp_artifact_gateway.cursor.secrets import (
    SecretStore,
    SecretsConfig,
    generate_secrets_file,
)

__all__ = [
    # Secrets
    "SecretStore",
    "SecretsConfig",
    "generate_secrets_file",
    # Signing / verification
    "sign_cursor",
    "verify_cursor",
    # Payload
    "build_cursor_payload",
    "verify_cursor_bindings",
    # Sample set hash
    "compute_sample_set_hash",
    # Exceptions
    "CursorError",
    "CursorInvalidError",
    "CursorExpiredError",
    "CursorStaleError",
]
