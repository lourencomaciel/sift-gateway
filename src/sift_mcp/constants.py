"""Define immutable constants for Sift v1.9.

Provides workspace identifiers, version strings for
canonicalization and traversal contracts, reserved gateway key
prefixes, artifact/blob ID prefixes, default filesystem layout
paths, and envelope type markers.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Single-tenant workspace (§2.1)
# ---------------------------------------------------------------------------
WORKSPACE_ID: str = "local"

# ---------------------------------------------------------------------------
# Version constants (§10.3, §12.4, §13.5.3, §14)
# ---------------------------------------------------------------------------
CANONICALIZER_VERSION: str = "jcs_rfc8785_v1"
MAPPER_VERSION: str = "mapper_v1"
TRAVERSAL_CONTRACT_VERSION: str = "traversal_v1"
CURSOR_VERSION: str = "cursor_v1"
PRNG_VERSION: str = "prng_xoshiro256ss_v1"

# ---------------------------------------------------------------------------
# Reserved gateway arg stripping (§4.2)
# ---------------------------------------------------------------------------
RESERVED_EXACT_KEYS: frozenset[str] = frozenset(
    {
        "_gateway_context",
        "_gateway_parent_artifact_id",
        "_gateway_chain_seq",
    }
)
RESERVED_PREFIX: str = "_gateway_"

# ---------------------------------------------------------------------------
# Artifact ID prefix (Addendum A.2)
# ---------------------------------------------------------------------------
ARTIFACT_ID_PREFIX: str = "art_"

# ---------------------------------------------------------------------------
# Binary blob ID prefix (§6.1)
# ---------------------------------------------------------------------------
BLOB_ID_PREFIX: str = "bin_"

# ---------------------------------------------------------------------------
# Default filesystem layout (§17)
# ---------------------------------------------------------------------------
DEFAULT_DATA_DIR: str = ".sift-mcp"
STATE_SUBDIR: str = "state"
RESOURCES_SUBDIR: str = "resources"
BLOBS_BIN_SUBDIR: str = "blobs/bin"
TMP_SUBDIR: str = "tmp"
LOGS_SUBDIR: str = "logs"
CONFIG_FILENAME: str = "config.json"

# ---------------------------------------------------------------------------
# Envelope constants (§5)
# ---------------------------------------------------------------------------
ENVELOPE_TYPE: str = "mcp_envelope"
RESPONSE_TYPE_RESULT: str = "gateway_tool_result"
RESPONSE_TYPE_ERROR: str = "gateway_error"
