"""Define frozen dataclass models for the artifact envelope.

Provide the ``Envelope`` container and its constituent content
part types (``JsonContentPart``, ``TextContentPart``,
``ResourceRefContentPart``, ``BinaryRefContentPart``) plus
the ``ErrorBlock`` for error envelopes.  All models are
frozen dataclasses with ``to_dict`` serialization.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, cast

from sift_gateway.constants import ENVELOPE_TYPE


@dataclass(frozen=True)
class ErrorBlock:
    """Structured error information from an upstream tool call.

    Attributes:
        code: Machine-readable error code string.
        message: Human-readable error description.
        retryable: Whether the caller may retry the request.
        upstream_trace_id: Trace ID from the upstream, if any.
        details: Arbitrary additional error context.
    """

    code: str
    message: str
    retryable: bool = False
    upstream_trace_id: str | None = None
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Serialize the error block to a plain dict.

        Returns:
            A dict with all error fields including defaults.
        """
        return {
            "code": self.code,
            "message": self.message,
            "retryable": self.retryable,
            "upstream_trace_id": self.upstream_trace_id,
            "details": self.details,
        }


@dataclass(frozen=True)
class JsonContentPart:
    """Content part holding an arbitrary JSON-serializable value.

    Attributes:
        value: The JSON-compatible Python value.
        type: Discriminator literal, always "json".
    """

    value: Any
    type: Literal["json"] = "json"

    def to_dict(self) -> dict[str, Any]:
        """Serialize the JSON content part to a plain dict.

        Returns:
            A dict with ``type`` and ``value`` keys.
        """
        return {"type": self.type, "value": self.value}


@dataclass(frozen=True)
class TextContentPart:
    """Content part holding a plain-text string.

    Attributes:
        text: The text content.
        type: Discriminator literal, always "text".
    """

    text: str
    type: Literal["text"] = "text"

    def to_dict(self) -> dict[str, Any]:
        """Serialize the text content part to a plain dict.

        Returns:
            A dict with ``type`` and ``text`` keys.
        """
        return {"type": self.type, "text": self.text}


@dataclass(frozen=True)
class ResourceRefContentPart:
    """Content part referencing an external or internal resource.

    Attributes:
        uri: Resource URI (e.g. https:// or internal://).
        mime: MIME type of the resource, if known.
        name: Human-readable resource name, if available.
        durability: "internal" for gateway-managed resources
            or "external_ref" for external URIs.
        content_hash: Hash of the resource content, if known.
        type: Discriminator literal, always "resource_ref".
    """

    uri: str
    mime: str | None = None
    name: str | None = None
    durability: Literal["internal", "external_ref"] = "external_ref"
    content_hash: str | None = None
    type: Literal["resource_ref"] = "resource_ref"

    def to_dict(self) -> dict[str, Any]:
        """Serialize the resource ref part to a plain dict.

        Optional fields (mime, name, content_hash) are omitted
        when None.

        Returns:
            A dict with type, uri, durability and any optional
            fields that are set.
        """
        payload: dict[str, Any] = {
            "type": self.type,
            "uri": self.uri,
            "durability": self.durability,
        }
        if self.mime is not None:
            payload["mime"] = self.mime
        if self.name is not None:
            payload["name"] = self.name
        if self.content_hash is not None:
            payload["content_hash"] = self.content_hash
        return payload


@dataclass(frozen=True)
class BinaryRefContentPart:
    """Content part referencing a binary blob in the blob store.

    Attributes:
        blob_id: Content-addressed blob store identifier.
        binary_hash: SHA-256 hex digest of the raw bytes.
        mime: MIME type of the binary content.
        byte_count: Size of the binary content in bytes.
        type: Discriminator literal, always "binary_ref".
    """

    blob_id: str
    binary_hash: str
    mime: str
    byte_count: int
    type: Literal["binary_ref"] = "binary_ref"

    def to_dict(self) -> dict[str, Any]:
        """Serialize the binary ref part to a plain dict.

        Returns:
            A dict with type, blob_id, binary_hash, mime, and
            byte_count keys.
        """
        return {
            "type": self.type,
            "blob_id": self.blob_id,
            "binary_hash": self.binary_hash,
            "mime": self.mime,
            "byte_count": self.byte_count,
        }


ContentPart = (
    JsonContentPart
    | TextContentPart
    | ResourceRefContentPart
    | BinaryRefContentPart
)


@dataclass(frozen=True)
class Envelope:
    """Immutable artifact envelope wrapping tool call results.

    Store the normalized content parts from an upstream MCP
    tool response along with metadata for integrity
    verification and error reporting.

    Attributes:
        upstream_instance_id: Identity of the upstream server.
        upstream_prefix: Namespace prefix for the tool.
        tool: Bare upstream tool name.
        status: "ok" for success, "error" for failure.
        content: Ordered sequence of normalized content parts.
        error: Structured error block, or None on success.
        meta: Auxiliary metadata dict (warnings, etc.).
        type: Envelope type discriminator constant.
    """

    upstream_instance_id: str
    upstream_prefix: str
    tool: str
    status: Literal["ok", "error"]
    content: list[ContentPart] = field(default_factory=list)
    error: ErrorBlock | None = None
    meta: dict[str, Any] = field(default_factory=dict)
    type: Literal["mcp_envelope"] = cast(Literal["mcp_envelope"], ENVELOPE_TYPE)

    def to_dict(self) -> dict[str, Any]:
        """Serialize the envelope and all content parts to a dict.

        Returns:
            A dict matching the canonical envelope JSON schema.
        """
        return {
            "type": self.type,
            "upstream_instance_id": self.upstream_instance_id,
            "upstream_prefix": self.upstream_prefix,
            "tool": self.tool,
            "status": self.status,
            "content": [part.to_dict() for part in self.content],
            "error": None if self.error is None else self.error.to_dict(),
            "meta": self.meta,
        }

    @property
    def contains_binary_refs(self) -> bool:
        """Check whether the envelope references any binary blobs.

        Returns:
            True if at least one BinaryRefContentPart exists.
        """
        return any(
            isinstance(part, BinaryRefContentPart) for part in self.content
        )
