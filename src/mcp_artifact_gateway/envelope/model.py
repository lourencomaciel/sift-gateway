"""Envelope data models."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

from mcp_artifact_gateway.constants import ENVELOPE_TYPE


@dataclass(frozen=True)
class ErrorBlock:
    code: str
    message: str
    retryable: bool = False
    upstream_trace_id: str | None = None
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "retryable": self.retryable,
            "upstream_trace_id": self.upstream_trace_id,
            "details": self.details,
        }


@dataclass(frozen=True)
class JsonContentPart:
    value: Any
    type: Literal["json"] = "json"

    def to_dict(self) -> dict[str, Any]:
        return {"type": self.type, "value": self.value}


@dataclass(frozen=True)
class TextContentPart:
    text: str
    type: Literal["text"] = "text"

    def to_dict(self) -> dict[str, Any]:
        return {"type": self.type, "text": self.text}


@dataclass(frozen=True)
class ResourceRefContentPart:
    uri: str
    mime: str | None = None
    name: str | None = None
    durability: Literal["internal", "external_ref"] = "external_ref"
    content_hash: str | None = None
    type: Literal["resource_ref"] = "resource_ref"

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"type": self.type, "uri": self.uri, "durability": self.durability}
        if self.mime is not None:
            payload["mime"] = self.mime
        if self.name is not None:
            payload["name"] = self.name
        if self.content_hash is not None:
            payload["content_hash"] = self.content_hash
        return payload


@dataclass(frozen=True)
class BinaryRefContentPart:
    blob_id: str
    binary_hash: str
    mime: str
    byte_count: int
    type: Literal["binary_ref"] = "binary_ref"

    def to_dict(self) -> dict[str, Any]:
        return {
            "type": self.type,
            "blob_id": self.blob_id,
            "binary_hash": self.binary_hash,
            "mime": self.mime,
            "byte_count": self.byte_count,
        }


ContentPart = JsonContentPart | TextContentPart | ResourceRefContentPart | BinaryRefContentPart


@dataclass(frozen=True)
class Envelope:
    upstream_instance_id: str
    upstream_prefix: str
    tool: str
    status: Literal["ok", "error"]
    content: list[ContentPart] = field(default_factory=list)
    error: ErrorBlock | None = None
    meta: dict[str, Any] = field(default_factory=dict)
    type: Literal["mcp_envelope"] = ENVELOPE_TYPE

    def to_dict(self) -> dict[str, Any]:
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
        return any(isinstance(part, BinaryRefContentPart) for part in self.content)

