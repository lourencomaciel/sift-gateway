"""Resource reference storage for internal/external durability."""

from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path

from mcp_artifact_gateway.fs.blob_store import normalize_mime
from mcp_artifact_gateway.util.hashing import sha256_hex


@dataclass(frozen=True)
class ResourceRef:
    uri: str
    mime: str
    name: str | None
    durability: str
    content_hash: str | None
    byte_count: int
    fs_path: str | None


def _atomic_write_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            dir=path.parent,
            prefix=".tmp-resource-",
            delete=False,
        ) as handle:
            tmp_path = Path(handle.name)
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
    finally:
        if tmp_path is not None and tmp_path.exists():
            try:
                tmp_path.unlink()
            except FileNotFoundError:
                pass


def _sha256_file(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1_048_576)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


class ResourceStore:
    """Store resources for internal durability mode."""

    def __init__(self, resources_dir: Path) -> None:
        self.resources_dir = resources_dir

    def _path_for_hash(self, content_hash: str) -> Path:
        return self.resources_dir / content_hash[:2] / content_hash[2:4] / content_hash

    def put_bytes(
        self,
        payload: bytes,
        *,
        mime: str | None = None,
        name: str | None = None,
        durability: str = "internal",
        source_uri: str | None = None,
    ) -> ResourceRef:
        if durability not in {"internal", "external_ref"}:
            msg = f"unsupported durability: {durability}"
            raise ValueError(msg)

        normalized_mime = normalize_mime(mime)
        if durability == "external_ref":
            if not isinstance(source_uri, str) or not source_uri.strip():
                msg = "external_ref durability requires non-empty source_uri"
                raise ValueError(msg)
            content_hash = sha256_hex(payload)
            return ResourceRef(
                uri=source_uri.strip(),
                mime=normalized_mime,
                name=name,
                durability=durability,
                content_hash=f"sha256:{content_hash}",
                byte_count=len(payload),
                fs_path=None,
            )

        content_hash = sha256_hex(payload)
        path = self._path_for_hash(content_hash)
        if path.exists():
            actual_size = path.stat().st_size
            if actual_size != len(payload):
                msg = f"existing resource size mismatch for {content_hash}"
                raise ValueError(msg)
            existing_hash = _sha256_file(path)
            if existing_hash != content_hash:
                msg = f"existing resource content hash mismatch for {content_hash}"
                raise ValueError(msg)
        else:
            _atomic_write_bytes(path, payload)

        return ResourceRef(
            uri=path.resolve().as_uri(),
            mime=normalized_mime,
            name=name,
            durability=durability,
            content_hash=f"sha256:{content_hash}",
            byte_count=len(payload),
            fs_path=str(path),
        )
