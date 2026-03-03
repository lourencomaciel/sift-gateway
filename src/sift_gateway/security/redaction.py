"""Redact detected secrets from tool responses before model exposure."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
import importlib
import json
import re
from typing import Any, Protocol, cast
from urllib.parse import parse_qsl, unquote, urlsplit

_MIN_SECRET_CANDIDATE_LENGTH = 8
_MIN_PLAUSIBLE_SECRET_LENGTH = 24
_HEX_ONLY_RE = re.compile(r"^[0-9a-fA-F]+$")
# Compatibility-first policy: apply relaxed redaction to file URLs to keep
# signed links functional. In relaxed mode we redact only known token patterns
# and skip scanner-only heuristic matches.
_SCANNER_FILTER_FIELD_SUFFIX = "_url"
_COMMON_FILE_EXTENSIONS = frozenset(
    ["3g2", "3gp", "3mf", "7z", "aab", "aac", "abw", "ac3", "afm", "ai", "aif", "aiff", "alac", "amr", "ape", "apk", "appimage", "ar", "arj", "asc", "asf", "asm", "asp", "aspx", "atom", "au", "avi", "avif", "avro", "azw", "azw3", "bak", "bash", "bat", "bcpio", "bin", "blend", "bmp", "bz2", "c", "cab", "caf", "cbr", "cbt", "cbz", "cc", "cgi", "class", "conf", "cpio", "cpp", "crt", "cs", "csh", "css", "csv", "cue", "cxx", "dart", "dat", "db", "db3", "deb", "der", "diff", "dmg", "doc", "docm", "docx", "dot", "dotm", "dotx", "dtd", "dxf", "dylib", "ear", "ebook", "ejs", "eml", "eot", "eps", "epub", "erb", "exe", "f4v", "fbx", "fcgi", "feather", "flac", "flv", "fnt", "fpx", "fs", "ftl", "gcode", "gem", "gif", "glb", "gltf", "go", "gpx", "gz", "h", "hbs", "hdf", "hdf5", "heic", "heif", "hpp", "htm", "html", "ico", "ics", "iges", "igs", "ini", "ipa", "ipynb", "iso", "jar", "java", "jfif", "jpe", "jpeg", "jpg", "js", "json", "json5", "jsonl", "jsx", "kar", "key", "kml", "kmz", "kt", "kts", "less", "lock", "log", "lua", "lz", "lz4", "lzh", "m1v", "m2a", "m2ts", "m2v", "m3u", "m3u8", "m4a", "m4b", "m4p", "m4v", "map", "markdown", "md", "mdb", "mid", "midi", "mkv", "mml", "mm", "mov", "mp1", "mp2", "mp3", "mp4", "mpa", "mpe", "mpeg", "mpg", "mpp", "msi", "msp", "mts", "mustache", "mxf", "nar", "ndjson", "nes", "njk", "numbers", "obj", "odp", "ods", "odt", "ogg", "ogm", "ogv", "one", "onepkg", "opml", "opus", "orc", "ost", "otf", "p12", "p7b", "p7c", "pak", "parquet", "pdf", "pem", "pfx", "pgp", "php", "pl", "ply", "png", "pot", "potm", "potx", "ppa", "ppam", "pps", "ppsm", "ppsx", "ppt", "pptm", "pptx", "ps", "ps1", "psm1", "psql", "pub", "py", "pyc", "pyd", "pyi", "qt", "r", "ra", "raf", "ram", "rar", "rb", "rdf", "reg", "rpm", "rst", "rtf", "rw2", "s3m", "sass", "sb3", "sc", "scss", "sgi", "sh", "sig", "skp", "sln", "so", "sql", "sqlite", "sqlite3", "stl", "stp", "svg", "svgz", "swf", "swift", "tar", "tbz", "tcl", "tex", "text", "tfm", "tgz", "tif", "tiff", "tk", "tlz", "toml", "torrent", "tsv", "ttc", "ttf", "twig", "txz", "txt", "vbs", "vcf", "vob", "vue", "wav", "weba", "webm", "webp", "wma", "wmv", "woff", "woff2", "wpd", "wps", "x3d", "xaml", "xcf", "xhtml", "xlam", "xls", "xlsb", "xlsm", "xlsx", "xlt", "xltm", "xltx", "xml", "xpi", "xps", "xsd", "xsl", "xz", "yaml", "yml", "z", "zip", "zst"]
)
_NON_RELAXED_FILE_EXTENSIONS = frozenset(
    {
        "asp",
        "aspx",
        "cgi",
        "ejs",
        "erb",
        "fcgi",
        "htm",
        "html",
        "js",
        "json",
        "json5",
        "jsonl",
        "jsx",
        "mustache",
        "njk",
        "php",
        "ts",
        "tsx",
        "twig",
        "xhtml",
    }
)
_SIGNED_URL_QUERY_KEYS = frozenset(
    {
        "expires",
        "googleaccessid",
        "key-pair-id",
        "oh",
        "oe",
        "policy",
        "sig",
        "signature",
        "x-amz-credential",
        "x-amz-expires",
        "x-amz-security-token",
        "x-amz-signature",
        "x-goog-credential",
        "x-goog-expires",
        "x-goog-signature",
    }
)
_QUERY_SECRET_RE = re.compile(
    r"(?i)([?&](?:access_token|api[_-]?key|client_secret|password|token)=)"
    r"([^&#\"\s]+)"
)
_BEARER_SECRET_RE = re.compile(r"(?i)(\bBearer\s+)([A-Za-z0-9._~+/=-]{6,})")
_KNOWN_TOKEN_PATTERNS = (
    re.compile(r"\bsk_(?:live|test)_[A-Za-z0-9]{8,}\b"),
    re.compile(r"\bgh[pousr]_[A-Za-z0-9]{20,}\b"),
    re.compile(r"\bgithub_pat_[A-Za-z0-9_]{20,}\b"),
    re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{10,}\b"),
    re.compile(r"\bEA[A-Za-z0-9]{20,}\b"),
)
_PUBLIC_ID_PREFIXES = (
    "art_",
    "bin_",
    "act_",
    "ad_",
    "adset_",
    "campaign_",
    "page_",
    "biz_",
)
_HTTP_URL_IN_TEXT_RE = re.compile(r"(?i)https?://[^\s`\"']+")
_URL_PATH_ID_SEGMENT_RE = re.compile(r"^[A-Za-z0-9_-]{20,128}$")
_ID_WORD_RE = re.compile(r"(?i)\bid\b")


@dataclass(frozen=True)
class _HttpUrlContext:
    span_start: int
    span_end: int
    path_segments: frozenset[str]
    path_variants: frozenset[str]


class SecretRedactionError(RuntimeError):
    """Raised when response redaction cannot run in fail-closed mode."""


class _SecretScanner(Protocol):
    """Secret detector abstraction for response redaction."""

    @property
    def available(self) -> bool:
        """Whether the scanner backend is available."""

    def detect(self, text: str) -> set[str]:
        """Return detected secret values from a string payload."""


class DetectSecretsScanner:
    """Adapter over ``detect-secrets`` line scanning."""

    def __init__(self) -> None:
        """Initialize scanner backend handles when installed."""
        self._scan_line: Any | None = None
        self._default_settings: Any | None = None
        self._import_error: Exception | None = None
        try:
            scan_module = importlib.import_module("detect_secrets.core.scan")
            settings_module = importlib.import_module("detect_secrets.settings")
            self._scan_line = cast(Any, scan_module).scan_line
            self._default_settings = cast(Any, settings_module).default_settings
        except Exception as exc:  # pragma: no cover - import-path specific
            self._import_error = exc

    @property
    def available(self) -> bool:
        """Return true when ``detect-secrets`` imports successfully."""
        return (
            self._scan_line is not None and self._default_settings is not None
        )

    @property
    def import_error(self) -> Exception | None:
        """Return the import error when scanner backend is unavailable."""
        return self._import_error

    def detect(self, text: str) -> set[str]:
        """Scan text and return detected secret values."""
        if not self.available:
            return set()
        assert self._default_settings is not None
        assert self._scan_line is not None
        detected: set[str] = set()
        lines = text.splitlines() or [text]
        with self._default_settings():
            for line in lines:
                findings = self._scan_line(line)
                for finding in cast(Iterable[Any], findings):
                    secret_value = getattr(finding, "secret_value", None)
                    if isinstance(secret_value, str) and secret_value:
                        detected.add(secret_value)
        return detected


@dataclass(frozen=True)
class RedactionResult:
    """Result payload for one redaction pass."""

    payload: dict[str, Any]
    redacted_count: int


class ResponseSecretRedactor:
    """Recursively redact detected secrets from JSON-like payloads."""

    def __init__(
        self,
        *,
        enabled: bool,
        fail_closed: bool,
        max_scan_bytes: int,
        replacement: str,
        scanner: _SecretScanner | None = None,
    ) -> None:
        """Build a response redactor with scanner and policy controls."""
        self.enabled = enabled
        self.fail_closed = fail_closed
        self.max_scan_bytes = max_scan_bytes
        self.replacement = replacement
        self._scanner = scanner or DetectSecretsScanner()

    def redact_payload(self, payload: dict[str, Any]) -> RedactionResult:
        """Redact secrets from a structured tool response payload."""
        if not self.enabled:
            return RedactionResult(payload=payload, redacted_count=0)

        if not self._scanner.available:
            if self.fail_closed:
                msg = "secret redaction is enabled but detect-secrets is unavailable"
                raise SecretRedactionError(msg)
            return RedactionResult(payload=payload, redacted_count=0)

        try:
            value, redacted_count = self._redact_value(payload)
        except Exception as exc:
            if self.fail_closed:
                msg = "secret redaction failed while scanning response payload"
                raise SecretRedactionError(msg) from exc
            return RedactionResult(payload=payload, redacted_count=0)
        return RedactionResult(
            payload=cast(dict[str, Any], value),
            redacted_count=redacted_count,
        )

    def _redact_value(
        self,
        value: Any,
        *,
        key_hint: str | None = None,
        skip_scanner: bool = False,
    ) -> tuple[Any, int]:
        if isinstance(value, str):
            return self._redact_string(
                value, key_hint=key_hint, skip_scanner=skip_scanner
            )

        if isinstance(value, list):
            return self._redact_list(
                value, key_hint=key_hint, skip_scanner=skip_scanner
            )

        if isinstance(value, dict):
            return self._redact_dict(value)

        return value, 0

    def _redact_list(
        self,
        values: list[Any],
        *,
        key_hint: str | None = None,
        skip_scanner: bool = False,
    ) -> tuple[list[Any], int]:
        total = 0
        changed = False
        updated_items: list[Any] = []
        for item in values:
            updated_item, item_redactions = self._redact_value(
                item,
                key_hint=key_hint,
                skip_scanner=skip_scanner,
            )
            total += item_redactions
            changed = changed or updated_item is not item
            updated_items.append(updated_item)
        return (updated_items if changed else values), total

    def _redact_dict(
        self, values: dict[str, Any]
    ) -> tuple[dict[str, Any], int]:
        total = 0
        changed = False
        updated_map: dict[str, Any] = {}
        for key, current in values.items():
            skip_scanner = self._should_skip_scanner_for_field(
                key=key,
                value=current,
                parent=values,
            )
            updated_current, current_redactions = self._redact_value(
                current,
                key_hint=key,
                skip_scanner=skip_scanner,
            )
            total += current_redactions
            changed = changed or updated_current is not current
            updated_map[key] = updated_current
        return (updated_map if changed else values), total

    def _should_skip_scanner_for_field(
        self,
        *,
        key: str,
        value: Any,
        parent: dict[str, Any],
    ) -> bool:
        if not isinstance(value, str):
            return False
        normalized_key = key.lower()
        if normalized_key == "blob_id" and value.startswith("bin_"):
            return True
        if normalized_key == "uri" and value.startswith("sift://blob/"):
            return True
        if normalized_key not in {"data", "base64", "image_data"}:
            return False
        mime_type = parent.get("mimeType")
        if not isinstance(mime_type, str):
            mime_type = parent.get("mime_type")
        if isinstance(mime_type, str) and mime_type.lower().startswith("image/"):
            return True
        payload_type = parent.get("type")
        return isinstance(payload_type, str) and payload_type.lower() == "image"

    def _should_use_relaxed_url_redaction(
        self,
        key_hint: str | None,
        *,
        text: str | None = None,
    ) -> bool:
        if not isinstance(text, str):
            return False
        if not self._is_http_url(text):
            return False
        # Meta/Facebook CDN media URLs frequently include high-entropy signing
        # fragments that trigger scanner heuristics. Treat all fbcdn URLs in
        # relaxed mode and rely on explicit known-token patterns for secrets.
        if self._is_likely_media_cdn_host(text):
            return True
        if isinstance(key_hint, str):
            normalized_key = key_hint.lower()
            if (
                normalized_key in {"url", "src"}
                and self._is_likely_media_cdn_host(text)
            ):
                return self._has_signed_file_url_query(text)
        if self._looks_like_common_file_url(text):
            if isinstance(key_hint, str) and key_hint.lower().endswith(
                _SCANNER_FILTER_FIELD_SUFFIX
            ):
                return True
            return self._has_signed_file_url_query(text)
        if isinstance(key_hint, str) and key_hint.lower().endswith(
            _SCANNER_FILTER_FIELD_SUFFIX
        ):
            return self._has_signed_file_url_query(text)
        if not self._has_signed_file_url_query(text):
            return False
        return self._query_contains_embedded_http_url(text)

    def _is_http_url(self, text: str) -> bool:
        parsed = urlsplit(text)
        return parsed.scheme.lower() in {"http", "https"} and bool(
            parsed.netloc
        )

    def _is_likely_media_cdn_host(self, text: str) -> bool:
        parsed = urlsplit(text)
        host = parsed.netloc.lower().split(":", 1)[0]
        return host.endswith("fbcdn.net")

    def _query_contains_embedded_http_url(self, text: str) -> bool:
        parsed = urlsplit(text)
        if not parsed.query:
            return False
        for _key, value in parse_qsl(parsed.query, keep_blank_values=True):
            candidate = unquote(value).strip()
            if not candidate:
                continue
            nested = urlsplit(candidate)
            if nested.scheme.lower() in {"http", "https"} and nested.netloc:
                return True
        return False

    def _looks_like_common_file_url(self, text: str) -> bool:
        if not self._is_http_url(text):
            return False
        parsed = urlsplit(text)
        filename = unquote(parsed.path.rsplit("/", 1)[-1]).lower()
        if not filename or "." not in filename:
            return False
        extension = filename.rsplit(".", 1)[-1]
        return (
            extension in _COMMON_FILE_EXTENSIONS
            and extension not in _NON_RELAXED_FILE_EXTENSIONS
        )

    def _has_signed_file_url_query(self, text: str) -> bool:
        parsed = urlsplit(text)
        if not parsed.query:
            return False
        for key, _ in parse_qsl(parsed.query, keep_blank_values=True):
            normalized = key.lower()
            if normalized in _SIGNED_URL_QUERY_KEYS:
                return True
            if normalized.startswith("_nc_"):
                return True
            if normalized.startswith("x-amz-"):
                return True
            if normalized.startswith("x-goog-"):
                return True
        return False

    def _should_scan_string(self, text: str) -> bool:
        raw = text.encode("utf-8", errors="replace")
        return len(raw) <= self.max_scan_bytes

    def _detected_secret_candidates(self, text: str) -> set[str]:
        return {
            candidate
            for candidate in self._scanner.detect(text)
            if (
                len(candidate) >= _MIN_SECRET_CANDIDATE_LENGTH
                and self._is_plausible_secret_candidate(candidate)
            )
        }

    def _replace_detected_secrets(
        self,
        text: str,
        detected: set[str],
    ) -> tuple[str, int]:
        redacted = text
        total_hits = 0
        url_contexts = self._extract_http_url_contexts(text)
        for secret in sorted(detected, key=len, reverse=True):
            if self._is_public_url_path_identifier(
                secret,
                text=text,
                url_contexts=url_contexts,
            ):
                continue
            if secret in redacted:
                total_hits += 1
                redacted = redacted.replace(secret, self.replacement)
        return redacted, total_hits

    def _extract_http_url_contexts(self, text: str) -> list[_HttpUrlContext]:
        contexts: list[_HttpUrlContext] = []
        for match in _HTTP_URL_IN_TEXT_RE.finditer(text):
            url = match.group(0)
            parsed = urlsplit(url)
            if parsed.scheme.lower() not in {"http", "https"}:
                continue
            if not parsed.netloc:
                continue
            path = unquote(parsed.path or "")
            normalized_path = path.lstrip("/")
            path_segments = frozenset(
                segment
                for segment in normalized_path.split("/")
                if _URL_PATH_ID_SEGMENT_RE.fullmatch(segment)
            )
            path_variants: set[str] = set()
            if normalized_path:
                path_variants.add(normalized_path)
                if parsed.query:
                    path_variants.add(f"{normalized_path}?{parsed.query}")
                host_labels = [
                    label for label in parsed.netloc.split(".") if label
                ]
                for idx in range(len(host_labels)):
                    host_suffix = ".".join(host_labels[idx:])
                    if not host_suffix:
                        continue
                    path_variants.add(f"{host_suffix}/{normalized_path}")
                    if parsed.query:
                        path_variants.add(
                            f"{host_suffix}/{normalized_path}?{parsed.query}"
                        )
            contexts.append(
                _HttpUrlContext(
                    span_start=match.start(),
                    span_end=match.end(),
                    path_segments=path_segments,
                    path_variants=frozenset(path_variants),
                )
            )
        return contexts

    def _is_inside_url_span(
        self,
        *,
        start: int,
        end: int,
        url_contexts: list[_HttpUrlContext],
    ) -> bool:
        return any(
            start >= context.span_start and end <= context.span_end
            for context in url_contexts
        )

    def _has_id_labeled_occurrence_outside_urls(
        self,
        candidate: str,
        *,
        text: str,
        url_contexts: list[_HttpUrlContext],
    ) -> bool:
        for match in re.finditer(re.escape(candidate), text):
            if self._is_inside_url_span(
                start=match.start(),
                end=match.end(),
                url_contexts=url_contexts,
            ):
                continue
            window_start = max(0, match.start() - 32)
            window_end = min(len(text), match.end() + 32)
            window = text[window_start:window_end]
            if _ID_WORD_RE.search(window):
                return True
        return False

    def _is_public_url_path_identifier(
        self,
        candidate: str,
        *,
        text: str,
        url_contexts: list[_HttpUrlContext],
    ) -> bool:
        if not candidate:
            return False
        if not url_contexts:
            return False
        normalized = candidate.lstrip("/")
        if "/" in normalized:
            if "?" in normalized or "&" in normalized or "=" in normalized:
                return False
            if not any(
                normalized in context.path_variants
                for context in url_contexts
            ):
                return False
            segments = [
                segment
                for segment in normalized.split("/")
                if _URL_PATH_ID_SEGMENT_RE.fullmatch(segment)
            ]
            if not segments:
                return False
            return any(
                self._has_id_labeled_occurrence_outside_urls(
                    segment,
                    text=text,
                    url_contexts=url_contexts,
                )
                for segment in segments
            )
        if not _URL_PATH_ID_SEGMENT_RE.fullmatch(candidate):
            return False
        if not any(
            candidate in context.path_segments
            for context in url_contexts
        ):
            return False
        return self._has_id_labeled_occurrence_outside_urls(
            candidate,
            text=text,
            url_contexts=url_contexts,
        )

    def _redact_string(
        self,
        text: str,
        *,
        key_hint: str | None = None,
        skip_scanner: bool = False,
    ) -> tuple[str, int]:
        json_result = self._redact_embedded_json_string(
            text,
            key_hint=key_hint,
        )
        if json_result is not None:
            return json_result
        if not self._should_scan_string(text):
            return text, 0

        redacted, total_hits = self._redact_known_secret_patterns(text)
        if skip_scanner:
            if total_hits == 0 or redacted == text:
                return text, 0
            return redacted, total_hits
        if self._should_use_relaxed_url_redaction(
            key_hint,
            text=text,
        ):
            if total_hits == 0 or redacted == text:
                return text, 0
            return redacted, total_hits

        detected = self._detected_secret_candidates(text)
        redacted, scanner_hits = self._replace_detected_secrets(
            redacted,
            detected,
        )
        total_hits += scanner_hits

        if total_hits == 0 or redacted == text:
            return text, 0
        return redacted, total_hits

    def _redact_embedded_json_string(
        self,
        text: str,
        *,
        key_hint: str | None = None,
    ) -> tuple[str, int] | None:
        stripped = text.strip()
        if len(stripped) < 2:
            return None
        if stripped[0] not in {"{", "["}:
            return None
        if stripped[-1] not in {"}", "]"}:
            return None
        try:
            parsed = json.loads(stripped)
        except Exception:
            return None
        if not isinstance(parsed, (dict, list)):
            return None
        redacted_value, redacted_count = self._redact_value(
            parsed,
            key_hint=key_hint,
            skip_scanner=False,
        )
        if redacted_count == 0:
            return text, 0
        serialized = json.dumps(
            redacted_value,
            ensure_ascii=False,
            separators=(",", ":"),
        )
        if stripped == text:
            return serialized, redacted_count
        leading = len(text) - len(text.lstrip())
        trailing = len(text) - len(text.rstrip())
        prefix = text[:leading]
        suffix = text[len(text) - trailing :] if trailing else ""
        return f"{prefix}{serialized}{suffix}", redacted_count

    def _replace_group_two_pattern(
        self,
        *,
        text: str,
        pattern: re.Pattern[str],
    ) -> tuple[str, int]:
        hits = 0

        def _replace_group_two(match: re.Match[str]) -> str:
            nonlocal hits
            prefix = match.group(1)
            value = match.group(2)
            if value == self.replacement:
                return match.group(0)
            hits += 1
            return f"{prefix}{self.replacement}"

        return pattern.sub(_replace_group_two, text), hits

    def _replace_full_token_pattern(
        self,
        *,
        text: str,
        pattern: re.Pattern[str],
    ) -> tuple[str, int]:
        hits = 0

        def _replace_token(match: re.Match[str]) -> str:
            nonlocal hits
            token = match.group(0)
            if token == self.replacement:
                return token
            hits += 1
            return self.replacement

        return pattern.sub(_replace_token, text), hits

    def _redact_known_secret_patterns(self, text: str) -> tuple[str, int]:
        redacted, query_hits = self._replace_group_two_pattern(
            text=text,
            pattern=_QUERY_SECRET_RE,
        )
        redacted, bearer_hits = self._replace_group_two_pattern(
            text=redacted,
            pattern=_BEARER_SECRET_RE,
        )
        total_hits = 0
        total_hits += query_hits + bearer_hits

        for pattern in _KNOWN_TOKEN_PATTERNS:
            redacted, token_hits = self._replace_full_token_pattern(
                text=redacted,
                pattern=pattern,
            )
            total_hits += token_hits

        return redacted, total_hits

    def _matches_known_secret_prefix(self, candidate: str) -> bool:
        lower = candidate.lower()
        if lower.startswith("sk_") and len(candidate) >= 8:
            return True
        if lower.startswith("ghp_") and len(candidate) >= 8:
            return True
        if lower.startswith("bearer "):
            token = candidate[7:].strip()
            return len(token) >= 6
        return False

    def _is_long_structural_secret_candidate(self, candidate: str) -> bool:
        if len(candidate) < _MIN_PLAUSIBLE_SECRET_LENGTH:
            return False
        if any(ch.isspace() for ch in candidate):
            return False
        if candidate.startswith(_PUBLIC_ID_PREFIXES):
            return False
        if _HEX_ONLY_RE.fullmatch(candidate):
            return False
        has_alpha = any(ch.isalpha() for ch in candidate)
        has_digit = any(ch.isdigit() for ch in candidate)
        return has_alpha and has_digit

    def _is_plausible_secret_candidate(self, candidate: str) -> bool:
        return self._matches_known_secret_prefix(
            candidate
        ) or self._is_long_structural_secret_candidate(candidate)
