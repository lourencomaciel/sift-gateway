"""Extract Python code and root-path selections from LLM output."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CodeExtractionResult:
    """Outcome of extracting code from LLM text.

    Attributes:
        code: The extracted Python source code.
        had_fences: Whether the text contained markdown code fences.
        has_entrypoint: Whether the code contains ``def run``.
    """

    code: str
    had_fences: bool
    has_entrypoint: bool


def extract_code(
    text: str,
    *,
    entrypoint: str = "def run",
) -> CodeExtractionResult:
    """Extract Python code from LLM response text.

    Tries markdown fences first, then falls back to raw text.
    Selects the candidate that contains *entrypoint* when possible.

    Args:
        text: Raw LLM response text.
        entrypoint: Function signature to look for (default
            ``"def run"``).

    Returns:
        A ``CodeExtractionResult`` with the best candidate.
    """
    candidates: list[str] = []
    had_fences = False

    if "```python" in text:
        parts = text.split("```python", 1)
        if len(parts) > 1:
            candidates.append(parts[1].split("```", 1)[0].strip())
            had_fences = True
    if "```" in text and not candidates:
        parts = text.split("```", 1)
        if len(parts) > 1:
            candidates.append(parts[1].split("```", 1)[0].strip())
            had_fences = True

    candidates.append(text.strip())

    for candidate in candidates:
        if entrypoint in candidate:
            return CodeExtractionResult(
                code=candidate,
                had_fences=had_fences,
                has_entrypoint=True,
            )

    # No candidate contains the entrypoint.
    logger.warning(
        "LLM response has no '%s'; using raw text as code",
        entrypoint,
    )
    return CodeExtractionResult(
        code=candidates[0] if candidates else text.strip(),
        had_fences=had_fences,
        has_entrypoint=False,
    )


def extract_root_path_comment(
    text: str,
    available_paths: Sequence[str],
) -> str | None:
    """Parse a ``# root_path: ...`` comment from LLM output.

    Returns ``None`` when no valid selection is found so the caller
    can fall back to the first available root.

    Args:
        text: Raw LLM response text (before code extraction).
        available_paths: Valid root paths to match against.

    Returns:
        The selected root path, or ``None`` if not found.
    """
    for line in text.strip().splitlines():
        stripped = line.strip()
        if stripped.startswith("# root_path:"):
            candidate = stripped.split(":", 1)[1].strip()
            if candidate in available_paths:
                return candidate
    return None
