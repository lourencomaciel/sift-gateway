"""Install and entrypoint hint helpers for artifact code queries."""

from __future__ import annotations

from collections.abc import Callable, Collection
import re

_RE_NO_MODULE = re.compile(r"No module named '([^']+)'")
_RE_IMPORT_NOT_ALLOWED = re.compile(r"import not allowed: (\S+)")

# Well-known module-to-distribution mappings for packages where
# the import root differs from the pip distribution name.
MODULE_TO_DIST: dict[str, str] = {
    "PIL": "pillow",
    "attr": "attrs",
    "bs4": "beautifulsoup4",
    "cv2": "opencv-python",
    "dateutil": "python-dateutil",
    "dotenv": "python-dotenv",
    "skimage": "scikit-image",
    "sklearn": "scikit-learn",
    "yaml": "pyyaml",
}


def module_to_dist(
    root: str,
    *,
    packages_distributions_fn: Callable[[], object],
) -> str:
    """Map an import root to its pip distribution name."""
    try:
        metadata = packages_distributions_fn()
        if isinstance(metadata, dict):
            dists = metadata.get(root)
            if isinstance(dists, list) and dists:
                first = dists[0]
                if isinstance(first, str):
                    return first
    except Exception:
        pass
    return MODULE_TO_DIST.get(root, root)


def enrich_install_hint(
    msg: str,
    *,
    packages_distributions_fn: Callable[[], object],
    stdlib_roots: Collection[str],
) -> str:
    """Append an agent-actionable install hint when possible."""
    m = _RE_NO_MODULE.search(msg)
    if m:
        root = m.group(1).split(".")[0]
        dist = module_to_dist(
            root,
            packages_distributions_fn=packages_distributions_fn,
        )
        return f"{msg}\nRun: sift-gateway install {dist}"
    m = _RE_IMPORT_NOT_ALLOWED.search(msg)
    if m:
        root = m.group(1).split(".")[0]
        # stdlib modules are policy-blocked, not missing —
        # suggesting install would be misleading.
        if root in stdlib_roots:
            return msg
        dist = module_to_dist(
            root,
            packages_distributions_fn=packages_distributions_fn,
        )
        return f"{msg}\nRun: sift-gateway install {dist}"
    return msg


def enrich_entrypoint_hint(
    msg: str,
    *,
    details_code: str | None,
    multi_artifact: bool,
) -> str:
    """Append an entrypoint-shape hint for missing run(...) errors."""
    if details_code != "CODE_ENTRYPOINT_MISSING":
        return msg
    if multi_artifact:
        hint = (
            "Hint: For multi-artifact queries define "
            "def run(artifacts, schemas, params): ... where artifacts is "
            "dict[artifact_id -> list[dict]]."
        )
    else:
        hint = (
            "Hint: For single-artifact queries define "
            "def run(data, schema, params): ... where data is list[dict]."
        )
    if hint in msg:
        return msg
    return f"{msg}\n{hint}"

