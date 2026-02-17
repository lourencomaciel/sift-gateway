"""Install and uninstall Python packages for code queries.

Manages packages in sift's own Python environment and updates
the instance config allowlist so newly installed packages are
permitted in ``query_kind=code`` execution.
"""

from __future__ import annotations

from importlib.metadata import packages_distributions
import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Any

from sift_mcp.codegen.ast_guard import ALLOWED_IMPORT_ROOTS
from sift_mcp.constants import CONFIG_FILENAME, STATE_SUBDIR

# ------------------------------------------------------------------
# Config helpers (intentionally duplicated from init.py to avoid
# pulling in the entire init module and its heavy imports).
# ------------------------------------------------------------------


def _config_path(data_dir: Path) -> Path:
    """Return the ``config.json`` path for a data directory."""
    return data_dir / STATE_SUBDIR / CONFIG_FILENAME


def _load_config(path: Path) -> dict[str, Any]:
    """Load ``config.json`` or return an empty dict."""
    if not path.exists():
        return {}
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        return {}
    return raw


def _write_config(path: Path, data: dict[str, Any]) -> None:
    """Atomically write ``config.json``."""
    import tempfile

    content = json.dumps(data, indent=2, ensure_ascii=False) + "\n"
    fd, tmp = tempfile.mkstemp(
        dir=str(path.parent),
        suffix=".tmp",
    )
    try:
        os.write(fd, content.encode("utf-8"))
        os.close(fd)
        fd = -1
        os.replace(tmp, str(path))
    except BaseException:
        if fd >= 0:
            os.close(fd)
        with _SuppressOsError():
            os.unlink(tmp)
        raise


class _SuppressOsError:
    """Suppress OSError in cleanup paths."""

    def __enter__(self) -> None:
        pass

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> bool:
        return isinstance(exc_val, OSError)


# ------------------------------------------------------------------
# Public API
# ------------------------------------------------------------------


def install_packages(
    packages: list[str],
    *,
    data_dir: Path | None = None,
) -> int:
    """Install packages into sift's Python environment.

    Runs ``pip install`` via the current interpreter, then adds
    each package root to the instance's ``code_query_allowed_import_roots``
    if it is not already in the effective allowlist.

    Args:
        packages: Package names to install (e.g. ``["pandas"]``).
        data_dir: Instance data directory.  When ``None``,
            allowlist updates are skipped.

    Returns:
        Exit code (``0`` on success).
    """
    result = subprocess.run(
        [sys.executable, "-m", "pip", "install", *packages],
        check=False,
    )
    if result.returncode != 0:
        print(f"pip install failed (exit {result.returncode})")
        return result.returncode

    if data_dir is not None:
        _update_allowlist(data_dir, packages, add=True)

    for pkg in packages:
        print(f"Installed {pkg}.")
    return 0


def uninstall_packages(
    packages: list[str],
    *,
    data_dir: Path | None = None,
) -> int:
    """Uninstall packages and remove from the instance allowlist.

    Args:
        packages: Package names to uninstall.
        data_dir: Instance data directory.  When ``None``,
            allowlist updates are skipped.

    Returns:
        Exit code (``0`` on success).
    """
    # Resolve import roots *before* pip uninstall so that
    # distribution metadata is still available for packages whose
    # distribution name differs from import root (e.g.
    # python-dateutil -> dateutil).  Filter out roots that are
    # shared with other installed distributions.
    pre_resolved: list[str] | None = None
    if data_dir is not None:
        raw_roots = _resolve_import_roots(packages)
        pre_resolved = _filter_shared_roots(packages, raw_roots)

    result = subprocess.run(
        [sys.executable, "-m", "pip", "uninstall", "-y", *packages],
        check=False,
    )
    if result.returncode != 0:
        print(f"pip uninstall failed (exit {result.returncode})")
        return result.returncode

    if data_dir is not None and pre_resolved is not None:
        _update_allowlist_by_roots(data_dir, pre_resolved)

    for pkg in packages:
        print(f"Uninstalled {pkg}.")
    return 0


def _strip_spec(spec: str) -> str:
    """Strip extras and version constraints from a pip package spec.

    Args:
        spec: Raw pip spec (e.g. ``"pandas[all]>=1.0"``).

    Returns:
        Bare distribution name (e.g. ``"pandas"``).
    """
    return (
        spec.split("[", 1)[0]
        .split(">=", 1)[0]
        .split("<=", 1)[0]
        .split("==", 1)[0]
        .split("~=", 1)[0]
        .split("!=", 1)[0]
        .split(">", 1)[0]
        .split("<", 1)[0]
        .strip()
    )


def _resolve_import_roots(packages: list[str]) -> list[str]:
    """Map pip package specs to their importable top-level module names.

    Uses ``importlib.metadata`` to look up the installed distribution's
    ``top_level.txt`` record.  Falls back to normalising the package
    spec string when metadata is unavailable (e.g. the package was not
    found in the environment).

    Args:
        packages: Pip package specs (e.g. ``["scikit-learn"]``).

    Returns:
        One import root per package (de-duplicated, order-preserved).
    """
    # Build reverse map: top-level module -> distribution names.
    try:
        pkg_map = packages_distributions()
    except Exception:
        pkg_map = {}

    # Invert: distribution name -> top-level modules.
    # Normalise keys with PEP 503 rules (lowercase, hyphens → underscores)
    # so that lookups from user-supplied specs always match.
    dist_to_tops: dict[str, list[str]] = {}
    for top_mod, dists in pkg_map.items():
        for d in dists:
            key = d.lower().replace("-", "_")
            dist_to_tops.setdefault(key, []).append(top_mod)

    seen: set[str] = set()
    roots: list[str] = []
    for spec in packages:
        normalised = _strip_spec(spec).lower().replace("-", "_")

        top_modules = dist_to_tops.get(normalised)
        if top_modules:
            for mod in top_modules:
                if mod not in seen:
                    seen.add(mod)
                    roots.append(mod)
        else:
            # Fallback: use normalised distribution name.
            fallback = normalised.split(".", 1)[0]
            if fallback not in seen:
                seen.add(fallback)
                roots.append(fallback)

    return roots


def _filter_shared_roots(
    packages: list[str],
    roots: list[str],
) -> list[str]:
    """Keep only roots not shared with other installed distributions.

    When multiple distributions provide the same top-level import
    root (e.g. several ``google-*`` packages all provide ``google``),
    removing that root from the allowlist would break imports for
    the packages that remain installed.

    Args:
        packages: Package specs being uninstalled.
        roots: Already-resolved import roots for *packages*.

    Returns:
        Subset of *roots* whose providers are all being uninstalled.
    """
    try:
        pkg_map = packages_distributions()
    except Exception:
        return roots

    uninstalling: set[str] = set()
    for spec in packages:
        uninstalling.add(
            _strip_spec(spec).lower().replace("-", "_")
        )

    safe: list[str] = []
    for root in roots:
        providers = pkg_map.get(root, [])
        remaining = [
            d
            for d in providers
            if d.lower().replace("-", "_") not in uninstalling
        ]
        if not remaining:
            safe.append(root)
    return safe


def _update_allowlist_by_roots(
    data_dir: Path,
    roots: list[str],
) -> None:
    """Remove pre-resolved import roots from the instance allowlist.

    Unlike ``_update_allowlist`` this accepts already-resolved
    root names so that callers can resolve *before* pip uninstall
    removes the distribution metadata.

    Args:
        data_dir: Instance data directory.
        roots: Import root names to remove.
    """
    cfg_path = _config_path(data_dir)
    if not cfg_path.exists():
        return

    cfg = _load_config(cfg_path)
    current: list[str] | None = cfg.get(
        "code_query_allowed_import_roots"
    )
    if current is None:
        return

    before = len(current)
    current = [r for r in current if r not in set(roots)]
    if len(current) < before:
        cfg["code_query_allowed_import_roots"] = current
        _write_config(cfg_path, cfg)


def _update_allowlist(
    data_dir: Path,
    packages: list[str],
    *,
    add: bool,
) -> None:
    """Add or remove package roots from the instance allowlist.

    When the config has no explicit allowlist (``null``), adding
    a package that is already in the built-in defaults is a no-op.
    Adding a package outside the defaults materialises the full
    list plus the new entry.

    Args:
        data_dir: Instance data directory.
        packages: Package root names.
        add: ``True`` to add, ``False`` to remove.
    """
    cfg_path = _config_path(data_dir)
    if not cfg_path.exists():
        if not add:
            return
        cfg_path.parent.mkdir(parents=True, exist_ok=True)

    cfg = _load_config(cfg_path)
    current: list[str] | None = cfg.get(
        "code_query_allowed_import_roots"
    )
    effective = (
        set(current) if current is not None else set(ALLOWED_IMPORT_ROOTS)
    )

    roots = _resolve_import_roots(packages)
    changed = False

    if add:
        for root in roots:
            if root not in effective:
                if current is None:
                    current = sorted(ALLOWED_IMPORT_ROOTS)
                    cfg["code_query_allowed_import_roots"] = current
                current.append(root)
                effective.add(root)
                changed = True
                print(
                    f"  Added '{root}' to code query"
                    " allowlist."
                )
    else:
        if current is not None:
            before = len(current)
            current = [r for r in current if r not in set(roots)]
            if len(current) < before:
                cfg["code_query_allowed_import_roots"] = current
                changed = True

    if changed:
        _write_config(cfg_path, cfg)
