"""Packaged OpenClaw integration assets."""

from __future__ import annotations

from importlib.resources import files


def read_asset(name: str) -> str:
    """Read one packaged OpenClaw asset by filename."""
    return (
        files("sift_gateway.openclaw")
        .joinpath(name)
        .read_text(encoding="utf-8")
    )


def skill_text() -> str:
    """Return the packaged OpenClaw skill contents."""
    return read_asset("SKILL.md")


__all__ = ["read_asset", "skill_text"]
