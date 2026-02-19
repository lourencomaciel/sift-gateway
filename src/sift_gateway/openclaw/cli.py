"""Console helper for exporting packaged OpenClaw skill assets."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

from sift_gateway.openclaw import skill_text


def _build_parser() -> argparse.ArgumentParser:
    """Build CLI parser for OpenClaw skill export."""
    parser = argparse.ArgumentParser(
        prog="sift-openclaw-skill",
        description="Print or write packaged OpenClaw SKILL.md",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Write SKILL.md to this path instead of stdout",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing output file",
    )
    return parser


def cli(argv: list[str] | None = None) -> int:
    """Run OpenClaw skill export command."""
    args = _build_parser().parse_args(argv)
    content = skill_text()
    if args.output is None:
        sys.stdout.write(content)
        if not content.endswith("\n"):
            sys.stdout.write("\n")
        return 0

    output_path = Path(args.output).expanduser()
    if output_path.exists() and not args.force:
        sys.stderr.write(
            f"refusing to overwrite existing file: {output_path}\n"
            "use --force to overwrite\n"
        )
        return 1
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(content, encoding="utf-8")
    sys.stdout.write(f"wrote OpenClaw skill: {output_path}\n")
    return 0


def main() -> None:
    """Run command as module/script entrypoint."""
    raise SystemExit(cli())


__all__ = ["cli", "main"]
