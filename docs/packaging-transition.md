# Packaging Strategy

This document records the final packaging and command decision for Sift.

## Decision

1. Keep a single published package: `sift-gateway`.
2. Keep a single primary command handle: `sift-gateway`.
3. Run both operating modes from the same executable:
   - MCP gateway mode (default server behavior)
   - Artifact CLI mode (`list`, `schema`, `get`, `query`, `code`, `run`, `diff`)
4. Keep optional dependency extras for heavier compute features.

## Rationale

1. Avoid command-handle ambiguity for users and docs.
2. Avoid PyPI name collisions on short handles such as `sift`.
3. Keep installation, onboarding, and support paths consistent.

## Current State (February 19, 2026)

- Package name: `sift-gateway`
- Primary command: `sift-gateway`
- Additional helper command: `sift-gateway-openclaw-skill`
- Optional extras:
  - `code` (`pandas`, `numpy`, `jmespath`)
  - `data-science` (compatibility alias)

## Non-Goals

1. No separate `sift` command distribution.
2. No `sift-data` wrapper package plan.
3. No dual-handle documentation strategy.

## Validation Checklist

- `uv build` succeeds.
- Package metadata exposes `sift-gateway` and `sift-gateway-openclaw-skill`.
- `uv run sift-gateway --version` succeeds.
- `uv run sift-gateway list --limit 1 --json` succeeds.
- `uv run sift-gateway --check` succeeds.
