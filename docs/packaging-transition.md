# Packaging Strategy

Final packaging and command decision for Sift.

## Decision

1. Keep one published package: `sift-gateway`.
2. Keep one primary command: `sift-gateway`.
3. Run both modes from the same executable:
   - MCP gateway mode (default behavior)
   - Artifact CLI mode (`run`, `code`)
4. Keep optional dependency extras for heavier compute features.

## Rationale

1. Avoid command-handle ambiguity.
2. Avoid PyPI collisions on short names.
3. Keep install and support paths consistent.

## Current state (February 20, 2026)

- Package name: `sift-gateway`
- Primary command: `sift-gateway`
- Additional helper command: `sift-gateway-openclaw-skill`
- Optional extras:
  - `code` (`pandas`, `numpy`, `jmespath`)
  - `data-science` (compatibility alias)

## Non-goals

1. No separate `sift` command distribution.
2. No wrapper package split.
3. No dual-handle documentation strategy.

## Validation checklist

- `uv build` succeeds.
- Package metadata exposes `sift-gateway` and `sift-gateway-openclaw-skill`.
- `uv run sift-gateway --version` succeeds.
- `uv run sift-gateway run -- echo '{"ok":true}' --json` succeeds.
- `uv run sift-gateway --check` succeeds.
