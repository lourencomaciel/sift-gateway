# Packaging Transition Plan

This document tracks the CLI-first naming transition while preserving MCP compatibility.

## Goals

1. Publish CLI-focused distribution identity (`sift-data`) without breaking existing users.
2. Keep `sift-gateway` install and command path stable during transition.
3. Keep base install lean; heavy compute dependencies remain optional extras.

## Current State (February 19, 2026)

- Package name: `sift-gateway`
- Entrypoints:
  - `sift-gateway` (gateway/MCP flows)
  - `sift` (CLI artifact workflows)
- Optional extras:
  - `code` (`pandas`, `numpy`, `jmespath`)
  - `data-science` (backward-compatible alias)

## Transition Strategy

### Phase 1: Compatibility-first

- Keep `sift-gateway` as authoritative package.
- Keep both CLI and MCP entrypoints in one distribution.
- Use docs and quickstarts to position `sift` command as primary for CLI workflows.

### Phase 2: Alias distribution

- Publish `sift-data` as a thin wrapper distribution that depends on `sift-gateway`.
- Ensure `sift` command behavior is identical.
- Preserve all MCP behavior and docs under `sift-gateway`.

### Phase 3: Long-term branding

- Evaluate making `sift-data` primary distribution name.
- Keep `sift-gateway` as compatibility package for at least one major cycle.
- Announce migration windows and deprecation policy in changelog/release notes.

## Extras Policy

- Base install must not auto-install data-science dependencies.
- `code` extra is the canonical way to enable code-query dependencies.
- `data-science` remains as alias during transition to avoid breakage.

## Validation Checklist

- `uv build` succeeds.
- `sift` and `sift-gateway` console scripts are generated from package metadata.
- Installing base package does not require pandas/numpy/jmespath.
- Installing `.[code]` enables code-query data-science stack.
