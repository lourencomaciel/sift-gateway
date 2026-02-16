# OpenClaw Guide

OpenClaw users can onboard Sift with a single command.

## Quick Setup

```bash
pipx install sift-mcp
sift-mcp init --client openclaw
```

This imports your OpenClaw `mcpServers`, externalizes inline secrets, writes
Sift config under `.sift-mcp/state/`, and rewrites the selected OpenClaw config
to gateway-only mode.

## Scope Selection

By default, Sift uses `--scope auto`:

- If `./.openclaw/mcp-servers.json` exists, Sift uses it.
- Otherwise Sift uses `~/.config/openclaw/mcp-servers.json`.

You can force scope explicitly:

```bash
# Project-local OpenClaw config
sift-mcp init --client openclaw --scope project

# Global OpenClaw config
sift-mcp init --client openclaw --scope global
```

## Safety / Rollback

Preview changes before writing:

```bash
sift-mcp init --client openclaw --scope auto --dry-run
```

Rollback:

```bash
sift-mcp init --client openclaw --scope global --revert
```

Use the same scope on revert that you used during migration.

## Verification

```bash
sift-mcp --check
```

`--check` verifies config parsing, database connectivity, filesystem access, and
upstream reachability.

## OpenClaw Bootstrap Skill Prompt

If you use OpenClaw custom skills, this prompt can be used as the skill body:

```text
Bootstrap and verify Sift MCP in this workspace.

Steps:
1) Run `pipx install sift-mcp` if `sift-mcp` is missing.
2) Run `sift-mcp init --client openclaw --scope auto --dry-run`.
3) Run `sift-mcp init --client openclaw --scope auto`.
4) Run `sift-mcp --check`.
5) Report which OpenClaw config file was migrated and whether checks passed.

Rules:
- If migration reports no `mcpServers`, stop and explain the fix.
- Never print secrets from upstream env/headers.
```
