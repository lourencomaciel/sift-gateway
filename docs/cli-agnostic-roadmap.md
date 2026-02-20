# Sift Protocol-Agnostic Roadmap (Archived)

This roadmap is now archived as implemented.

## Final Contract (V1)

Public runtime surface is intentionally narrow:

1. Capture data:
   - CLI: `sift-gateway run -- <command>`
   - MCP: mirrored upstream tool calls
2. Continue upstream pagination:
   - CLI: `sift-gateway run --continue-from <artifact_id> -- <next-command>`
   - MCP: `artifact(action="next_page", artifact_id=...)`
3. Analyze artifacts:
   - CLI: `sift-gateway code ...`
   - MCP: `artifact(action="query", query_kind="code", ...)`

Response contract is shared across interfaces:

- `response_mode="full"` or `response_mode="schema_ref"`
- `artifact_id`
- optional `pagination`, `lineage`, and `metadata`

## What was removed from the public contract

1. Legacy retrieval query kinds: `describe`, `get`, `select`, `search`.
2. Legacy artifact CLI commands: `list`, `schema`, `get`, `query`, `diff`.
3. Raw-passthrough behavior as a distinct external mode.
4. Automatic page-loop behavior as a default client abstraction.

## Why this shape

1. Keeps the agent mental model small and deterministic.
2. Unifies MCP and CLI around one persistence and execution core.
3. Makes pagination explicit and lineage-safe.
4. Reduces context tokens by favoring schema-ref when advantageous.

## Status

- Core extraction complete.
- Contract-v1 routing complete.
- Docs, tests, and release flow aligned with v1.
