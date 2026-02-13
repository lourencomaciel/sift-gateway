# Artifact Query References

## Problem

LLMs connected via Sift frequently need to pass data from one tool's output to another tool's input. Today this requires an intermediate retrieval call that pulls data into the context window just to shuttle it forward. For large artifacts (>8KB), this wastes tokens and round-trips.

## Solution

Allow artifact IDs with optional JSONPath queries as tool arguments. The gateway resolves these references server-side before forwarding to the upstream tool. The LLM never sees the intermediate data.

## Syntax

```
art_<32hex>                    full payload resolution
art_<32hex>:$.path.to.field    JSONPath subset resolution
```

Examples:

| Reference | Resolves to |
|-----------|------------|
| `art_7f3a...` | Entire stored JSON/text payload |
| `art_7f3a...:$.items[0].name` | Single string value |
| `art_7f3a...:$.items[*].email` | List of all email values |
| `art_7f3a...:$.config.db.host` | Nested scalar |

## Detection

Top-level string values in forwarded tool arguments are inspected:

1. Match `^art_[0-9a-f]{32}` prefix.
2. If that is the full string: bare ref (resolve full payload).
3. If followed by `:$`: split on first `:`, parse remainder as JSONPath.
4. Anything else: not a reference, pass through unchanged.

Nested values (inside dicts or lists) are never inspected.

## Resolution

For each detected reference:

1. Fetch the artifact envelope from the database (reuses `FETCH_ARTIFACT_SQL`).
2. Validate: not deleted, not binary-only.
3. Extract the JSON target via `extract_json_target` (same as `artifact.get`).
4. If JSONPath query present: evaluate via `evaluate_jsonpath`.
   - Empty match list: return `ResolveError`.
   - Single match: return the scalar (unwrapped).
   - Multiple matches: return the list.
5. Substitute the resolved value into the argument dict.

## Integration point

Inside `handle_mirrored_tool` (Phase 1.75), after cache check and quota enforcement, before the upstream call. Uses a short-lived DB connection. The original pointer-containing args are used for request identity hashing and cache lookup; only the upstream call receives resolved args.

## Usage hint

The `build_usage_hint` function includes:

> Tip: pass "art_xxx" directly as an argument to another tool. Use "art_xxx:$.path" to pass a specific field (e.g. "art_xxx:$.items[0].name").

## Invariants

- **Caching**: Request identity hashes pre-resolution args (pointer strings). Same reference = same cache key.
- **DB-less mode**: Resolution skipped entirely (no artifacts to resolve).
- **Binary-ref artifacts**: Refused with clear error if no JSON/text content parts.
- **Schema validation**: Unaffected (only checks required/additional, not types).
- **Passthrough**: Determined by response size, not input.

## Future extension

WHERE filtering (`art_xxx:$.items:where=status='active'`) can be added later as a query parameter extension. For now, WHERE queries use the existing `artifact(action="select", where=...)` path which returns small results via passthrough.

## Files

| File | Change |
|------|--------|
| `src/sift_mcp/mcp/resolve_refs.py` | Update detection regex, add JSONPath evaluation |
| `src/sift_mcp/mcp/handlers/mirrored_tool.py` | No change (integration already in place) |
| `src/sift_mcp/tools/usage_hint.py` | Update hint text with query syntax |
| `tests/unit/test_resolve_refs.py` | Add query resolution tests |
| `tests/unit/test_usage_hint.py` | Update hint assertion |
