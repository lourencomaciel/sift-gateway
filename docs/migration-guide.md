# Migration Guide: Contract V1 (`run`, `next_page`, `code`)

This guide covers migration from older retrieval surfaces to the simplified
contract.

## Who should read this

1. Existing `sift-gateway` users upgrading in place.
2. Teams standardizing agent flows around artifact-first execution.
3. Maintainers preparing release cutover notes.

## Compatibility summary

1. Public artifact retrieval is now:
   - `artifact(action="query", query_kind="code")`
   - `artifact(action="next_page", artifact_id=...)`
2. CLI public data path is now:
   - `sift-gateway run`
   - `sift-gateway run --continue-from ...`
   - `sift-gateway code`
3. Legacy query kinds (`describe`, `get`, `select`, `search`) and legacy artifact
   CLI commands are no longer the primary contract.

## Packaging and extras

1. Package remains `sift-gateway`.
2. Main command remains `sift-gateway`.
3. Code runtime dependencies remain optional:
   - `pip install "sift-gateway[code]"`
4. `data-science` stays as a compatibility alias.

## Data and migrations

1. SQLite migrations are additive and auto-applied at startup.
2. Existing artifacts remain readable.
3. No daemon is required; MCP and CLI share the same local storage model.

## Migration steps

1. Upgrade package.
2. Replace old retrieval flows with code-first flows:

```bash
# old mental model: list/schema/query/get/select
# new model:
sift-gateway run -- <command>
sift-gateway code <artifact_id> '$' --expr 'len(df)'
```

3. Replace cursor/continuation usage with explicit page chaining:

```bash
sift-gateway run --continue-from <artifact_id> -- <next-command>
```

4. For MCP clients, use:

```python
artifact(action="next_page", artifact_id="art_...")
artifact(action="query", query_kind="code", ...)
```

## Rollback and safety

1. Keep a backup of your data dir before major upgrades.
2. If code queries are not required, disable with `SIFT_GATEWAY_CODE_QUERY_ENABLED=false`.
3. Keep `SIFT_GATEWAY_SECRET_REDACTION_ENABLED=true` in production.

## Release validation

Before promoting a release candidate:

1. Run unit tests and static checks.
2. Run benchmark matrix in `docs/performance-benchmarks.md`.
3. Confirm docs and changelog are updated for this contract.
