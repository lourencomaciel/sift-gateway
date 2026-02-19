# Migration Guide: MCP-Only to CLI-Agnostic Sift

This guide covers migration to the protocol-agnostic architecture where MCP and
CLI surfaces share one execution core.

## Who should read this

1. Existing `sift-gateway` users upgrading in place.
2. Teams adopting `sift-gateway` CLI workflows alongside MCP.
3. Maintainers preparing release cutover notes.

## Compatibility Summary

1. Existing MCP query contracts remain backward compatible.
2. New protocol-neutral capture fields are additive:
   - `capture_kind`
   - `capture_origin`
   - `capture_key`
3. Legacy MCP identity fields remain available during transition:
   - `source_tool`
   - `upstream_instance_id`
   - `request_key`

## Packaging and Extras

1. Keep installing `sift-gateway` for current MCP setups.
2. CLI-first path includes `sift-gateway` command entrypoint.
3. Code runtime dependencies are optional:
   - `pip install "sift-gateway[code]"`
4. `data-science` remains as a compatibility alias for now.

## Data and Migrations

1. SQLite migrations are additive and auto-applied at startup.
2. Existing artifacts are backfilled into neutral capture identity columns.
3. No daemon is required; both MCP and CLI use the same local data model.

## CLI Adoption Path

1. Install/upgrade package.
2. Start with retrieval on existing artifacts:

```bash
sift-gateway list
sift-gateway schema <artifact_id>
sift-gateway query <artifact_id> '$'
```

3. Add capture workflows:

```bash
sift-gateway run -- gh api repos/org/repo/pulls
kubectl get pods -o json | sift-gateway run --stdin --tag k8s
```

4. Add code analysis only when needed:

```bash
sift-gateway code <artifact_id> '$' --expr "df['state'].value_counts().to_dict()"
```

## Rollback and Safety

1. If CLI workflows are not needed, continue MCP-only usage unchanged.
2. You can disable code queries via config/env without affecting retrieval/capture.
3. Preserve your data dir backup before major upgrades in production contexts.

## Release Validation

Before promoting a release candidate:

1. Run unit tests and static checks.
2. Run the benchmark matrix in `docs/performance-benchmarks.md`.
3. Confirm docs and changelog entries are updated for migration messaging.

