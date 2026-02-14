# Task Plan: `sift-mcp upstream add` subcommand

## Goal
Add a CLI subcommand that accepts a JSON mcpServers snippet and registers one or more new upstream MCP servers into the gateway config, reusing existing secret externalization and config persistence.

## Current Phase
Phase 1

## Phases

### Phase 1: Implement `run_upstream_add()` core logic
- [ ] Create `src/sift_mcp/config/upstream_add.py` with `run_upstream_add()` and `print_add_summary()`
- [ ] Accept raw mcpServers JSON dict (parsed from CLI string)
- [ ] Load existing gateway config from `state/config.json`
- [ ] Validate: no duplicate prefixes against existing config
- [ ] Validate: transport inference (command XOR url) via `_infer_transport()`
- [ ] Externalize secrets (env/headers) to `upstream_secrets/{prefix}.json` via `write_secret()`
- [ ] Strip secrets from entry after externalizing, add `_gateway.secret_ref`
- [ ] Merge new servers into existing `mcpServers` (reject conflicts by default)
- [ ] Atomic write updated config via `_write_json()` pattern
- [ ] Return summary dict (`added`, `skipped`, `config_path`)
- [ ] Support `--dry-run` flag
- **Status:** pending

### Phase 2: Wire into CLI (main.py)
- [ ] Add `_add_upstream_subcommand(sub)` following `_add_init_subcommand` pattern
- [ ] Register `upstream` parent subcommand with `add` nested subcommand
- [ ] `add` takes a positional `snippet` arg (JSON string) + `--data-dir` + `--dry-run`
- [ ] Add `_run_upstream_add(args)` dispatch function
- [ ] Wire dispatch in `serve()` alongside existing `init` check
- **Status:** pending

### Phase 3: Unit tests
- [ ] Test: add single stdio server (command + args + env)
- [ ] Test: add single http server (url + headers)
- [ ] Test: add multiple servers in one snippet
- [ ] Test: reject duplicate prefix
- [ ] Test: secrets externalized and stripped from entry
- [ ] Test: dry-run writes nothing
- [ ] Test: invalid JSON / missing command+url raises error
- [ ] Test: works with empty existing config
- [ ] Test: CLI arg parsing
- **Status:** pending

### Phase 4: Verification
- [ ] `python -m pytest tests/unit/ -q` passes
- [ ] `python -m ruff check src tests` clean
- [ ] `python -m ruff format src tests` clean
- [ ] `python -m mypy src` clean
- **Status:** pending

## Design Decisions

| Decision | Rationale |
|----------|-----------|
| Input = mcpServers JSON string | Matches what every MCP server README provides |
| Nested subcommand: `upstream add` | Leaves room for future `upstream list`, `upstream remove` |
| Reject duplicate prefix by default | Safe default; user can edit config.json manually to override |
| Always externalize secrets | Consistent with init/sync behavior |
| Reuse `_write_json` pattern from init.py | Battle-tested atomic write; keep duplicated for now |

## Key Reusable Functions

| Function | Source | Purpose |
|----------|--------|---------|
| `_infer_transport()` | `config/mcp_servers.py` | Validate command vs url |
| `write_secret()` | `config/upstream_secrets.py` | Externalize env/headers |
| `_validate_prefix()` | `config/upstream_secrets.py` | Reject path traversal |
| `_write_json()` | `config/init.py` | Atomic JSON persistence |
| `_load_gateway_config()` | `config/init.py` | Read existing state/config.json |
| `_ensure_gateway_config_dir()` | `config/init.py` | Create state/ dir if needed |
