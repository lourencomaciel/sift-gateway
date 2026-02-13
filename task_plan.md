# Task Plan: Rebrand SidePouch → Sift

## Goal
Rename the project from `sidepouch-mcp` to `sift-mcp` across all code, config, docs, and tests. The result should build, lint, and pass all tests under the new name with zero references to the old name.

## Current Phase
Phase 1

## Scope

~1,276 occurrences across 180+ files. Six string patterns to replace:

| Pattern | Replacement | Count | Category |
|---------|-------------|-------|----------|
| `sidepouch_mcp` | `sift_mcp` | ~928 | Python package (imports, patches, module paths) |
| `sidepouch-mcp` | `sift-mcp` | ~140 | CLI name, package name, data dir, URLs |
| `SIDEPOUCH_MCP` | `SIFT_MCP` | ~133 | Environment variables |
| `SidePouch` | `Sift` | ~57 | Brand name in prose |
| `sidepouch` | `sift` | ~18 | Docker names, DB user/db names, misc |
| `.sidepouch-mcp` | `.sift-mcp` | ~18 | Data directory path |

### What does NOT change
- `_gateway_*` reserved prefix (functional, not brand)
- `gateway_status` / `GatewayServer` (describes the role, not the brand)
- Internal domain terms: `artifact`, `envelope`, `cursor`, `mapping`
- Database schema (table/column names) — no migration needed
- Git history (we're renaming forward, not rewriting)

## Phases

### Phase 1: Directory rename + pyproject.toml
- [ ] `git mv src/sidepouch_mcp src/sift_mcp`
- [ ] Update `pyproject.toml`: name, entry point, known-first-party
- [ ] Update `.gitignore`: `.sidepouch-mcp/` → `.sift-mcp/`
- **Status:** pending

### Phase 2: Mechanical string replacement (source)
- [ ] Replace `sidepouch_mcp` → `sift_mcp` in all `src/sift_mcp/**/*.py` files (imports, docstrings, module refs)
- [ ] Replace `SidePouch` → `Sift` in docstrings/comments in `src/`
- [ ] Update `constants.py`: `DEFAULT_DATA_DIR`, any brand strings
- [ ] Update `config/settings.py`: env var prefix `SIDEPOUCH_MCP` → `SIFT_MCP`
- [ ] Update `config/docker_postgres.py`: container/volume names
- [ ] Update `main.py`: CLI help text, prog name, error messages
- [ ] Update `mcp/server.py`: FastMCP app name
- [ ] Update `config/init.py`, `config/sync.py`: brand refs, data dir paths
- **Status:** pending

### Phase 3: Mechanical string replacement (tests)
- [ ] Replace `sidepouch_mcp` → `sift_mcp` in all `tests/**/*.py` (imports + monkeypatch targets)
- [ ] Replace `SIDEPOUCH_MCP` → `SIFT_MCP` in test env var references
- [ ] Replace `.sidepouch-mcp` → `.sift-mcp` in test assertions
- [ ] Replace `sidepouch-mcp` → `sift-mcp` in CLI test invocations
- [ ] Update Docker test assertions (container/volume names)
- **Status:** pending

### Phase 4: Documentation + config files
- [ ] Update `README.md`: all brand refs, CLI examples, env vars, paths
- [ ] Update `CLAUDE.md`: title, CLI examples
- [ ] Update `CONTRIBUTING.md`: clone URL, directory name
- [ ] Update `CHANGELOG.md`: add rebrand entry
- [ ] Update `docs/spec_v1_9.md`: title, brand refs
- [ ] Update `docs/config.md`: env vars, defaults, paths
- [ ] Update `.env.example`: env vars, data dir, DSN
- [ ] Update `docker-compose.yml`: user/db names (sidepouch → sift)
- **Status:** pending

### Phase 5: Lock file + build verification
- [ ] Regenerate `uv.lock` via `uv lock`
- [ ] `uv sync` to verify install
- [ ] `uv run sift-mcp --help` to verify CLI entry point
- **Status:** pending

### Phase 6: Test + lint
- [ ] `python -m ruff format src tests`
- [ ] `python -m ruff check src tests`
- [ ] `python -m pytest tests/unit/ -q` — all ~1260 tests pass
- [ ] Grep for any remaining `sidepouch` references (case-insensitive)
- [ ] Fix any stragglers
- **Status:** pending

### Phase 7: Commit + PR
- [ ] Create branch `rebrand-sift`
- [ ] Commit with descriptive message
- [ ] Push and create PR
- **Status:** pending

## Execution Strategy

The safest order is:
1. Rename the directory first (`git mv`) — this is the atomic breaking change
2. Do all string replacements in one pass — the codebase is broken between steps 1 and 2 anyway
3. Regenerate lock file
4. Verify everything

For the string replacements, order matters to avoid double-replacing:
1. `sidepouch_mcp` → `sift_mcp` (longest, most specific — Python package)
2. `.sidepouch-mcp` → `.sift-mcp` (data dir paths — before general hyphenated)
3. `sidepouch-mcp` → `sift-mcp` (CLI/package name)
4. `SIDEPOUCH_MCP` → `SIFT_MCP` (env vars)
5. `SidePouch` → `Sift` (brand name in prose)
6. `sidepouch` → `sift` (remaining: Docker names, DB user, misc)

Step 6 must be done carefully — only target known locations (docker-compose, docker_postgres.py, DSN strings) to avoid false positives.

## Decisions Made
| Decision | Rationale |
|----------|-----------|
| Keep `_gateway_*` prefix | Functional name, not brand |
| Keep `GatewayServer` class name | Describes architectural role |
| No backward compat shim for data dir | Breaking change, clean cut |
| Rename Docker container/volume | Clean break, no migration needed |
| Don't rewrite git history | Forward rename only |

## Open Questions
1. GitHub repo rename (`sidepouch-mcp` → `sift-mcp`) — separate step, user decision
2. PyPI package name reservation — user decision on timing

## Notes
- The `uv.lock` file will be fully regenerated, not find-replaced
- Test monkeypatches reference module paths as strings — these must be updated too
- 13 pre-existing psycopg test failures are unrelated to this rename
