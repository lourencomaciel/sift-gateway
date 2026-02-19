# Documentation Map

This directory is organized into a small set of primary docs plus one
maintainer-only guide.

## Start Here

- `quickstart.md` — install, init, and first artifact query
- `openclaw/README.md` — OpenClaw-first quickstart and skill pack
- `cli-output-format.md` — stable human and JSON output contract for `sift-gateway`
- `config.md` — all config keys, env vars, defaults, and runtime flags
- `api_contracts.md` — tool contract and response shapes
- `migration-guide.md` — MCP-to-CLI-agnostic migration steps and compatibility
- `recipes.md` — practical patterns and end-to-end examples
- `deployment.md` — URL mode, scaling, and ops checks
- `errors.md` — gateway/runtime error taxonomy
- `observability.md` — structured log events and metrics
- `packaging-transition.md` — CLI-first naming and extras transition plan
- `performance-benchmarks.md` — benchmark runner and baseline matrix
- `benchmarks/README.md` — published benchmark baselines and raw artifacts
- `security-hardening.md` — capture/code security pass checklist
- `release-checklist.md` — release-candidate hardening checklist

## OpenClaw Pack

- `openclaw/SKILL.md` — installable skill instructions
- `openclaw/troubleshooting.md` — context overflow troubleshooting
- `openclaw/response-templates.md` — compact response templates

## Architecture Reference

- `architecture.md` — implementation architecture and invariants

## Maintainer Workflow

- See `../CONTRIBUTING.md` for repository guardrails and release workflow.

## Why This Layout

- The primary docs above are enough for most users.
- The architecture spec is kept separate so operational docs stay short.
- Maintainer policy/setup details live in the contributor guide.
