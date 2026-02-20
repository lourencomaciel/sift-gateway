# OpenClaw Integration Pack

This pack provides a CLI-first OpenClaw skill for capturing large command output as artifacts and querying it without flooding model context.

## Included assets

- installable skill file: `docs/openclaw/SKILL.md`
- packaged mirror: `src/sift_gateway/openclaw/SKILL.md`
- writer CLI: `sift-gateway-openclaw-skill`

## Quickstart (CLI mode)

1. Install Sift Gateway:

```bash
uv tool install sift-gateway
```

Alternative:

```bash
pipx install sift-gateway
```

2. Install the packaged skill:

```bash
mkdir -p ~/.openclaw/skills/context-query-guard
sift-gateway-openclaw-skill --output ~/.openclaw/skills/context-query-guard/SKILL.md
```

3. Register the skill in `~/.openclaw/config.toml`:

```toml
[skills]
entries = [
  { path = "~/.openclaw/skills/context-query-guard/SKILL.md", enabled = true }
]
```

Optional if your global policy blocks shell commands:

```toml
[skills]
entries = [
  { path = "~/.openclaw/skills/context-query-guard/SKILL.md", enabled = true, allow_shell_commands = true }
]
```

4. Restart OpenClaw (or reload skills), then validate:

```bash
sift-gateway run --json -- echo '[{"id":1,"state":"open"},{"id":2,"state":"closed"}]'
sift-gateway code --json <artifact_id> '$' --code "def run(data, schema, params): return len(data)"
```

If `pagination.next.kind=="command"`:

```bash
sift-gateway run --json --continue-from <artifact_id> -- <next-command-with-next_params-applied>
```

5. Add one short profile rule:

```text
When output may exceed ~4KB, capture with `sift-gateway run --json` and analyze with `sift-gateway code --json`.
```

## Load-time gating metadata

The skill uses OpenClaw metadata gating via `metadata.openclaw.requires.bins` and includes an install hint for the macOS Skills UI:

```yaml
metadata: {"openclaw":{"requires":{"bins":["sift-gateway"]},"install":[{"id":"uv","kind":"uv","package":"sift-gateway","bins":["sift-gateway"],"label":"Install Sift Gateway (uv)"}]}}
```

## Operating rules

- Prefer `--json` for all run/code invocations.
- Keep only `artifact_id` and compact findings in prompt context.
- If `response_mode` is `schema_ref`, inspect `schemas_compact` before writing code queries.
- If `status` is `error` or `command_exit_code` is non-zero, fix capture first.
- Continue pagination only when `pagination.next.kind=="command"`.

## Troubleshooting

### Symptom: context still gets flooded

Cause:

- large commands are still executed directly.

Fix:

```bash
sift-gateway run --json -- <large-command>
```

### Symptom: capture command failed

Cause:

- auth/permissions issue, missing binary, or non-zero upstream exit.

Fix:

- run the command standalone first
- inspect `status`, `command_exit_code`, and `payload` from `run --json`

### Symptom: pagination stopped early

Cause:

- continuation command was not issued.

Fix:

```bash
sift-gateway run --json --continue-from <artifact_id> -- <next-command-with-next_params-applied>
```

### Symptom: code query failed

Cause:

- missing code source, invalid `--params` JSON, or wrong root path.

Fix:

```bash
sift-gateway code --json <artifact_id> '$.items' --code "def run(data, schema, params): return len(data)"
```

## Related docs

- `docs/openclaw/SKILL.md`
