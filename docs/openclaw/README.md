# OpenClaw Integration Pack

This pack makes Sift the default large-output path for OpenClaw agents.

## What you get

- installable skill file: `docs/openclaw/SKILL.md`
- OpenClaw-first quickstart
- troubleshooting guidance for context overflow
- compact response templates

## OpenClaw-first quickstart

1. Install Sift:

```bash
pipx install sift-gateway
```

2. Install the packaged skill:

```bash
mkdir -p ~/.openclaw/skills/sift-gateway
sift-gateway-openclaw-skill --output ~/.openclaw/skills/sift-gateway/SKILL.md
```

3. Restart OpenClaw (or reload skills), then run:

```bash
sift-gateway run -- echo '[{"id":1,"state":"open"},{"id":2,"state":"closed"}]'
sift-gateway code <artifact_id> '$' --expr "df.shape[0]"
```

If `pagination.has_next_page=true`, continue with:

```bash
sift-gateway run --continue-from <artifact_id> -- <next-command-with-next_params-applied>
```

4. Add one short system instruction in your OpenClaw profile:

```text
When command output may exceed ~4KB, capture with `sift-gateway run` and analyze with `sift-gateway code`.
```

## Capture vs inline decision rule

- Inline: expected output < 4KB and used once.
- Capture: lists, logs, paginated APIs, JSON blobs, tabular data, or reused data.
- Always capture: `gh api`, `kubectl ... -o json`, `curl` returning arrays/objects.

## Manual validation checklist

- `sift-gateway run -- <cmd>` returns artifact summary.
- paginated captures continue via `run --continue-from`.
- `sift-gateway run --stdin` works from a pipe.
- `sift-gateway code` returns focused outputs in both `--expr` and `--file` modes.

## Related docs

- `docs/openclaw/SKILL.md`
- `docs/openclaw/troubleshooting.md`
- `docs/openclaw/response-templates.md`
