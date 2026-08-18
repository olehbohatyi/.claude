# .claude

Personal Claude Code configuration — synced across machines

## structure

- `skills/` — custom skills
- `agents/` — custom subagents
- `others/` — everything else (settings, hooks, commands, etc.)

## setup

clone into `~/.claude`

- back up or merge any existing `~/.claude` contents first
- cloning into a non-empty `~/.claude` directory will fail

## notes

- session data, transcripts, caches, and credentials are intentionally excluded (see `.gitignore`)