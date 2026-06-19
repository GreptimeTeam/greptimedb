# Agent Skills

Shared agent skills live in `.agents/skills`.

- Codex automatically discovers repo skills from `.agents/skills`.
- Claude Code reads the same skills through `.claude/skills`, which is a symlink to this directory.
- Add or update shared skills under `.agents/skills/<skill-name>/SKILL.md`.
- If a new skill does not appear, restart the agent or start a new thread.
