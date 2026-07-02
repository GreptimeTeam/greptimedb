# Agent Resources

Shared resources for coding agents (Codex, Claude Code, ...) and contributors.

## Skills

Shared agent skills live in `.agents/skills`.

- Codex automatically discovers repo skills from `.agents/skills`.
- Claude Code reads the same skills through `.claude/skills`, which is a symlink to this directory.
- Add or update shared skills under `.agents/skills/<skill-name>/SKILL.md`.
- If a new skill does not appear, restart the agent or start a new thread.

## Per-crate guides

Hot crates carry an `AGENTS.md` next to their code as a navigation aid (module
map, read/write paths, change-coupling points, test commands, gotchas):

- [`src/mito2/AGENTS.md`](../src/mito2/AGENTS.md) — primary time-series storage engine
- [`src/metric-engine/AGENTS.md`](../src/metric-engine/AGENTS.md) — metrics engine (logical/physical regions)
- [`src/flow/AGENTS.md`](../src/flow/AGENTS.md) — stream processing / continuous aggregation
- [`src/frontend/AGENTS.md`](../src/frontend/AGENTS.md) — request entry point and orchestration
- [`src/meta-srv/AGENTS.md`](../src/meta-srv/AGENTS.md) — metadata and cluster coordination

## Architecture invariants

[`architecture-invariants.md`](architecture-invariants.md) lists repo-wide rules
that are easy to violate and expensive to get wrong (format compatibility, crate
layering, async runtimes, error handling, feature gating, the DataFusion fork).

## Generated files

[`generated-files.md`](generated-files.md) lists tool-generated artifacts that
must not be hand-edited (sqlness `.result`, `config.md`, dashboards, ...).
